"""整版发布器：覆盖证明、原子发布与新旧版差异。

发布前提
========
只有当前有效页（半开范围）**合并后恰好完整覆盖**整个稳定商品 ID 域，
且候选证据无矛盾（矛盾在接收期已拦截，这里再做一次防御性复核），
才允许发布。覆盖证明与每页原始响应定位随版本持久化保留。

覆盖证明基于区间合并，时间复杂度 O(P log P)（P 为页数），
空窗口只体现为区间端点，绝不逐 ID 展开。

发布是单事务原子操作：任何一步失败，旧已发布版保持完全可读。
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

from .protocol import NotPublishable
from .repository import Repository, coverage_manifest, utc_now


@dataclass(frozen=True, slots=True)
class Gap:
    lo: int
    hi: int

    @property
    def size(self) -> int:
        return self.hi - self.lo


def compute_coverage(repo: Repository, version_id: str) -> dict[str, Any]:
    """合并候选页区间，产出覆盖证明结构（不修改任何状态）。

    返回::

        {"domain": [lo, hi), "covered": bool,
         "union_segments": [[lo, hi), ...], "gaps": [{"lo","hi","size"}, ...],
         "page_count": n, "empty_pages": [...]}
    """
    version = repo.get_version(version_id)
    if version is None:
        raise NotPublishable(f"观察版 {version_id} 不存在")
    dlo, dhi = version["domain_lo"], version["domain_hi"]
    pages = repo.list_pages(version_id)

    intervals = sorted(
        ((p["range_lo"], p["range_hi"], p["page_id"]) for p in pages),
        key=lambda x: (x[0], x[1]),
    )
    union: list[list[int]] = []
    empty_pages: list[str] = []
    for lo, hi, pid in intervals:
        if lo == hi:
            empty_pages.append(pid)  # 空页合法：贡献空区间，不展开任何 ID
            continue
        if not union or lo > union[-1][1]:
            union.append([lo, hi])
        elif hi > union[-1][1]:
            union[-1][1] = hi  # 邻接或重叠 → 合并；重叠本身合法

    gaps: list[dict[str, int]] = []
    cursor = dlo
    for lo, hi in union:
        if lo > cursor:
            gaps.append({"lo": cursor, "hi": lo, "size": lo - cursor})
        cursor = max(cursor, hi)
    if cursor < dhi:
        gaps.append({"lo": cursor, "hi": dhi, "size": dhi - cursor})

    return {
        "domain": [dlo, dhi],
        "domain_size": dhi - dlo,
        "covered": not gaps,
        "union_segments": union,
        "gaps": gaps,
        "page_count": len(pages),
        "empty_pages": empty_pages,
    }


def defensive_consistency_check(repo: Repository, version_id: str) -> None:
    """发布前防御性复核：证据必须落在声明范围内、同商品载荷必须一致。

    正常路径下接收期已保证成立；此处任何失败都拒绝发布。
    """
    conn = repo.conn
    row = conn.execute(
        """
        SELECT e.product_id AS pid, e.page_id AS pid_page
        FROM evidence e JOIN pages p
          ON p.version_id=e.version_id AND p.page_id=e.page_id
        WHERE e.version_id=?
          AND (e.product_id < p.range_lo OR e.product_id >= p.range_hi)
        LIMIT 1
        """,
        (version_id,),
    ).fetchone()
    if row is not None:
        raise NotPublishable(
            f"防御检查失败：商品 {row['pid']} 证据越出页 {row['pid_page']} 声明范围"
        )

    row = conn.execute(
        """
        SELECT product_id, COUNT(DISTINCT url || '|' || price || '|' || product_revision) AS variants
        FROM evidence WHERE version_id=?
        GROUP BY product_id HAVING variants > 1 LIMIT 1
        """,
        (version_id,),
    ).fetchone()
    if row is not None:
        raise NotPublishable(
            f"防御检查失败：商品 {row['product_id']} 存在互相矛盾的多份载荷"
        )


def publish(repo: Repository, version_id: str) -> dict[str, Any]:
    """原子发布一个完整观察版；返回发布摘要（含四类差异计数）。

    失败（缺口/矛盾/不存在）抛 :class:`NotPublishable`，旧版不受影响。
    """
    version = repo.get_version(version_id)
    if version is None:
        raise NotPublishable(f"观察版 {version_id} 不存在")
    if version["state"] == "abandoned":
        raise NotPublishable(f"观察版 {version_id} 已失效放弃，不能发布")

    coverage = compute_coverage(repo, version_id)
    if not coverage["covered"]:
        gap_desc = ", ".join(f"[{g['lo']},{g['hi']})" for g in coverage["gaps"])
        raise NotPublishable(f"覆盖存在缺口: {gap_desc}")
    defensive_consistency_check(repo, version_id)

    manifest = coverage_manifest(repo, version_id)
    proof = {
        "version_id": version_id,
        "computed_at": utc_now(),
        "coverage": coverage,
        "pages": manifest["pages"],  # 每页修订、范围、内容摘要、原始响应定位
    }

    old_version = repo.latest_published_version()
    with repo.transaction() as conn:
        # 1) 落发布版本与覆盖证明（旧版行保留，读者仍可读）。
        conn.execute(
            "INSERT OR REPLACE INTO published_versions VALUES (?,?,?,?,?,?)",
            (version_id, version["domain_lo"], version["domain_hi"], utc_now(),
             json.dumps(proof, ensure_ascii=False, sort_keys=True),
             json.dumps(manifest, ensure_ascii=False, sort_keys=True)),
        )

        # 2) 去重后的在售快照（矛盾已被拦截，任意一份证据即权威载荷）。
        conn.execute("DELETE FROM published_products WHERE version_id=?", (version_id,))
        conn.execute(
            """
            INSERT INTO published_products
            SELECT version_id, product_id,
                   MIN(url), MIN(price), MIN(product_revision)
            FROM evidence WHERE version_id=?
            GROUP BY version_id, product_id
            """,
            (version_id,),
        )

        # 3) 与旧版的四类差异（纯集合 SQL，列名显式对齐表结构）。
        conn.execute("DELETE FROM published_diff WHERE version_id=?", (version_id,))
        if old_version is None:
            conn.execute(
                """
                INSERT INTO published_diff (
                    version_id, product_id, change_type,
                    old_url, new_url, old_price, new_price,
                    old_product_revision, new_product_revision)
                SELECT ?, product_id, 'added',
                       NULL, url, NULL, price, NULL, product_revision
                FROM published_products WHERE version_id=?
                """,
                (version_id, version_id),
            )
        else:
            conn.execute(
                """
                INSERT INTO published_diff (
                    version_id, product_id, change_type,
                    old_url, new_url, old_price, new_price,
                    old_product_revision, new_product_revision)
                SELECT ?, n.product_id,
                       CASE
                         WHEN o.product_id IS NULL THEN 'added'
                         WHEN o.url <> n.url OR o.price <> n.price
                              OR o.product_revision <> n.product_revision
                         THEN 'changed'
                         ELSE 'unchanged'
                       END,
                       o.url, n.url, o.price, n.price,
                       o.product_revision, n.product_revision
                FROM published_products n
                LEFT JOIN published_products o
                  ON o.version_id=? AND o.product_id=n.product_id
                WHERE n.version_id=?
                UNION ALL
                SELECT ?, o.product_id, 'removed',
                       o.url, NULL, o.price, NULL, o.product_revision, NULL
                FROM published_products o
                WHERE o.version_id=?
                  AND NOT EXISTS (
                      SELECT 1 FROM published_products n
                      WHERE n.version_id=? AND n.product_id=o.product_id
                  )
                """,
                (version_id, old_version, version_id,
                 version_id, old_version, version_id),
            )
        conn.execute(
            "UPDATE versions SET state='published' WHERE version_id=?", (version_id,)
        )

    counts = repo.diff_counts(version_id)
    return {
        "version_id": version_id,
        "old_version": old_version,
        "product_count": repo.count_published_products(version_id),
        "diff_counts": counts,
        "coverage_proof": proof,
    }


def coverage_status(repo: Repository, version_id: str) -> dict[str, Any]:
    """供演示/CLI 展示当前候选的移窗缺口与重叠情况（只读）。"""
    coverage = compute_coverage(repo, version_id)
    pages = repo.list_pages(version_id)
    overlaps: list[dict[str, Any]] = []
    spans = sorted(
        ((p["range_lo"], p["range_hi"], p["page_id"], p["revision"]) for p in pages),
        key=lambda x: (x[0], x[1]),
    )
    for i in range(1, len(spans)):
        (plo, phi, pid, prev), (lo, hi, other, orev) = spans[i - 1], spans[i]
        if lo < phi:
            overlaps.append({
                "pages": [pid, other],
                "overlap": [lo, min(phi, hi)],
                "revisions": [prev, orev],
            })
    return {**coverage, "overlaps": overlaps}
