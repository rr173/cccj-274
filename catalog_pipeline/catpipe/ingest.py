"""单页接收引擎：实现全部校验、替换、重送与拒绝语义。

接收语义（与规格逐条对应）
==========================
* 页声明半开范围 ``[lo, hi)``；商品必须落在范围内，越界即拒绝整次接收。
* 范围内商品 ID 重复 → 拒绝整次接收（暂存表主键 + 流式插入，不放大内存）。
* 同观察版同商品的出现/缺席或载荷（URL/价格/商品修订）矛盾 → 拒绝整次接收。
* URL 变动不能另造商品：URL 只是商品载荷字段，身份永远是稳定 product_id，
  迁移在同 product_id 上表现为载荷变更。
* 同页较高修订：**整份替换**旧页的值与覆盖（先删旧证据/旧范围再插新值）。
* 同号同内容重送：无操作；同号异内容或低修订：拒绝。
* 任一拒绝路径都在事务内回滚，候选区保持原状，原始响应移入隔离区。
* 观察版/令牌失效（:class:`VersionInvalid`）由上层放弃整轮，旧轮不补洞。

矛盾检查全部基于 ``evidence``/``pages`` 的集合 SQL，
高修订处理不需要重新扫描任何其他页的原始响应。
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from typing import BinaryIO

from .protocol import (
    PageHeader,
    ProductRow,
    RejectedError,
    VersionInvalid,
    dump_jsonl_line,
    iter_jsonl,
    page_content_hash_from_sorted,
    stream_checksum,
)
from .repository import Repository, new_delivery_id, utc_now


@dataclass(frozen=True, slots=True)
class Receipt:
    delivery_id: str
    version_id: str
    page_id: str
    revision: int
    outcome: str  # accepted | replaced | noop
    content_hash: str
    raw_locator: str
    detail: str = ""


class IngestionEngine:
    def __init__(self, repo: Repository):
        self.repo = repo

    # ---- 流式入口 ---------------------------------------------------------

    def ingest_stream(self, stream: BinaryIO, delivery_id: str | None = None) -> Receipt:
        """消费一个 JSONL 页响应流。

        全程流式：逐行落盘、逐行进暂存表，整页大小与商品数均可远超内存。
        整个接收（暂存 + 矛盾检查 + 替换/落证据）处于**单事务**内：
        任一步拒绝即 ROLLBACK，暂存行随事务自动消失，候选区保持原状；
        成功才 COMMIT。事务结束后再做隔离与审计落账（各自独立事务）。
        """
        delivery_id = delivery_id or new_delivery_id()
        spool = self.repo.spool_path(delivery_id)
        checksum = stream_checksum()
        state = "want_header"
        header: PageHeader | None = None
        staged = 0
        end_count: int | None = None
        end_checksum: str | None = None
        conn = self.repo.conn

        conn.execute("BEGIN IMMEDIATE")
        try:
            with open(spool, "wb") as raw:
                for obj in iter_jsonl(stream):
                    line = dump_jsonl_line(obj)
                    raw.write(line)
                    # 校验和口径：覆盖 end 之前的全部行（end 行自身携带摘要，不参与摘要）。
                    if obj.get("type") != "end":
                        checksum.update(line)
                    t = obj.get("type")

                    if state == "want_header":
                        if t != "page":
                            raise RejectedError("protocol", "响应首行必须是 page 页声明")
                        header = PageHeader.from_obj(obj)
                        self._check_header(header)
                        state = "want_product_or_end"
                        continue

                    if t == "product":
                        if state != "want_product_or_end":
                            raise RejectedError("protocol", "end 之后不允许再出现商品行")
                        row = ProductRow.from_obj(obj)
                        self._stage_row(conn, delivery_id, header, row)  # type: ignore[arg-type]
                        staged += 1
                    elif t == "end":
                        if state != "want_product_or_end":
                            raise RejectedError("protocol", "出现多个 end 行")
                        try:
                            end_count = int(obj["count"])
                            end_checksum = str(obj.get("checksum", ""))
                        except (KeyError, TypeError, ValueError) as exc:
                            raise RejectedError("protocol", f"end 行字段非法: {exc}") from exc
                        state = "done"
                    else:
                        raise RejectedError("protocol", f"未知的行类型: {t!r}")

            if header is None:
                raise RejectedError("protocol", "空响应，缺少页声明")
            if state != "done":
                raise RejectedError("truncated", "响应在 end 行之前结束")
            if end_count != staged:
                raise RejectedError(
                    "count_mismatch", f"end.count={end_count} 与实际商品行数 {staged} 不一致"
                )
            if end_checksum != checksum.hexdigest():
                raise RejectedError(
                    "checksum_mismatch", "传输校验和不一致，响应可能被截断或篡改"
                )
            receipt = self._commit_in_tx(conn, delivery_id, header, spool)
            conn.execute("COMMIT")
            self.repo.log_delivery(
                delivery_id, header.version_id, header.page_id, header.revision,
                receipt.outcome, None, receipt.detail, spool, receipt.content_hash,
            )
            return receipt
        except RejectedError as exc:
            # 回滚即清空本次全部暂存，候选区原封不动。
            conn.execute("ROLLBACK")
            locator = self.repo.quarantine(delivery_id, exc.reason_code, exc.message)
            self.repo.log_delivery(
                delivery_id, getattr(header, "version_id", None),
                getattr(header, "page_id", None),
                getattr(header, "revision", None),
                "rejected", exc.reason_code, exc.message, locator, None,
            )
            raise

    # ---- 阶段校验 ---------------------------------------------------------

    @staticmethod
    def _check_header(h: PageHeader) -> None:
        if h.range_lo < 0 or h.domain_lo < 0:
            raise RejectedError("bad_range", "ID 范围起点不能为负")
        if h.range_lo > h.range_hi:
            raise RejectedError("bad_range", f"页 {h.page_id} 半开范围起点大于终点")
        if not (h.domain_lo <= h.range_lo and h.range_hi <= h.domain_hi):
            raise RejectedError(
                "out_of_domain",
                f"页 {h.page_id} 范围 [{h.range_lo},{h.range_hi}) 越出 ID 域 "
                f"[{h.domain_lo},{h.domain_hi})",
            )
        if h.revision < 0:
            raise RejectedError("bad_revision", "修订号不能为负")

    def _stage_row(self, conn: sqlite3.Connection, delivery_id: str,
                   h: PageHeader, row: ProductRow) -> None:
        if not (h.range_lo <= row.product_id < h.range_hi):
            raise RejectedError(
                "product_out_of_range",
                f"商品 {row.product_id} 不在页 {h.page_id} 声明范围 "
                f"[{h.range_lo},{h.range_hi}) 内",
            )
        try:
            conn.execute(
                "INSERT INTO page_staging VALUES (?,?,?,?,?,?)",
                (delivery_id, h.version_id, row.product_id, row.url,
                 row.price, row.product_revision),
            )
        except sqlite3.IntegrityError as exc:
            raise RejectedError(
                "duplicate_product",
                f"页 {h.page_id} 范围内商品 ID {row.product_id} 重复出现",
            ) from exc

    # ---- 提交期：版本/令牌、矛盾集合检查、整份替换 -------------------------

    def _commit_in_tx(self, conn: sqlite3.Connection, delivery_id: str,
                      h: PageHeader, spool: str) -> Receipt:
        """在调用方已开启的事务内完成版本/令牌校验、矛盾检查与落库。"""
        repo = self.repo
        version = conn.execute(
            "SELECT * FROM versions WHERE version_id=?", (h.version_id,)
        ).fetchone()
        if version is None:
            # 全新观察版：首次接收即建立候选轮次。
            conn.execute(
                "INSERT INTO versions VALUES (?,?,?,?, 'candidate', ?, NULL, NULL)",
                (h.version_id, h.token, h.domain_lo, h.domain_hi, utc_now()),
            )
        else:
            if version["state"] == "abandoned":
                raise VersionInvalid(
                    f"观察版 {h.version_id} 已失效放弃，不能用旧轮补洞"
                )
            if version["token"] != h.token:
                raise VersionInvalid(f"观察版 {h.version_id} 令牌不匹配")
            if version["domain_lo"] != h.domain_lo or version["domain_hi"] != h.domain_hi:
                raise RejectedError(
                    "domain_changed",
                    f"同观察版 {h.version_id} 的 ID 域声明前后不一致",
                )

        existing = conn.execute(
            "SELECT * FROM pages WHERE version_id=? AND page_id=?",
            (h.version_id, h.page_id),
        ).fetchone()

        # 流式摘要：游标已 ORDER BY product_id，不在内存缓存整页商品。
        new_hash = page_content_hash_from_sorted(
            h.page_id, h.revision, h.range_lo, h.range_hi,
            repo.iter_staging_products_sorted(delivery_id),
        )

        if existing is not None:
            if h.revision < existing["revision"]:
                raise RejectedError(
                    "stale_revision",
                    f"页 {h.page_id} 修订 {h.revision} 低于已收修订 "
                    f"{existing['revision']}",
                )
            if h.revision == existing["revision"]:
                if new_hash == existing["content_hash"]:
                    # 同号同内容：无操作重送。删除本次暂存行，页与证据保持不变。
                    repo.clear_staging(conn, delivery_id)
                    return Receipt(
                        delivery_id, h.version_id, h.page_id, h.revision,
                        "noop", new_hash, existing["raw_locator"],
                        "同修订同内容重送，无操作",
                    )
                raise RejectedError(
                    "same_revision_conflict",
                    f"页 {h.page_id} 同修订号 {h.revision} 内容不一致",
                )
            # 高修订：整份替换。先对新内容做全套矛盾检查，再删旧值/旧覆盖。
            self._check_contradictions(conn, delivery_id, h, replace_page_id=h.page_id)
            conn.execute(
                "DELETE FROM evidence WHERE version_id=? AND page_id=?",
                (h.version_id, h.page_id),
            )
            outcome, detail = "replaced", f"高修订 {existing['revision']} -> {h.revision}，整份替换"
        else:
            self._check_contradictions(conn, delivery_id, h, replace_page_id=None)
            outcome, detail = "accepted", "新页接收"

        conn.execute(
            "INSERT INTO evidence (version_id, product_id, page_id, url, price, "
            "product_revision, updated_revision) "
            "SELECT version_id, product_id, ?, url, price, product_revision, ? "
            "FROM page_staging WHERE delivery_id=?",
            (h.page_id, h.revision, delivery_id),
        )
        conn.execute(
            "INSERT INTO pages VALUES (?,?,?,?,?,?,?,?) "
            "ON CONFLICT(version_id, page_id) DO UPDATE SET "
            "revision=excluded.revision, range_lo=excluded.range_lo, "
            "range_hi=excluded.range_hi, content_hash=excluded.content_hash, "
            "raw_locator=excluded.raw_locator, received_at=excluded.received_at",
            (h.version_id, h.page_id, h.revision, h.range_lo, h.range_hi,
             new_hash, spool, utc_now()),
        )
        repo.clear_staging(conn, delivery_id)
        return Receipt(delivery_id, h.version_id, h.page_id, h.revision,
                       outcome, new_hash, spool, detail)

    # ---- 矛盾检查（纯集合 SQL，不重扫任何页原文） --------------------------

    @staticmethod
    def _check_contradictions(
        conn: sqlite3.Connection, delivery_id: str, h: PageHeader,
        replace_page_id: str | None,
    ) -> None:
        vid = h.version_id
        exclude = "AND ev.page_id != ?" if replace_page_id is not None else ""
        ex_params = [replace_page_id] if replace_page_id is not None else []

        # (1) 同商品已在其他页出现：载荷必须完全一致（URL 迁移也只能在同 ID 上变值）。
        row = conn.execute(
            f"""
            SELECT s.product_id AS pid, ev.url AS old_url, s.url AS new_url,
                   ev.price AS old_price, s.price AS new_price,
                   ev.product_revision AS old_pr, s.product_revision AS new_pr,
                   ev.page_id AS other_page
            FROM page_staging s
            JOIN evidence ev ON ev.version_id=s.version_id AND ev.product_id=s.product_id
            WHERE s.delivery_id=? {exclude}
              AND (ev.url != s.url OR ev.price != s.price
                   OR ev.product_revision != s.product_revision)
            LIMIT 1
            """,
            [delivery_id, *ex_params],
        ).fetchone()
        if row is not None:
            raise RejectedError(
                "payload_conflict",
                f"商品 {row['pid']} 与页 {row['other_page']} 载荷矛盾："
                f"(url={row['old_url']!r}/{row['new_url']!r}, "
                f"price={row['old_price']}/{row['new_price']}, "
                f"product_revision={row['old_pr']}/{row['new_pr']})",
            )

        # (2) 新页范围内、但未列入新页的商品，若其他页宣称其在售 → 出现/缺席矛盾。
        row = conn.execute(
            f"""
            SELECT ev.product_id AS pid, ev.page_id AS other_page
            FROM evidence ev
            WHERE ev.version_id=? {exclude}
              AND ev.product_id >= ? AND ev.product_id < ?
              AND NOT EXISTS (
                  SELECT 1 FROM page_staging s
                  WHERE s.delivery_id=? AND s.product_id=ev.product_id
              )
            LIMIT 1
            """,
            [vid, *ex_params, h.range_lo, h.range_hi, delivery_id],
        ).fetchone()
        if row is not None:
            raise RejectedError(
                "presence_conflict",
                f"页 {h.page_id} 范围 [{h.range_lo},{h.range_hi}) 内商品 "
                f"{row['pid']} 缺席，但页 {row['other_page']} 宣称其在售",
            )

        # (3) 新页在售商品若落在其他页的完整范围内、却未被该页列入 → 同样矛盾。
        page_exclude = "AND pg.page_id != ?" if replace_page_id is not None else ""
        row = conn.execute(
            f"""
            SELECT s.product_id AS pid, pg.page_id AS other_page
            FROM page_staging s
            JOIN pages pg ON pg.version_id=s.version_id
            WHERE s.delivery_id=? {page_exclude}
              AND s.product_id >= pg.range_lo AND s.product_id < pg.range_hi
              AND NOT EXISTS (
                  SELECT 1 FROM evidence ev2
                  WHERE ev2.version_id=s.version_id AND ev2.product_id=s.product_id
                    AND ev2.page_id=pg.page_id
              )
            LIMIT 1
            """,
            [delivery_id, *ex_params],
        ).fetchone()
        if row is not None:
            raise RejectedError(
                "presence_conflict",
                f"页 {h.page_id} 在售商品 {row['pid']} 落在页 {row['other_page']} "
                f"完整范围内却在该页缺席",
            )
