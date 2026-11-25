"""按版本发布的商品限制清单（召回/禁采）可复现示例。

运行::

    python3 -I examples/restriction_example.py --workdir ./restriction-demo

会创建两个模拟供应商的已发布 catpipe SQLite 库、两份限制清单 JSON
（r1/r2，r2 是 r1 的修订新版本）与多份批次 JSON，并演示：

1. 无限制批次按最低价选择；
2. 启用限制清单版本 + 核算时点后，仅生效条目排除对应报价，同款关系不变；
3. 仍有可选报价时按现有规则选最低价；组内报价全部被排除时不输出目录记录；
4. 更换核算时点使到期召回自动恢复；清单修订以新版本号发布，不改变旧批次；
5. 清单版本/条目无效时整批失败，不留下部分目录结果；
6. 为成功批次导出审计来源包，证据内嵌清单版本、文件 SHA-256 与每条排除
   报价的记录编号依据，并完成全量离线复核。
"""

from __future__ import annotations

import json
import os
import sys
import tempfile
from pathlib import Path
from typing import Any, Callable

_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from catpipe.audit import export_package, verify_package  # noqa: E402
from catpipe.procurement import CrossCatalog, InvalidBatch  # noqa: E402
from catpipe.repository import Repository  # noqa: E402


def _write_published_supplier(
    db_path: Path, versions: dict[str, list[tuple[int, str, int, int]]]
) -> None:
    repo = Repository(os.fspath(db_path))
    try:
        for version_id, products in versions.items():
            repo.conn.execute(
                "INSERT OR REPLACE INTO published_versions "
                "(version_id,domain_lo,domain_hi,published_at,"
                "coverage_proof_json,manifest_json) VALUES(?,?,?,?,?,?)",
                (version_id, 0, 1_000_000, f"{version_id}-at", "{}", "{}"),
            )
            repo.conn.executemany(
                "INSERT OR REPLACE INTO published_products "
                "(version_id,product_id,url,price,product_revision) "
                "VALUES(?,?,?,?,?)",
                [(version_id, *p) for p in products],
            )
    finally:
        repo.close()


def _write_json(path: Path, value: dict[str, Any]) -> None:
    path.write_text(
        json.dumps(value, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )


def run_demo(
    workdir: str | os.PathLike[str] | None = None,
    verbose: Callable[[str], None] = print,
) -> dict[str, Any]:
    root = Path(workdir or tempfile.mkdtemp(prefix="catpipe-restriction-demo-"))
    root.mkdir(parents=True, exist_ok=True)

    alpha_db = root / "alpha_works.db"
    beta_db = root / "beta_store.db"
    cross_db = root / "procurement_catalog.db"

    # 组 G1：alpha/1 与 beta/1 同款；组 G2：alpha/2 与 beta/2 同款。
    _write_published_supplier(alpha_db, {
        "obs-alpha-1": [
            (1, "https://alpha.example/p/1", 60, 1),
            (2, "https://alpha.example/p/2", 50, 1),
        ],
    })
    _write_published_supplier(beta_db, {
        "obs-beta-1": [
            (1, "https://beta.example/item/1", 80, 1),
            (2, "https://beta.example/item/2", 200, 1),
        ],
    })

    # 限制清单 r1：RC-2026-01 召回 alpha/1（半年窗口）；
    # RC-2026-02 长期禁采 beta/2。
    list_r1 = {
        "list_id": "product-restrictions",
        "version": "2026-r1",
        "entries": [
            {
                "record_id": "RC-2026-01",
                "supplier": "alpha",
                "product_id": 1,
                "effective_from": "2026-01-01",
                "effective_to": "2026-06-30",
            },
            {
                "record_id": "RC-2026-02",
                "supplier": "beta",
                "product_id": 2,
                "effective_from": "2026-03-01",
                "effective_to": None,
            },
        ],
    }
    r1_path = root / "restrictions-2026-r1.json"
    _write_json(r1_path, list_r1)

    # 修订版 r2（新版本号，旧版本 r1 保持不可变）：解除 beta/2 禁采，
    # 新增对 beta/1 的禁采窗口。
    list_r2 = {
        "list_id": "product-restrictions",
        "version": "2026-r2",
        "entries": [
            {
                "record_id": "RC-2026-01",
                "supplier": "alpha",
                "product_id": 1,
                "effective_from": "2026-01-01",
                "effective_to": "2026-06-30",
            },
            {
                "record_id": "RC-2026-03",
                "supplier": "beta",
                "product_id": 1,
                "effective_from": "2026-08-01",
                "effective_to": "2026-12-31",
            },
        ],
    }
    r2_path = root / "restrictions-2026-r2.json"
    _write_json(r2_path, list_r2)

    # 故意无效的清单：生效区间倒置，批次必须失败。
    bad_list = json.loads(json.dumps(list_r1))
    bad_list["version"] = "2026-broken"
    bad_list["entries"][0]["effective_to"] = "2025-01-01"
    bad_path = root / "restrictions-broken.json"
    _write_json(bad_path, bad_list)

    selection = [
        {"name": "alpha", "db_path": os.fspath(alpha_db), "version_id": "obs-alpha-1"},
        {"name": "beta", "db_path": os.fspath(beta_db), "version_id": "obs-beta-1"},
    ]
    claims = [
        {"op": "add", "claim_id": "claim-g1",
         "a": {"supplier": "alpha", "product_id": 1},
         "b": {"supplier": "beta", "product_id": 1}},
        {"op": "add", "claim_id": "claim-g2",
         "a": {"supplier": "alpha", "product_id": 2},
         "b": {"supplier": "beta", "product_id": 2}},
    ]

    batch1 = {
        "comment": "首批：选版 + 同款声明，不应用限制清单。",
        "suppliers": selection,
        "claims": claims,
    }
    batch2 = {
        "comment": "启用限制清单 2026-r1，核算时点 2026-05-01：两条条目都生效。",
        "restriction_list": {
            "list_id": "product-restrictions",
            "version": "2026-r1",
            "as_of": "2026-05-01",
            "path": os.fspath(r1_path),
        },
    }
    batch3 = {
        "comment": "核算时点移到 2026-07-15：RC-2026-01 到期，alpha/1 恢复可选。",
        "restriction_list": {
            "list_id": "product-restrictions",
            "version": "2026-r1",
            "as_of": "2026-07-15",
            "path": os.fspath(r1_path),
        },
    }
    batch4 = {
        "comment": "采用修订版 2026-r2：beta/2 解除、beta/1 在新窗口内被禁采。",
        "restriction_list": {
            "list_id": "product-restrictions",
            "version": "2026-r2",
            "as_of": "2026-09-01",
            "path": os.fspath(r2_path),
        },
    }
    batch_bad = {
        "comment": "故意使用条目无效的清单版本：整批失败，批次号不前进。",
        "restriction_list": {
            "list_id": "product-restrictions",
            "version": "2026-broken",
            "as_of": "2026-05-01",
            "path": os.fspath(bad_path),
        },
    }
    batch_paths = []
    for i, payload in enumerate((batch1, batch2, batch3, batch4, batch_bad), start=1):
        p = root / f"batch{i}.json"
        _write_json(p, payload)
        batch_paths.append(p)

    receipts: list[dict[str, Any]] = []
    failure_message: str | None = None
    with CrossCatalog(cross_db) as catalog:
        receipts.append(catalog.apply_batch(batch_paths[0], include_changes=False))
        receipts.append(catalog.apply_batch(batch_paths[1]))
        receipts.append(catalog.apply_batch(batch_paths[2]))
        receipts.append(catalog.apply_batch(batch_paths[3], include_changes=False))
        try:
            catalog.apply_batch(batch_paths[4], include_changes=False)
        except InvalidBatch as exc:
            failure_message = str(exc)
        latest_batch = catalog.latest_batch_number()
        final_catalog = catalog.catalog()
        excluded_b2 = catalog.excluded_quotes(2)
        contexts = [
            dict(r)
            for r in catalog.conn.execute(
                "SELECT * FROM cc_batch_restriction_context ORDER BY batch_number"
            ).fetchall()
        ]
        lists = catalog.restriction_lists()

    # 为批次 2 导出审计来源包并离线复核。
    pkg_dir = root / "audit-package-batch2"
    export_result = export_package(cross_db, 2, pkg_dir)
    verify_report = verify_package(pkg_dir)

    summary = {
        "workdir": os.fspath(root),
        "cross_db": os.fspath(cross_db),
        "list_files": [r1_path.name, r2_path.name, bad_path.name],
        "batch_files": [p.name for p in batch_paths],
        "latest_batch": latest_batch,
        "failure_message": failure_message,
        "restriction_lists": lists,
        "batch_restriction_contexts": contexts,
        "batch2_excluded_quotes": excluded_b2,
        "final_catalog": final_catalog,
        "audit_package": export_result,
        "audit_verify_ok": verify_report["ok"],
    }
    (root / "demo-summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )

    if verbose:
        verbose(f"示例目录: {root}")
        verbose(f"跨供应商 SQLite: {cross_db}")
        for n, receipt in enumerate(receipts, start=1):
            verbose(f"\n== 批次 {n} ==")
            verbose(json.dumps(
                {"counts": receipt["counts"], "restriction": receipt.get("restriction")},
                ensure_ascii=False, indent=2, sort_keys=True))
        verbose("\n批次 2 被排除的报价及依据:")
        for row in excluded_b2:
            verbose(
                f"  - {row['supplier']}/{row['product_id']} 价格 {row['price']} "
                f"被 {row['list_id']}@{row['version']} 记录 {row['record_id']} "
                f"在 {row['as_of']} 排除（生效 {row['effective_from']}.."
                f"{row['effective_to'] or '开放'}）"
            )
        verbose(f"\n无效清单批次失败信息: {failure_message}")
        verbose(f"最新成功批次号: {latest_batch}（失败批次未占用编号）")
        verbose("\n最终目录:")
        for record in final_catalog:
            restricted = [
                f"{x['supplier']}/{x['product_id']}←{x['record_id']}"
                for x in record.get("restrictions", [])
            ]
            verbose(
                f"  key={record['key_tuple']} winner={record['winning_identity']} "
                f"price={record['price']} 排除={restricted or '无'}"
            )
        verbose(
            f"\n审计包: {pkg_dir} 离线复核={'通过' if verify_report['ok'] else '失败'}"
        )
        procurement_cli = _REPO_ROOT / "catpipe" / "procurement.py"
        audit_cli = _REPO_ROOT / "catpipe" / "audit.py"
        verbose("\n可复现命令:")
        verbose(f"  python3 -I {procurement_cli} restrictions --db {cross_db}")
        verbose(f"  python3 -I {procurement_cli} excluded --db {cross_db} --batch 2")
        verbose(f"  python3 -I {procurement_cli} catalog --db {cross_db} --format jsonl")
        verbose(f"  python3 -I {audit_cli} verify --package {pkg_dir}")
        verbose(f"  python3 -I {audit_cli} records --package {pkg_dir}")

    return summary


def main() -> int:
    import argparse

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--workdir", help="示例输出目录")
    args = parser.parse_args()
    run_demo(args.workdir)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
