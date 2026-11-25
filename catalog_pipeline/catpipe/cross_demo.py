"""可复现的跨供应商目录示例。

运行::

    python3 -I examples/cross_catalog_example.py --workdir ./cross-demo

会创建三个模拟供应商的已发布 catpipe SQLite 库、若干批次 JSON，并用
:class:`catpipe.procurement.CrossCatalog` 应用三批变化。
"""

from __future__ import annotations

import json
import os
import sys
import tempfile
from pathlib import Path
from typing import Any, Callable

# 允许直接 `python3 -I examples/cross_catalog_example.py` 运行。
_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from catpipe.procurement import CrossCatalog, InvalidBatch  # noqa: E402
from catpipe.repository import Repository  # noqa: E402


def write_published_supplier(
    db_path: Path,
    versions: dict[str, list[tuple[int, str, int, int]]],
) -> None:
    """用 catpipe 自己的 SQLite schema 写一个“已发布”供应商库。

    实际环境中这些库由现有采集/发布流水线生成；示例为了可复现，直接写发布表。
    """
    repo = Repository(os.fspath(db_path))
    try:
        for version_id, products in versions.items():
            repo.conn.execute(
                "INSERT OR REPLACE INTO published_versions "
                "(version_id,domain_lo,domain_hi,published_at,"
                "coverage_proof_json,manifest_json) "
                "VALUES(?,?,?,?,?,?)",
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
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n",
                    encoding="utf-8")


def run_demo(workdir: str | os.PathLike[str] | None = None,
             verbose: Callable[[str], None] = print) -> dict[str, Any]:
    root = Path(workdir or tempfile.mkdtemp(prefix="catpipe-cross-demo-"))
    root.mkdir(parents=True, exist_ok=True)

    alpha_db = root / "alpha_books.db"
    beta_db = root / "beta_store.db"
    gamma_db = root / "gamma_shop.db"
    cross_db = root / "procurement_catalog.db"

    # 第一批源版本：相同价格用于演示“平价按身份排序”。
    write_published_supplier(alpha_db, {
        "obs-alpha-2026-01": [
            (2, "https://alpha.example/p/2", 1200, 1),
            (10, "https://alpha.example/p/10", 800, 2),
            (100, "https://alpha.example/p/100", 3000, 1),
        ],
        # 故意提前写一个更新但本示例先不选版；未指定换版时不能跟随它。
        "obs-alpha-2026-02": [
            (2, "https://alpha.example/p/2", 1200, 1),
            (10, "https://alpha.example/p/10-v2", 650, 3),
            (101, "https://alpha.example/p/101", 4100, 1),
        ],
    })
    write_published_supplier(beta_db, {
        "obs-beta-1": [
            (7, "https://beta.example/item/7", 700, 3),
            (20, "https://beta.example/item/20", 1300, 1),
            (21, "https://beta.example/item/21", 990, 2),
        ],
        # 未在第二批选版；即使这里有异常低价，也不能影响采购目录。
        "obs-beta-2-unselected": [
            (7, "https://beta.example/item/7-new", 1, 4),
            (20, "https://beta.example/item/20", 1300, 1),
            (21, "https://beta.example/item/21", 990, 2),
        ],
    })
    write_published_supplier(gamma_db, {
        "obs-gamma-9": [
            (3, "https://gamma.example/sku/3", 700, 1),
            (5, "https://gamma.example/sku/5", 500, 1),
        ],
    })

    batch1 = {
        "comment": "首次选版 + 同款声明；c1/c2 形成传递关系，c4 连接两个暂不在售商品。",
        "suppliers": [
            {"name": "alpha_books", "db_path": os.fspath(alpha_db),
             "version_id": "obs-alpha-2026-01"},
            {"name": "beta_store", "db_path": os.fspath(beta_db),
             "version_id": "obs-beta-1"},
            {"name": "gamma_shop", "db_path": os.fspath(gamma_db),
             "version_id": "obs-gamma-9"},
        ],
        "claims": [
            {"op": "add", "claim_id": "claim-alpha10-beta7",
             "a": {"supplier": "alpha_books", "product_id": 10},
             "b": {"supplier": "beta_store", "product_id": 7}},
            {"op": "add", "claim_id": "claim-beta7-gamma3",
             "a": {"supplier": "beta_store", "product_id": 7},
             "b": {"supplier": "gamma_shop", "product_id": 3}},
            {"op": "add", "claim_id": "claim-alpha2-beta20",
             "a": {"supplier": "alpha_books", "product_id": 2},
             "b": {"supplier": "beta_store", "product_id": 20}},
            {"op": "add", "claim_id": "claim-offsale",
             "a": {"supplier": "alpha_books", "product_id": 999001},
             "b": {"supplier": "beta_store", "product_id": 888002}},
        ],
    }
    batch1_path = root / "batch1.json"
    _write_json(batch1_path, batch1)

    catalog = CrossCatalog(cross_db)
    receipt1 = catalog.apply_batch(batch1_path, include_changes=True)
    catalog.close()

    # 第二批：alpha 换版（10 降价、100 缺货、101 新在售）；替换一条声明。
    batch2 = {
        "comment": "alpha 换版；把 alpha2 的同款从 beta20 替换为 gamma5。",
        "suppliers": [
            {"name": "alpha_books", "db_path": os.fspath(alpha_db),
             "version_id": "obs-alpha-2026-02"},
        ],
        "claims": [
            {"op": "replace", "claim_id": "claim-alpha2-beta20",
             "a": {"supplier": "alpha_books", "product_id": 2},
             "b": {"supplier": "gamma_shop", "product_id": 5}},
        ],
    }
    batch2_path = root / "batch2.json"
    _write_json(batch2_path, batch2)
    catalog = CrossCatalog(cross_db)
    receipt2 = catalog.apply_batch(batch2_path, include_changes=True)

    # 第三批：撤回传递链中的一条边，演示分组分裂和新增键。
    batch3 = {
        "comment": "选 beta 新版并撤回 alpha10=beta7；beta7=gamma3 仍有效。",
        "suppliers": [
            {"name": "beta_store", "db_path": os.fspath(beta_db),
             "version_id": "obs-beta-2-unselected"},
        ],
        "claims": [
            {"op": "withdraw", "claim_id": "claim-alpha10-beta7"},
        ],
    }
    batch3_path = root / "batch3.json"
    _write_json(batch3_path, batch3)
    receipt3 = catalog.apply_batch(batch3_path, include_changes=True)

    invalid_batch = {
        "comment": "故意引用不存在的发布版；整批必须失败，批次号仍停在 3。",
        "suppliers": [
            {"name": "alpha_books", "db_path": os.fspath(alpha_db),
             "version_id": "does-not-exist"},
        ],
    }
    invalid_path = root / "batch-invalid.json"
    _write_json(invalid_path, invalid_batch)
    failure_message = None
    try:
        catalog.apply_batch(invalid_path, include_changes=False)
    except InvalidBatch as exc:
        failure_message = str(exc)

    latest_before_reopen = catalog.latest_batch_number()
    catalog.close()

    # 重开数据库，验证已成功批次变更持久可读。
    reopened = CrossCatalog(cross_db)
    latest_after_reopen = reopened.latest_batch_number()
    persisted_changes_1 = reopened.get_changes(1)
    persisted_changes_2 = reopened.get_changes(2)
    persisted_changes_3 = reopened.get_changes(3)
    final_records = reopened.catalog()
    reopened.close()

    summary = {
        "workdir": os.fspath(root),
        "cross_db": os.fspath(cross_db),
        "batch_files": [p.name for p in (batch1_path, batch2_path, batch3_path, invalid_path)],
        "latest_batch_before_reopen": latest_before_reopen,
        "latest_batch_after_reopen": latest_after_reopen,
        "failure_message": failure_message,
        "receipts": [
            {k: v for k, v in receipt1.items() if k != "changes"},
            {k: v for k, v in receipt2.items() if k != "changes"},
            {k: v for k, v in receipt3.items() if k != "changes"},
        ],
        "change_counts": [
            persisted_changes_1["counts"],
            persisted_changes_2["counts"],
            persisted_changes_3["counts"],
        ],
        "final_catalog": final_records,
    }
    (root / "demo-summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )

    if verbose:
        verbose(f"示例目录: {root}")
        verbose(f"跨供应商 SQLite: {cross_db}")
        for i, receipt in enumerate((receipt1, receipt2, receipt3), start=1):
            verbose(f"\n== 批次 {i} 摘要 ==")
            verbose(json.dumps(receipt["counts"], ensure_ascii=False, indent=2, sort_keys=True))
            verbose("变更键:")
            for event in receipt["changes"]:
                record = event.get("record")
                winner = (record or {}).get("winning_identity")
                verbose(
                    f"  - {event['change_type']:7s} {event['key']}"
                    + (f" winner={winner} price={(record or {}).get('price')}"
                       if record else "")
                )
        verbose(f"\n无效批次失败信息: {failure_message}")
        verbose(f"重开后最新批次: {latest_after_reopen}")
        verbose("\n最终目录:")
        for record in final_records:
            verbose(
                f"  key={record['key_tuple']} "
                f"members={[(m['supplier'], m['product_id']) for m in record['members']]} "
                f"winner={record['winning_identity']} price={record['price']}"
            )
        verbose("\n可复现命令:")
        procurement_cli = _REPO_ROOT / "catpipe" / "procurement.py"
        verbose(f"  python3 -I {procurement_cli} batches --db {cross_db}")
        verbose(f"  python3 -I {procurement_cli} changes --db {cross_db} --batch 2")
        verbose(f"  python3 -I {procurement_cli} catalog --db {cross_db} --format jsonl")
        verbose(f"  python3 -I {procurement_cli} serve --db {cross_db} --port 8091")

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
