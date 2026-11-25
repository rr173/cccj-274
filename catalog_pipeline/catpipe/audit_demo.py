"""审计历史来源包的可复现示例与自检演练。

运行::

    python3 -I examples/audit_package_example.py --workdir ./audit-demo

脚本会：

1. 建三个供应商的不可变发布库（其中 gamma/3 在所选观察版中明确缺席，
   并带完整覆盖证明），应用一个批次；
2. 导出批次 1 的审计来源包并离线复核；
3. 发布供应商新版本、应用后续批次并撤回一条声明，证明旧包事实不变、
   重新导出的证据字节一致；
4. 删除目标时点的供应商发布版，证明重新导出会严格失败且不产出 DONE，
   但已完成的旧包仍可离线复核；
5. 构造没有 DONE 的半成品，证明其状态是 incomplete、不能当作成品；
6. 额外对现有 ``cross_demo`` 生成的采购库导出一次，证明直接兼容。
"""

from __future__ import annotations

import json
import os
import sqlite3
import sys
import tempfile
from pathlib import Path
from typing import Any, Callable

_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from catpipe.audit import (  # noqa: E402
    AuditError,
    PackageIncomplete,
    export_package,
    package_status,
    verify_package,
)
from catpipe.procurement import CrossCatalog  # noqa: E402
from catpipe.repository import Repository  # noqa: E402

DOMAIN_HI = 1000


def write_supplier(
    db_path: Path,
    versions: dict[str, list[tuple[int, str, int, int]]],
    *,
    full_coverage_proof: bool = True,
) -> None:
    """写一个供应商发布库；默认附带完整区间覆盖证明。"""
    repo = Repository(os.fspath(db_path))
    try:
        for vid, products in versions.items():
            if full_coverage_proof:
                proof = {
                    "version_id": vid,
                    "coverage": {
                        "domain": [0, DOMAIN_HI],
                        "domain_size": DOMAIN_HI,
                        "covered": True,
                        "union_segments": [[0, DOMAIN_HI]],
                        "gaps": [],
                        "page_count": 1,
                    },
                    "pages": [{
                        "page_id": "p-full",
                        "revision": 1,
                        "range": [0, DOMAIN_HI],
                    }],
                }
                proof_json = json.dumps(proof, ensure_ascii=False, sort_keys=True)
            else:
                proof_json = "{}"
            repo.conn.execute(
                "INSERT OR REPLACE INTO published_versions "
                "(version_id,domain_lo,domain_hi,published_at,"
                "coverage_proof_json,manifest_json) VALUES(?,?,?,?,?,?)",
                (vid, 0, DOMAIN_HI, f"{vid}-at", proof_json, "{}"),
            )
            repo.conn.executemany(
                "INSERT OR REPLACE INTO published_products "
                "(version_id,product_id,url,price,product_revision) "
                "VALUES(?,?,?,?,?)",
                [(vid, *p) for p in products],
            )
    finally:
        repo.close()


def _write_json(path: Path, value: Any) -> None:
    path.write_text(
        json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def run_audit_demo(
    workdir: str | os.PathLike[str] | None = None,
    verbose: Callable[[str], None] = print,
) -> dict[str, Any]:
    root = Path(workdir or tempfile.mkdtemp(prefix="catpipe-audit-demo-")).resolve()
    root.mkdir(parents=True, exist_ok=True)

    alpha_db = root / "alpha_books.db"
    beta_db = root / "beta_store.db"
    gamma_db = root / "gamma_shop.db"
    cross_db = root / "procurement_audit.db"

    # ---- 第一批：一个三成员组（gamma/3 明确缺席）+ 一个同价平价组 ----------
    write_supplier(alpha_db, {
        "obs-a1": [
            (10, "https://alpha.example/p/10", 800, 2),
            (2, "https://alpha.example/p/2", 1200, 1),
            (100, "https://alpha.example/p/100", 3000, 1),
        ],
    })
    write_supplier(beta_db, {
        "obs-b1": [
            (7, "https://beta.example/item/7", 700, 3),
            (20, "https://beta.example/item/20", 1300, 1),
        ],
    })
    # gamma/3 故意不写在售行：完整覆盖域 [0,1000) 内无行 = 明确缺席。
    write_supplier(gamma_db, {
        "obs-g1": [
            (5, "https://gamma.example/sku/5", 1200, 1),
        ],
    })

    batch1 = {
        "batch_id": "audit-demo-batch-1",
        "suppliers": [
            {"name": "alpha_books", "db_path": os.fspath(alpha_db),
             "version_id": "obs-a1"},
            {"name": "beta_store", "db_path": os.fspath(beta_db),
             "version_id": "obs-b1"},
            {"name": "gamma_shop", "db_path": os.fspath(gamma_db),
             "version_id": "obs-g1"},
        ],
        "claims": [
            {"op": "add", "claim_id": "c-alpha10-beta7",
             "a": {"supplier": "alpha_books", "product_id": 10},
             "b": {"supplier": "beta_store", "product_id": 7}},
            {"op": "add", "claim_id": "c-beta7-gamma3",
             "a": {"supplier": "beta_store", "product_id": 7},
             "b": {"supplier": "gamma_shop", "product_id": 3}},
            {"op": "add", "claim_id": "c-alpha2-beta20",
             "a": {"supplier": "alpha_books", "product_id": 2},
             "b": {"supplier": "beta_store", "product_id": 20}},
            {"op": "add", "claim_id": "c-alpha2-gamma5",
             "a": {"supplier": "alpha_books", "product_id": 2},
             "b": {"supplier": "gamma_shop", "product_id": 5}},
        ],
    }
    _write_json(root / "batch1.json", batch1)

    catalog = CrossCatalog(os.fspath(cross_db))
    receipt1 = catalog.apply_batch(batch1, include_changes=False)
    catalog.close()
    verbose(f"批次 1 成功：batch_number={receipt1['batch_number']}")

    # ---- 导出 + 离线复核 --------------------------------------------------
    pkg1 = root / "audit-batch-1"
    result1 = export_package(cross_db, 1, pkg1)
    report1 = verify_package(pkg1)
    verbose(
        f"审计包完成：{pkg1}（{result1['group_count']} 组，"
        f"SHA-256 {result1['evidence_sha256'][:16]}…，离线复核 ok={report1['ok']}）"
    )

    first_records = [
        json.loads(line)
        for line in (pkg1 / "evidence.jsonl").read_text(encoding="utf-8").splitlines()
    ]
    absent_group = next(
        r for r in first_records
        if any(m["listing_status"] == "absent" for m in r["members"])
    )
    tie_group = next(
        r for r in first_records
        if r["group_key"] == {"supplier": "alpha_books", "product_id": 2}
    )
    verbose(
        "缺席演示组: "
        + json.dumps(
            {
                "key": absent_group["group_key"],
                "members": [
                    (m["supplier"], m["product_id"], m["listing_status"])
                    for m in absent_group["members"]
                ],
                "winner": absent_group["winner"]["identity"],
            },
            ensure_ascii=False,
        )
    )
    verbose(
        "同价平价组赢家（alpha < gamma）: "
        + json.dumps(tie_group["winner"]["identity"], ensure_ascii=False)
    )

    # ---- 锁定演练：供应商发新版 + 后续批次撤回声明 -------------------------
    write_supplier(alpha_db, {
        "obs-a2": [
            (10, "https://alpha.example/p/10-v2", 650, 3),
            (2, "https://alpha.example/p/2", 1200, 1),
            (101, "https://alpha.example/p/101", 4100, 1),
        ],
    })
    write_supplier(beta_db, {
        "obs-b2": [
            (7, "https://beta.example/item/7-new", 1, 4),
            (20, "https://beta.example/item/20", 1300, 1),
        ],
    })
    catalog = CrossCatalog(os.fspath(cross_db))
    catalog.apply_batch({
        "suppliers": [
            {"name": "alpha_books", "db_path": os.fspath(alpha_db),
             "version_id": "obs-a2"},
            {"name": "beta_store", "db_path": os.fspath(beta_db),
             "version_id": "obs-b2"},
        ],
        "claims": [{"op": "withdraw", "claim_id": "c-alpha10-beta7"}],
    }, include_changes=False)
    latest = catalog.latest_batch_number()
    catalog.close()

    pkg1_later = root / "audit-batch-1-reexported"
    result1_later = export_package(cross_db, 1, pkg1_later)
    locked_same = result1_later["evidence_sha256"] == result1["evidence_sha256"]
    report1_after = verify_package(pkg1)
    verbose(
        f"后续已到批次 {latest}；重导批次 1 的证据 SHA 一致={locked_same}；"
        f"旧包再次复核 ok={report1_after['ok']}"
    )

    # ---- 严格失败演练：删除批次 1 赢家所在的 beta obs-b1 发布版 -------------
    conn = sqlite3.connect(os.fspath(beta_db))
    conn.execute("DELETE FROM published_products WHERE version_id='obs-b1'")
    conn.execute("DELETE FROM published_versions WHERE version_id='obs-b1'")
    conn.commit()
    conn.close()

    failed_pkg = root / "audit-batch-1-should-fail"
    failure_message: str | None = None
    failure_rc: str | None = None
    try:
        export_package(cross_db, 1, failed_pkg)
        failure_rc = "ERROR: export unexpectedly succeeded"
    except AuditError as exc:
        failure_message = str(exc)
        failure_rc = type(exc).__name__
    no_done_after_failure = not (failed_pkg / "DONE").exists()
    verbose(f"源发布版缺失时导出严格失败：{failure_rc}；无 DONE={no_done_after_failure}")
    if failure_message:
        verbose(f"  {failure_message[:140]}")

    # 已完成的旧包不依赖源库，仍然可复核。
    report1_source_lost = verify_package(pkg1)
    verbose(f"源库旧版被删后，已完成包仍可离线复核 ok={report1_source_lost['ok']}")

    # ---- 半成品演练 -------------------------------------------------------
    partial = root / "partial-package"
    partial.mkdir(exist_ok=True)
    (partial / "evidence.jsonl").write_text(
        json.dumps({"partial": True}) + "\n", encoding="utf-8"
    )
    partial_status = package_status(partial)["status"]
    incomplete_rejected = False
    try:
        verify_package(partial)
    except PackageIncomplete:
        incomplete_rejected = True
    verbose(f"半成品状态={partial_status}，读取被拒绝={incomplete_rejected}")

    # ---- 兼容现有 cross_demo 采购库 ---------------------------------------
    cross_demo_dir = root / "cross-demo"
    cross_demo_pkg: Path | None = None
    cross_demo_report_ok: bool | None = None
    try:
        from catpipe.cross_demo import run_demo as run_cross_demo

        run_cross_demo(os.fspath(cross_demo_dir), verbose=lambda _msg: None)
        cross_proc_db = cross_demo_dir / "procurement_catalog.db"
        cross_demo_pkg = root / "audit-cross-demo-batch1"
        export_package(cross_proc_db, 1, cross_demo_pkg)
        cross_demo_report_ok = verify_package(cross_demo_pkg)["ok"]
        verbose(f"现有 cross_demo 采购库批次 1 审计包复核 ok={cross_demo_report_ok}")
    except Exception as exc:  # noqa: BLE001 - 示例中的兼容演示不应掩盖主流程
        verbose(f"cross_demo 兼容演示跳过/失败: {type(exc).__name__}: {exc}")

    summary = {
        "workdir": os.fspath(root),
        "procurement_db": os.fspath(cross_db),
        "package_batch_1": os.fspath(pkg1),
        "package_batch_1_reexported": os.fspath(pkg1_later),
        "evidence_sha256": result1["evidence_sha256"],
        "reexport_sha256": result1_later["evidence_sha256"],
        "locked_identical": locked_same,
        "verify_after_completion": report1["ok"],
        "verify_after_later_batches": report1_after["ok"],
        "verify_after_source_loss": report1_source_lost["ok"],
        "strict_failure": {
            "exception": failure_rc,
            "message": failure_message,
            "no_done": no_done_after_failure,
        },
        "partial_status": partial_status,
        "incomplete_rejected": incomplete_rejected,
        "cross_demo_package": (
            os.fspath(cross_demo_pkg) if cross_demo_pkg is not None else None
        ),
        "cross_demo_verify_ok": cross_demo_report_ok,
        "counts": result1["counts"],
    }
    (root / "audit-demo-summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    verbose("\n可复现命令:")
    audit_cli = _REPO_ROOT / "catpipe" / "audit.py"
    verbose(f"  python3 -I {audit_cli} verify --package {pkg1}")
    verbose(f"  python3 -I {audit_cli} records --package {pkg1}")
    verbose(
        f"  python3 -I {audit_cli} export --db {cross_db} --batch 1 "
        f"--out {root / 'another-audit-b1'}"
    )
    verbose(
        f"  python3 -I {_REPO_ROOT / 'catpipe' / 'cli.py'} audit verify --package {pkg1}"
    )
    return summary


def main() -> int:
    import argparse

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--workdir", help="示例输出目录（默认临时目录）")
    args = parser.parse_args()
    summary = run_audit_demo(args.workdir)
    # 任何关键演练不符合预期时以非零码退出，方便脚本/CI 使用。
    ok = (
        summary["locked_identical"]
        and summary["verify_after_source_loss"]
        and summary["strict_failure"]["exception"] not in (None, "None")
        and summary["strict_failure"]["no_done"]
        and summary["incomplete_rejected"]
    )
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
