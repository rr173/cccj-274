"""审计历史来源包测试：内容完整性、事实锁定、严格失败与崩溃安全。"""

from __future__ import annotations

import json
import os
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from catpipe.audit import (  # noqa: E402
    AuditBatchNotFound,
    AuditConnectionProofMissing,
    AuditSourceMissing,
    AuditSourcePackage,
    PackageIncomplete,
    PackageError,
    PackageVerificationError,
    export_package,
    iter_package_records,
    package_status,
    verify_evidence_record,
    verify_package,
)
from catpipe.procurement import CrossCatalog  # noqa: E402
from catpipe.repository import Repository  # noqa: E402

REPO_ROOT = Path(__file__).resolve().parent.parent


def publish_supplier(path: str, versions: dict, *, proof: str = "{}",
                     domain_hi: int = 1_000_000) -> None:
    repo = Repository(path)
    try:
        for vid, products in versions.items():
            repo.conn.execute(
                "INSERT INTO published_versions VALUES(?,?,?,?,?,?)",
                (vid, 0, domain_hi, f"{vid}-at", proof, "{}"),
            )
            repo.conn.executemany(
                "INSERT INTO published_products "
                "(version_id,product_id,url,price,product_revision) "
                "VALUES(?,?,?,?,?)",
                [(vid, *p) for p in products],
            )
    finally:
        repo.close()


def covered_proof(domain_hi: int = 1000) -> str:
    return json.dumps({
        "coverage": {
            "domain": [0, domain_hi],
            "covered": True,
            "union_segments": [[0, domain_hi]],
            "gaps": [],
        }
    })


class AuditPackageCase(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.mkdtemp(prefix="catpipe-audit-test-")
        self.a_db = os.path.join(self.tmp, "a.db")
        self.b_db = os.path.join(self.tmp, "b.db")
        self.g_db = os.path.join(self.tmp, "g.db")
        self.cross_db = os.path.join(self.tmp, "cross.db")
        self.pkg = os.path.join(self.tmp, "audit-b1")

    def tearDown(self) -> None:
        shutil.rmtree(self.tmp, ignore_errors=True)

    def _build_world(self, *, gamma3_row: bool = False, proof: str = "{}") -> None:
        gamma_products = [(5, "https://g/5", 1200, 1)]
        if gamma3_row:
            gamma_products.append((3, "https://g/3", 700, 1))
        publish_supplier(self.a_db, {"v1": [
            (10, "https://a/10", 800, 2),
            (2, "https://a/2", 1200, 1),
        ]})
        publish_supplier(self.b_db, {"v1": [
            (7, "https://b/7", 700, 3),
            (20, "https://b/20", 1300, 1),
        ]})
        publish_supplier(self.g_db, {"v1": gamma_products}, proof=proof,
                         domain_hi=1000 if proof != "{}" else 1_000_000)
        with CrossCatalog(self.cross_db) as cat:
            cat.apply_batch({
                "suppliers": [
                    {"name": "alpha", "db_path": self.a_db, "version_id": "v1"},
                    {"name": "beta", "db_path": self.b_db, "version_id": "v1"},
                    {"name": "gamma", "db_path": self.g_db, "version_id": "v1"},
                ],
                "claims": [
                    {"op": "add", "claim_id": "c1",
                     "a": ("alpha", 10), "b": ("beta", 7)},
                    {"op": "add", "claim_id": "c2",
                     "a": {"supplier": "beta", "product_id": 7},
                     "b": {"supplier": "gamma", "product_id": 3}},
                    {"op": "add", "claim_id": "c3",
                     "a": ("alpha", 2), "b": ("beta", 20)},
                    {"op": "add", "claim_id": "c4",
                     "a": ("alpha", 2), "b": ("gamma", 5)},
                ],
            }, include_changes=False)

    def _records(self, pkg: str | None = None) -> dict[tuple[str, int], dict]:
        out = {}
        for rec in iter_package_records(pkg or self.pkg):
            out[(rec["group_key"]["supplier"], rec["group_key"]["product_id"])] = rec
        return out

    # ---- 内容完整性 -------------------------------------------------------

    def test_record_contains_versions_rows_absence_winner_and_claims(self):
        self._build_world(proof=covered_proof())
        result = export_package(self.cross_db, 1, self.pkg)
        self.assertEqual(result["status"], "complete")
        self.assertTrue(os.path.isfile(os.path.join(self.pkg, "DONE")))
        self.assertTrue(os.path.isfile(os.path.join(self.pkg, "manifest.json")))
        self.assertTrue(os.path.isfile(os.path.join(self.pkg, "evidence.jsonl")))

        report = verify_package(self.pkg)
        self.assertTrue(report["ok"])
        self.assertEqual(report["group_count"], 2)

        groups = self._records()
        g = groups[("alpha", 10)]
        self.assertEqual(
            [(m["supplier"], m["product_id"]) for m in g["members"]],
            [("alpha", 10), ("beta", 7), ("gamma", 3)],
        )
        # 每个成员都带出供应商实际采用的观察版。
        for m in g["members"]:
            self.assertEqual(m["adopted_version_id"], "v1")
            self.assertEqual(g["supplier_versions"][m["supplier"]]["version_id"], "v1")

        by_id = {(m["supplier"], m["product_id"]): m for m in g["members"]}
        self.assertEqual(by_id[("alpha", 10)]["listing_status"], "on_sale")
        self.assertEqual(
            by_id[("alpha", 10)]["source_row"]["url"], "https://a/10"
        )
        # gamma/3 在覆盖域内但无行 -> 明确缺席，且覆盖区间可验算覆盖该 ID。
        absent = by_id[("gamma", 3)]
        self.assertEqual(absent["listing_status"], "absent")
        ae = absent["absence_evidence"]
        self.assertTrue(ae["product_within_domain"])
        self.assertTrue(ae["coverage_proof_available"])
        self.assertTrue(ae["id_covered_by_union_segments"])

        # 最低报价：beta/7 与（缺席的 gamma/3 不参与）；beta 700 最低。
        self.assertEqual(g["winner"]["identity"],
                         {"supplier": "beta", "product_id": 7})
        csr = g["winner"]["complete_source_row"]
        self.assertEqual(csr["price"], 700)
        self.assertEqual(csr["url"], "https://b/7")
        self.assertEqual(csr["version_id"], "v1")
        self.assertTrue(g["winner"]["source_row_matches_catalog_row"])

        # 当时有效且足以连通整组的声明编号 + 连接证据。
        self.assertEqual(g["connection_evidence"]["claim_ids"], ["c1", "c2"])
        self.assertEqual(g["connection_evidence"]["connecting_claim_ids"], ["c1", "c2"])
        self.assertTrue(g["connection_evidence"]["all_vertices_connected"])
        self.assertEqual({c["claim_id"] for c in g["claims"]}, {"c1", "c2"})

        # 平价组：alpha/2 与 gamma/5 同为 1200 -> 供应商名字典序 alpha 胜。
        tie = groups[("alpha", 2)]
        self.assertEqual(tie["winner"]["identity"],
                         {"supplier": "alpha", "product_id": 2})
        self.assertEqual(
            tie["connection_evidence"]["claim_ids"], ["c3", "c4"]
        )

        # 每条记录自身的纯函数复核也必须通过。
        for rec in self._records().values():
            self.assertEqual(verify_evidence_record(rec), [])

    def test_duplicate_claim_ids_same_edge_both_listed(self):
        publish_supplier(self.a_db, {"v1": [(1, "a1", 10, 1)]})
        publish_supplier(self.b_db, {"v1": [(2, "b2", 20, 1)]})
        publish_supplier(self.g_db, {"v1": []})
        with CrossCatalog(self.cross_db) as cat:
            cat.apply_batch({
                "suppliers": [
                    {"name": "alpha", "db_path": self.a_db, "version_id": "v1"},
                    {"name": "beta", "db_path": self.b_db, "version_id": "v1"},
                ],
                "claims": [
                    {"op": "add", "claim_id": "c1", "a": ("alpha", 1), "b": ("beta", 2)},
                    {"op": "add", "claim_id": "c2", "a": ("alpha", 1), "b": ("beta", 2)},
                ],
            }, include_changes=False)
        export_package(self.cross_db, 1, self.pkg)
        g = self._records()[("alpha", 1)]
        self.assertEqual(g["connection_evidence"]["claim_ids"], ["c1", "c2"])
        # 生成树只需其中一条边。
        self.assertEqual(len(g["connection_evidence"]["connecting_claim_ids"]), 1)
        verify_package(self.pkg)

    # ---- 事实锁定 ---------------------------------------------------------

    def test_later_withdraw_replace_and_new_versions_do_not_rewrite_package(self):
        self._build_world()
        first = export_package(self.cross_db, 1, self.pkg)
        hash_before = first["evidence_sha256"]

        # 供应商发布新版本（不可变发布版新增一行，旧 v1 保留）。
        publish_supplier(self.a_db, {"v2": [
            (10, "https://a/10-new", 1, 9),
            (2, "https://a/2", 1200, 1),
        ]})
        # 后续批次：alpha 换版 + 撤回 c1 + 替换 c3。
        with CrossCatalog(self.cross_db) as cat:
            cat.apply_batch({
                "suppliers": [{"name": "alpha", "db_path": self.a_db,
                               "version_id": "v2"}],
                "claims": [
                    {"op": "withdraw", "claim_id": "c1"},
                    {"op": "replace", "claim_id": "c3",
                     "a": ("alpha", 2), "b": ("gamma", 5)},
                ],
            }, include_changes=False)

        # 旧包文件没有被任何后续操作改动，仍可离线复核。
        self.assertTrue(verify_package(self.pkg)["ok"])

        # 重新导出批次 1：证据必须字节一致（锁定 N 时点事实）。
        again = os.path.join(self.tmp, "audit-b1-locked")
        second = export_package(self.cross_db, 1, again)
        self.assertEqual(second["evidence_sha256"], hash_before)
        self.assertEqual(
            Path(self.pkg, "evidence.jsonl").read_bytes(),
            Path(again, "evidence.jsonl").read_bytes(),
        )
        g = self._records(again)[("alpha", 10)]
        self.assertEqual(g["connection_evidence"]["claim_ids"], ["c1", "c2"])
        self.assertEqual(g["winner"]["complete_source_row"]["price"], 700)
        self.assertEqual(g["supplier_versions"]["alpha"]["version_id"], "v1")

        # 批次 2 的包反映新事实：c1 已撤回，alpha10 成为单成员组且采用 v2。
        p2 = os.path.join(self.tmp, "audit-b2")
        export_package(self.cross_db, 2, p2)
        b2 = {(r["group_key"]["supplier"], r["group_key"]["product_id"]): r
              for r in iter_package_records(p2)}
        self.assertIn(("alpha", 10), b2)
        self.assertEqual(b2[("alpha", 10)]["supplier_versions"]["alpha"]["version_id"], "v2")
        verify_package(p2)

    # ---- 严格失败 ---------------------------------------------------------

    def test_unknown_batch_fails(self):
        self._build_world()
        with self.assertRaises(AuditBatchNotFound):
            export_package(self.cross_db, 99, os.path.join(self.tmp, "x"))

    def test_missing_target_published_version_fails_without_done(self):
        self._build_world()
        # 删掉 beta 的目标发布版（模拟目标时点发布版已无法获取）。
        conn = sqlite3.connect(self.b_db)
        conn.execute("DELETE FROM published_versions WHERE version_id='v1'")
        conn.commit()
        conn.close()
        out = os.path.join(self.tmp, "fail-version")
        with self.assertRaises(AuditSourceMissing):
            export_package(self.cross_db, 1, out)
        self.assertFalse(os.path.exists(os.path.join(out, "DONE")))
        # 失败暂存目录已清理，目标目录不应成为成品。
        self.assertEqual(package_status(out)["status"], "absent")

    def test_missing_winner_source_row_fails(self):
        self._build_world()
        # alpha/10 价格 800、beta/7 700：赢家是 beta/7；删赢家行。
        conn = sqlite3.connect(self.b_db)
        conn.execute("DELETE FROM published_products WHERE version_id='v1' AND product_id=7")
        conn.commit()
        conn.close()
        out = os.path.join(self.tmp, "fail-row")
        with self.assertRaises(AuditSourceMissing):
            export_package(self.cross_db, 1, out)
        self.assertFalse(os.path.exists(os.path.join(out, "DONE")))

    def test_non_winner_missing_row_is_explicit_absence_not_failure(self):
        self._build_world()  # gamma/3 本来就无行且 proof='{}'
        export_package(self.cross_db, 1, self.pkg)
        g = self._records()[("alpha", 10)]
        gamma3 = next(m for m in g["members"] if m["product_id"] == 3)
        self.assertEqual(gamma3["listing_status"], "absent")
        # 合成库的空覆盖证明也允许基于“完整发布版行 + 域内无行”的缺席事实。
        self.assertFalse(gamma3["absence_evidence"]["coverage_proof_available"])
        verify_package(self.pkg)

    def test_missing_connection_proof_fails(self):
        self._build_world()
        conn = sqlite3.connect(self.cross_db)
        conn.execute("UPDATE cc_batches SET input_json='{}'")
        conn.execute("DELETE FROM cc_claims")
        conn.execute("DELETE FROM cc_claim_endpoints")
        conn.commit()
        conn.close()
        out = os.path.join(self.tmp, "fail-conn")
        with self.assertRaises(AuditConnectionProofMissing):
            export_package(self.cross_db, 1, out)
        self.assertFalse(os.path.exists(os.path.join(out, "DONE")))

    # ---- 完成标记 / 重启读取 / 防篡改 -------------------------------------

    def test_incomplete_package_is_not_a_product(self):
        os.makedirs(self.pkg, exist_ok=True)
        Path(self.pkg, "evidence.jsonl").write_text('{"partial":true}\n', encoding="utf-8")
        self.assertEqual(package_status(self.pkg)["status"], "incomplete")
        with self.assertRaises(PackageIncomplete):
            verify_package(self.pkg)
        with self.assertRaises(PackageIncomplete):
            list(iter_package_records(self.pkg))
        # 但底层流式读取在显式要求下仍可用于排障。
        recs = list(iter_package_records(self.pkg, require_complete=False))
        self.assertEqual(recs, [{"partial": True}])

    def test_completed_package_readable_in_fresh_subprocess(self):
        self._build_world()
        export_package(self.cross_db, 1, self.pkg)
        proc = subprocess.run(
            [sys.executable, "-I",
             str(REPO_ROOT / "catpipe" / "audit.py"),
             "verify", "--package", self.pkg],
            capture_output=True, text=True,
        )
        self.assertEqual(proc.returncode, 0, proc.stderr)
        payload = json.loads(proc.stdout)
        self.assertTrue(payload["ok"])

    def test_tampered_evidence_fails_verification(self):
        self._build_world()
        export_package(self.cross_db, 1, self.pkg)
        ev = Path(self.pkg, "evidence.jsonl")
        lines = ev.read_text(encoding="utf-8").splitlines()
        rec = json.loads(lines[0])
        rec["winner"]["complete_source_row"]["price"] = -1
        lines[0] = json.dumps(rec, ensure_ascii=False, sort_keys=True,
                              separators=(",", ":"))
        ev.write_text("\n".join(lines) + "\n", encoding="utf-8")
        with self.assertRaises(PackageVerificationError):
            verify_package(self.pkg)

    def test_semantic_verifier_catches_winner_and_graph_tampering(self):
        self._build_world()
        export_package(self.cross_db, 1, self.pkg)
        rec = next(iter(self._records().values()))
        bad_winner = json.loads(json.dumps(rec))
        bad_winner["winner"]["complete_source_row"]["price"] = 1
        self.assertTrue(any("complete_source_row" in p
                            for p in verify_evidence_record(bad_winner)))

        bad_graph = json.loads(json.dumps(rec))
        # 删掉一条声明边，连通分量将小于 members。
        if bad_graph["claims"]:
            bad_graph["claims"].pop()
        problems = verify_evidence_record(bad_graph)
        self.assertTrue(any("连通分量" in p for p in problems), problems)

    def test_file_mode_sidecars_and_verify(self):
        self._build_world()
        jsonl = os.path.join(self.tmp, "evidence-b1.jsonl")
        result = export_package(self.cross_db, 1, jsonl)
        self.assertEqual(result["mode"], "file")
        self.assertTrue(os.path.isfile(jsonl))
        self.assertTrue(os.path.isfile(jsonl.replace(".jsonl", ".manifest.json")))
        self.assertTrue(os.path.isfile(jsonl.replace(".jsonl", ".done")))
        self.assertTrue(verify_package(jsonl)["ok"])
        self.assertEqual(package_status(jsonl)["status"], "complete")

    def test_existing_completed_package_refuses_overwrite_without_force(self):
        self._build_world()
        export_package(self.cross_db, 1, self.pkg)
        with self.assertRaises(PackageError):
            export_package(self.cross_db, 1, self.pkg)
        # force 可以重建且仍然完整。
        export_package(self.cross_db, 1, self.pkg, force=True)
        self.assertTrue(verify_package(self.pkg)["ok"])

    def test_cross_catalog_method_entrypoint(self):
        self._build_world()
        with CrossCatalog(self.cross_db) as cat:
            result = cat.export_audit_package(1, self.pkg)
        self.assertEqual(result["group_count"], 2)
        self.assertTrue(verify_package(self.pkg)["ok"])
        # 类级别名存在。
        self.assertIsNotNone(CrossCatalog.export_audit)
        self.assertIsNotNone(AuditSourcePackage.export)

    # ---- 超内存设计：大量单成员组流式导出/复核 -----------------------------

    def test_large_catalog_streams_all_groups(self):
        n = 2500
        publish_supplier(self.a_db, {"big": [
            (i, f"https://a/{i}", 100 + (i % 7), 1) for i in range(n)
        ]})
        with CrossCatalog(self.cross_db) as cat:
            cat.apply_batch({
                "suppliers": [{"name": "alpha", "db_path": self.a_db,
                               "version_id": "big"}],
            }, include_changes=False)
        result = export_package(self.cross_db, None, self.pkg)  # 默认最新批次
        self.assertEqual(result["group_count"], n)
        report = verify_package(self.pkg)
        self.assertTrue(report["ok"])
        self.assertEqual(report["stats"]["groups"], n)
        # 流式迭代器不应一次性转成列表也能逐行计数。
        count = sum(1 for _ in iter_package_records(self.pkg))
        self.assertEqual(count, n)

    # ---- 可复现示例 -------------------------------------------------------

    def test_reproducible_audit_demo_all_drills_pass(self):
        from catpipe.audit_demo import run_audit_demo

        work = os.path.join(self.tmp, "audit-demo")
        summary = run_audit_demo(work, verbose=lambda _msg: None)
        self.assertTrue(summary["locked_identical"])
        self.assertTrue(summary["verify_after_source_loss"])
        self.assertTrue(summary["verify_after_later_batches"])
        self.assertEqual(summary["strict_failure"]["exception"], "AuditSourceMissing")
        self.assertTrue(summary["strict_failure"]["no_done"])
        self.assertTrue(summary["incomplete_rejected"])
        self.assertEqual(summary["partial_status"], "incomplete")
        self.assertTrue(summary["cross_demo_verify_ok"])
        self.assertTrue(verify_package(summary["package_batch_1"])["ok"])


if __name__ == "__main__":
    unittest.main(verbosity=2)
