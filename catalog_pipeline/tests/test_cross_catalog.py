"""采购端跨供应商目录与批次变更测试。"""

from __future__ import annotations

import json
import os
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from catpipe.procurement import (  # noqa: E402
    CrossCatalog,
    InvalidBatch,
    apply_batch,
)
from catpipe.repository import Repository  # noqa: E402


def publish_supplier(path: str, versions: dict[str, list[tuple]]) -> None:
    repo = Repository(path)
    try:
        for vid, products in versions.items():
            repo.conn.execute(
                "INSERT INTO published_versions VALUES(?,?,?,?,?,?)",
                (vid, 0, 1_000_000, f"{vid}-at", "{}", "{}"),
            )
            repo.conn.executemany(
                "INSERT INTO published_products "
                "(version_id,product_id,url,price,product_revision) "
                "VALUES(?,?,?,?,?)",
                [(vid, *p) for p in products],
            )
    finally:
        repo.close()


class CrossCatalogCase(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.mkdtemp(prefix="catpipe-cross-test-")
        self.a_db = os.path.join(self.tmp, "a.db")
        self.b_db = os.path.join(self.tmp, "b.db")
        self.c_db = os.path.join(self.tmp, "c.db")
        self.cross_db = os.path.join(self.tmp, "cross.db")

    def supplier_batch(self, version: str = "v1") -> dict:
        return {
            "suppliers": [
                {"name": "alpha", "db_path": self.a_db, "version_id": version},
                {"name": "beta", "db_path": self.b_db, "version_id": version},
                {"name": "gamma", "db_path": self.c_db, "version_id": version},
            ]
        }

    def open_catalog(self) -> CrossCatalog:
        return CrossCatalog(self.cross_db)

    def record_map(self, catalog: CrossCatalog) -> dict[tuple[str, int], dict]:
        return {(r["group_supplier"], r["group_product_id"]): r
                for r in catalog.conn.execute("SELECT * FROM cc_catalog")}

    def test_group_key_numeric_order_transitivity_tie_and_no_quote_omitted(self):
        publish_supplier(self.a_db, {"v1": [
            (2, "https://a/2", 100, 1),
            (10, "https://a/10", 10, 2),
            (20, "https://a/20", 5, 1),
        ]})
        publish_supplier(self.b_db, {"v1": [
            (1, "https://b/1", 3, 4),
            (2, "https://b/2", 3, 1),
        ]})
        publish_supplier(self.c_db, {"v1": [
            (3, "https://c/3", 3, 9),
        ]})

        batch = self.supplier_batch()
        batch["claims"] = [
            {"op": "add", "claim_id": "a2-a10",
             "a": ("alpha", 2), "b": ("alpha", 10)},
            {"op": "add", "claim_id": "a10-b1",
             "a": {"supplier": "alpha", "product_id": 10},
             "b": {"supplier": "beta", "product_id": 1}},
            {"op": "add", "claim_id": "b2-c3",
             "a": {"supplier": "beta", "product_id": 2},
             "b": {"supplier": "gamma", "product_id": 3}},
            # 两个商品都不在售：关系保留，但无报价组不进目录。
            {"op": "add", "claim_id": "offsale",
             "a": {"supplier": "alpha", "product_id": 999},
             "b": {"supplier": "beta", "product_id": 888}},
        ]
        with self.open_catalog() as catalog:
            receipt = catalog.apply_batch(batch)
            self.assertEqual(receipt["batch_number"], 1)
            self.assertEqual(receipt["counts"]["changes_added"], 3)

            rows = self.record_map(catalog)
            self.assertEqual(
                sorted(rows),
                [("alpha", 2), ("alpha", 20), ("beta", 2)],
            )

            big = json.loads(rows[("alpha", 2)]["record_json"])
            self.assertEqual(
                big["members"],
                [
                    {"supplier": "alpha", "product_id": 2},
                    {"supplier": "alpha", "product_id": 10},
                    {"supplier": "beta", "product_id": 1},
                ],
            )
            # 最低报价是 beta/1，价格 3。
            self.assertEqual(big["supplier"], "beta")
            self.assertEqual(big["product_id"], 1)
            self.assertEqual(big["url"], "https://b/1")
            self.assertEqual(big["price"], 3)
            self.assertEqual(big["product_revision"], 4)

            tie = json.loads(rows[("beta", 2)]["record_json"])
            # 平价时按身份排序：beta/2 小于 gamma/3。
            self.assertEqual(tie["winning_identity"],
                             {"supplier": "beta", "product_id": 2})

            # 无报价声明组仍有成员关系，只是没有目录行。
            off = catalog.conn.execute(
                "SELECT COUNT(*) c FROM cc_group_members "
                "WHERE group_supplier='alpha' AND group_product_id=999"
            ).fetchone()["c"]
            self.assertEqual(off, 2)

    def test_merge_emits_changed_old_key_and_removed_other_key(self):
        publish_supplier(self.a_db, {"v1": [(1, "a1", 10, 1)]})
        publish_supplier(self.b_db, {"v1": [(2, "b2", 20, 1)]})
        publish_supplier(self.c_db, {"v1": []})

        with self.open_catalog() as catalog:
            catalog.apply_batch({
                "suppliers": [
                    {"name": "alpha", "db_path": self.a_db, "version_id": "v1"},
                    {"name": "beta", "db_path": self.b_db, "version_id": "v1"},
                ]
            }, include_changes=False)
            receipt = catalog.apply_batch({"claims": [
                {"op": "add", "claim_id": "c1",
                 "a": {"supplier": "alpha", "product_id": 1},
                 "b": {"supplier": "beta", "product_id": 2}},
            ]})

            changes = {(e["change_type"], tuple(e["key_tuple"])): e
                       for e in receipt["changes"]}
            self.assertIn(("changed", ("alpha", 1)), changes)
            self.assertIn(("removed", ("beta", 2)), changes)
            merged = changes[("changed", ("alpha", 1))]["record"]
            self.assertEqual([m["product_id"] for m in merged["members"]], [1, 2])
            self.assertEqual(merged["price"], 10)

    def test_duplicate_claims_support_relation_until_last_withdrawal(self):
        publish_supplier(self.a_db, {"v1": [(1, "a1", 10, 1)]})
        publish_supplier(self.b_db, {"v1": [(2, "b2", 20, 1)]})
        publish_supplier(self.c_db, {"v1": []})

        with self.open_catalog() as catalog:
            catalog.apply_batch({
                "suppliers": [
                    {"name": "alpha", "db_path": self.a_db, "version_id": "v1"},
                    {"name": "beta", "db_path": self.b_db, "version_id": "v1"},
                ],
                "claims": [
                    {"op": "add", "claim_id": "c1", "a": ("alpha", 1), "b": ("beta", 2)},
                    {"op": "add", "claim_id": "c2", "a": ("alpha", 1), "b": ("beta", 2)},
                ],
            }, include_changes=False)

            r2 = catalog.apply_batch({"claims": [
                {"op": "withdraw", "claim_id": "c1"}
            ]})
            self.assertEqual(r2["counts"]["changes_added"], 0)
            self.assertEqual(r2["counts"]["changes_changed"], 0)
            self.assertEqual(r2["counts"]["changes_removed"], 0)
            self.assertEqual(len(catalog.catalog()), 1)

            r3 = catalog.apply_batch({"claims": [
                {"op": "withdraw", "claim_id": "c2"}
            ]})
            kinds = [(e["change_type"], e["key_tuple"]) for e in r3["changes"]]
            self.assertIn(("changed", ["alpha", 1]), kinds)
            self.assertIn(("added", ["beta", 2]), kinds)
            self.assertEqual(len(catalog.catalog()), 2)

    def test_selected_version_is_pinned_and_only_affected_row_rewritten(self):
        publish_supplier(self.a_db, {
            "v1": [(1, "a1-v1", 10, 1)],
            "v2": [(1, "a1-v2", 12, 2), (3, "a3", 7, 1)],
        })
        publish_supplier(self.b_db, {
            "v1": [(2, "b2-v1", 20, 1)],
            "v2": [(2, "b2-v2-should-not-follow", 1, 99)],
        })
        publish_supplier(self.c_db, {"v1": []})

        with self.open_catalog() as catalog:
            catalog.apply_batch({
                "suppliers": [
                    {"name": "alpha", "db_path": self.a_db, "version_id": "v1"},
                    {"name": "beta", "db_path": self.b_db, "version_id": "v1"},
                ]
            }, include_changes=False)

            # 源库之后新增 v2，但本批只给 alpha 换版；beta 必须沿用已复制的 v1。
            receipt = catalog.apply_batch({
                "suppliers": [
                    {"name": "alpha", "db_path": self.a_db, "version_id": "v2"},
                ]
            }, include_changes=False)
            self.assertEqual(receipt["counts"]["quotes_changed"], 1)
            self.assertEqual(receipt["counts"]["quotes_inserted"], 1)

            rows = self.record_map(catalog)
            a1 = json.loads(rows[("alpha", 1)]["record_json"])
            self.assertEqual(a1["url"], "a1-v2")
            self.assertEqual(a1["price"], 12)
            b2 = json.loads(rows[("beta", 2)]["record_json"])
            self.assertEqual(b2["url"], "b2-v1")
            self.assertEqual(b2["price"], 20)

            # 未受影响的 beta 行没有在批次 2 重写。
            self.assertEqual(rows[("beta", 2)]["batch_number"], 1)
            self.assertEqual(rows[("alpha", 1)]["batch_number"], 2)

            # 第三批显式选 beta v2 后才跟随。
            catalog.apply_batch({
                "suppliers": [
                    {"name": "beta", "db_path": self.b_db, "version_id": "v2"},
                ]
            }, include_changes=False)
            rows = self.record_map(catalog)
            b2 = json.loads(rows[("beta", 2)]["record_json"])
            self.assertEqual(b2["url"], "b2-v2-should-not-follow")
            self.assertEqual(b2["price"], 1)

    def test_failed_batch_is_atomic_and_reopen_reads_persisted_changes(self):
        publish_supplier(self.a_db, {"v1": [(1, "a1", 10, 1)]})
        publish_supplier(self.b_db, {"v1": [(2, "b2", 20, 1)]})
        publish_supplier(self.c_db, {"v1": []})

        with self.open_catalog() as catalog:
            catalog.apply_batch({
                "suppliers": [
                    {"name": "alpha", "db_path": self.a_db, "version_id": "v1"},
                ]
            }, include_changes=False)
            before = catalog.catalog()
            self.assertEqual(catalog.latest_batch_number(), 1)

            with self.assertRaises(InvalidBatch):
                catalog.apply_batch({
                    "suppliers": [
                        {"name": "alpha", "db_path": self.a_db,
                         "version_id": "missing"},
                    ],
                    "claims": [
                        {"op": "add", "claim_id": "should-rollback",
                         "a": ("alpha", 1), "b": ("beta", 2)},
                    ],
                }, include_changes=False)

            self.assertEqual(catalog.latest_batch_number(), 1)
            self.assertEqual(catalog.catalog(), before)
            self.assertIsNone(catalog.conn.execute(
                "SELECT 1 FROM cc_claims WHERE claim_id='should-rollback'"
            ).fetchone())

        reopened = CrossCatalog(self.cross_db)
        try:
            self.assertEqual(reopened.latest_batch_number(), 1)
            changes = reopened.get_changes(1)["changes"]
            self.assertEqual(len(changes), 1)
            self.assertEqual(changes[0]["change_type"], "added")
            self.assertEqual(changes[0]["record"]["price"], 10)
        finally:
            reopened.close()

    def test_duplicate_supplier_or_claim_in_batch_rejected(self):
        publish_supplier(self.a_db, {"v1": [(1, "a1", 10, 1)]})
        publish_supplier(self.b_db, {"v1": [(2, "b2", 20, 1)]})
        publish_supplier(self.c_db, {"v1": []})
        with self.open_catalog() as catalog:
            with self.assertRaises(InvalidBatch):
                catalog.apply_batch({"suppliers": [
                    {"name": "alpha", "db_path": self.a_db, "version_id": "v1"},
                    {"name": "alpha", "db_path": self.a_db, "version_id": "v1"},
                ]})
            with self.assertRaises(InvalidBatch):
                catalog.apply_batch({
                    "suppliers": [
                        {"name": "alpha", "db_path": self.a_db, "version_id": "v1"},
                        {"name": "beta", "db_path": self.b_db, "version_id": "v1"},
                    ],
                    "claims": [
                        {"op": "add", "claim_id": "c1", "a": ("alpha", 1), "b": ("beta", 2)},
                        {"op": "add", "claim_id": "c1", "a": ("alpha", 1), "b": ("beta", 2)},
                    ],
                })
            self.assertIsNone(catalog.latest_batch_number())

    def test_supplier_must_be_selected_before_claim(self):
        with self.open_catalog() as catalog:
            with self.assertRaises(InvalidBatch):
                catalog.apply_batch({"claims": [
                    {"op": "add", "claim_id": "c1",
                     "a": ("ghost", 1), "b": ("alpha", 2)},
                ]})

    def test_interrupted_recompute_rolls_back_whole_batch(self):
        publish_supplier(self.a_db, {"v1": [(1, "a1", 10, 1)]})
        publish_supplier(self.b_db, {"v1": []})
        publish_supplier(self.c_db, {"v1": []})
        catalog = self.open_catalog()

        def boom(*_args):
            raise RuntimeError("simulated interruption")

        catalog._recompute_one_group = boom  # type: ignore[assignment]
        with self.assertRaises(RuntimeError):
            catalog.apply_batch({
                "suppliers": [{"name": "alpha", "db_path": self.a_db,
                               "version_id": "v1"}],
            }, include_changes=False)
        catalog.close()

        reopened = self.open_catalog()
        try:
            self.assertIsNone(reopened.latest_batch_number())
            self.assertEqual(reopened.catalog(), [])
            self.assertEqual(
                reopened.conn.execute("SELECT COUNT(*) c FROM cc_supplier_quotes")
                .fetchone()["c"],
                0,
            )
        finally:
            reopened.close()

    def test_unselected_source_database_is_never_reopened(self):
        publish_supplier(self.a_db, {"v1": [(1, "a1", 10, 1)]})
        publish_supplier(self.b_db, {"v1": [(2, "b2", 20, 1)]})
        publish_supplier(self.c_db, {"v1": []})
        with self.open_catalog() as catalog:
            catalog.apply_batch({
                "suppliers": [
                    {"name": "alpha", "db_path": self.a_db, "version_id": "v1"},
                    {"name": "beta", "db_path": self.b_db, "version_id": "v1"},
                ]
            }, include_changes=False)

        # 删除/移走 beta 源库后，只提交声明批次也必须成功：不能重读 beta 源库。
        os.rename(self.b_db, self.b_db + ".moved-away")
        with self.open_catalog() as catalog:
            receipt = catalog.apply_batch({"claims": [
                {"op": "add", "claim_id": "c1", "a": ("alpha", 1), "b": ("beta", 2)},
            ]})
            self.assertEqual(receipt["counts"]["changes_removed"], 1)
            self.assertEqual(len(catalog.catalog()), 1)

    def test_out_of_stock_does_not_revoke_claim(self):
        publish_supplier(self.a_db, {
            "v1": [(1, "a1", 10, 1)],
            "v2": [],  # 商品 1 缺货
        })
        publish_supplier(self.b_db, {
            "v1": [(2, "b2", 20, 1)],
            "v2": [],  # 商品 2 也缺货
        })
        publish_supplier(self.c_db, {"v1": []})

        with self.open_catalog() as catalog:
            catalog.apply_batch({
                "suppliers": [
                    {"name": "alpha", "db_path": self.a_db, "version_id": "v1"},
                    {"name": "beta", "db_path": self.b_db, "version_id": "v1"},
                ],
                "claims": [
                    {"op": "add", "claim_id": "c1", "a": ("alpha", 1), "b": ("beta", 2)},
                ],
            }, include_changes=False)

            catalog.apply_batch({
                "suppliers": [
                    {"name": "alpha", "db_path": self.a_db, "version_id": "v2"},
                ]
            }, include_changes=False)
            record = catalog.catalog()[0]
            self.assertEqual([m["product_id"] for m in record["members"]], [1, 2])
            self.assertEqual(record["winning_identity"],
                             {"supplier": "beta", "product_id": 2})

            catalog.apply_batch({
                "suppliers": [
                    {"name": "beta", "db_path": self.b_db, "version_id": "v2"},
                ]
            }, include_changes=False)
            self.assertEqual(catalog.catalog(), [])
            claim = catalog.conn.execute(
                "SELECT status FROM cc_claims WHERE claim_id='c1'"
            ).fetchone()
            self.assertEqual(claim["status"], "active")
            members = catalog.conn.execute(
                "SELECT supplier,product_id FROM cc_group_members "
                "ORDER BY supplier,product_id"
            ).fetchall()
            self.assertEqual([tuple(r) for r in members],
                             [("alpha", 1), ("beta", 2)])

    def test_quote_appearing_in_formerly_quoteless_group_adds_key(self):
        publish_supplier(self.a_db, {
            "v1": [],
            "v2": [(99, "a99", 1, 1)],
        })
        publish_supplier(self.b_db, {"v1": []})
        publish_supplier(self.c_db, {"v1": []})
        with self.open_catalog() as catalog:
            catalog.apply_batch({
                "suppliers": [
                    {"name": "alpha", "db_path": self.a_db, "version_id": "v1"},
                    {"name": "beta", "db_path": self.b_db, "version_id": "v1"},
                ],
                "claims": [
                    {"op": "add", "claim_id": "future",
                     "a": ("alpha", 99), "b": ("beta", 98)},
                ],
            }, include_changes=False)
            self.assertEqual(catalog.catalog(), [])

            receipt = catalog.apply_batch({
                "suppliers": [
                    {"name": "alpha", "db_path": self.a_db, "version_id": "v2"},
                ]
            })
            self.assertEqual(receipt["counts"]["changes_added"], 1)
            record = receipt["changes"][0]["record"]
            self.assertEqual(record["key_tuple"], ["alpha", 99])
            self.assertEqual(record["price"], 1)
            self.assertEqual(len(record["members"]), 2)

    def test_historical_snapshot_replay(self):
        publish_supplier(self.a_db, {"v1": [(1, "old", 10, 1)],
                                     "v2": [(1, "new", 12, 2)]})
        publish_supplier(self.b_db, {"v1": []})
        publish_supplier(self.c_db, {"v1": []})
        with self.open_catalog() as catalog:
            catalog.apply_batch({
                "suppliers": [{"name": "alpha", "db_path": self.a_db,
                               "version_id": "v1"}]
            }, include_changes=False)
            catalog.apply_batch({
                "suppliers": [{"name": "alpha", "db_path": self.a_db,
                               "version_id": "v2"}]
            }, include_changes=False)

            old = catalog.snapshot(1)
            new = catalog.snapshot()
            self.assertEqual(old["batch_number"], 1)
            self.assertEqual(old["records"][0]["url"], "old")
            self.assertEqual(new["batch_number"], 2)
            self.assertEqual(new["records"][0]["url"], "new")

    def test_module_level_apply_batch_convenience(self):
        publish_supplier(self.a_db, {"v1": [(1, "a1", 10, 1)]})
        publish_supplier(self.b_db, {"v1": []})
        publish_supplier(self.c_db, {"v1": []})
        receipt = apply_batch(self.cross_db, {
            "suppliers": [{"name": "alpha", "db_path": self.a_db,
                           "version_id": "v1"}]
        })
        self.assertEqual(receipt["batch_number"], 1)
        self.assertEqual(receipt["changes"][0]["key_tuple"], ["alpha", 1])


if __name__ == "__main__":
    unittest.main(verbosity=2)
