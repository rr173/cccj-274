"""按版本发布的商品限制清单（召回/禁采）测试。

覆盖：

* 清单文档严格校验（日期、区间倒置、重复记录、同身份区间重叠、版本不一致）；
* 批次采用清单版本 + 核算时点，仅有效条目排除报价、同款关系不变；
* 仍有可选报价时按现有规则选最低价，全部被排除时不输出目录记录；
* 上下文沿用/换时点/清除、清单版本不可变、失败批次原子性；
* 批次结果与审计证据能指出清单版本和排除依据，后续修订不改变已成功批次。
"""

from __future__ import annotations

import json
import os
import shutil
import sys
import tempfile
import unittest
from datetime import date
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from catpipe.procurement import CrossCatalog, InvalidBatch  # noqa: E402
from catpipe.repository import Repository  # noqa: E402
from catpipe.restrictions import (  # noqa: E402
    RestrictionError,
    canonical_document,
    document_sha256,
    intervals_overlap,
    is_effective,
    load_restriction_selection,
    parse_calendar_date,
    validate_restriction_document,
)


def publish_supplier(path: str, versions: dict) -> None:
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


class RestrictionDocumentCase(unittest.TestCase):
    def _doc(self, **over):
        doc = {
            "list_id": "L",
            "version": "v1",
            "entries": [
                {"record_id": "R1", "supplier": "alpha", "product_id": 1,
                 "effective_from": "2026-01-01", "effective_to": "2026-06-30"},
            ],
        }
        doc.update(over)
        return doc

    def test_valid_document_canonical_and_stable_hash(self):
        doc = self._doc()
        canonical = validate_restriction_document(doc)
        self.assertEqual(canonical["list_id"], "L")
        self.assertEqual(canonical["entries"][0]["effective_to"], "2026-06-30")
        # 输入键顺序不影响规范文档与指纹。
        shuffled = json.loads(json.dumps(doc))
        shuffled["entries"] = list(reversed(shuffled["entries"]))
        self.assertEqual(
            document_sha256(canonical),
            document_sha256(validate_restriction_document(shuffled)),
        )

    def test_open_ended_interval(self):
        doc = self._doc()
        doc["entries"][0]["effective_to"] = None
        canonical = validate_restriction_document(doc)
        entry = canonical["entries"][0]
        self.assertTrue(is_effective(entry, date(2030, 1, 1)))
        self.assertFalse(is_effective(entry, date(2025, 12, 31)))

    def test_date_parsing_is_strict(self):
        with self.assertRaises(RestrictionError):
            parse_calendar_date("2026/01/01", what="x")
        with self.assertRaises(RestrictionError):
            parse_calendar_date("2026-02-30", what="x")
        with self.assertRaises(RestrictionError):
            parse_calendar_date("2026-01-01T00:00:00", what="x")
        self.assertEqual(parse_calendar_date("2026-01-01", what="x"), date(2026, 1, 1))

    def test_interval_overlap_semantics(self):
        self.assertTrue(
            intervals_overlap(date(2026, 1, 1), date(2026, 1, 31),
                              date(2026, 1, 31), date(2026, 2, 1))
        )
        # 相邻区间不重叠。
        self.assertFalse(
            intervals_overlap(date(2026, 1, 1), date(2026, 1, 31),
                              date(2026, 2, 1), None)
        )

    def test_missing_list_id_version_entries(self):
        for bad in ({}, {"list_id": "L"}, {"list_id": "L", "version": "v1"},
                    {"list_id": "L", "version": "v1", "entries": "nope"}):
            with self.assertRaises(RestrictionError):
                validate_restriction_document(bad)

    def test_invalid_entries(self):
        # 缺少记录编号
        bad = self._doc()
        del bad["entries"][0]["record_id"]
        with self.assertRaises(RestrictionError):
            validate_restriction_document(bad)
        # 区间倒置
        bad = self._doc()
        bad["entries"][0]["effective_to"] = "2025-01-01"
        with self.assertRaises(RestrictionError):
            validate_restriction_document(bad)
        # 重复记录编号
        bad = self._doc()
        bad["entries"].append(dict(bad["entries"][0]))
        with self.assertRaises(RestrictionError):
            validate_restriction_document(bad)
        # 同身份区间重叠
        bad = self._doc()
        bad["entries"][0]["record_id"] = "R1"
        bad["entries"].append({
            "record_id": "R2", "supplier": "alpha", "product_id": 1,
            "effective_from": "2026-06-01", "effective_to": None})
        with self.assertRaises(RestrictionError):
            validate_restriction_document(bad)
        # 相邻接续区间合法
        ok = self._doc()
        ok["entries"].append({
            "record_id": "R2", "supplier": "alpha", "product_id": 1,
            "effective_from": "2026-07-01", "effective_to": None})
        validate_restriction_document(ok)

    def test_version_mismatch_rejected(self):
        with self.assertRaises(RestrictionError):
            validate_restriction_document(
                self._doc(), expected_list_id="other", expected_version="v9"
            )

    def test_selection_header_absent_clear_select(self):
        self.assertIsNone(load_restriction_selection({}))
        clear = load_restriction_selection({"restriction_list": None})
        self.assertTrue(clear.clear)
        doc = self._doc()
        sel = load_restriction_selection({
            "restriction_list": {
                "list_id": "L", "version": "v1", "as_of": "2026-05-01",
                "document": doc,
            }
        })
        self.assertEqual(sel.list_id, "L")
        self.assertEqual(sel.as_of, date(2026, 5, 1))
        self.assertEqual(sel.document["entries"][0]["record_id"], "R1")


class RestrictionBatchCase(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.mkdtemp(prefix="catpipe-rest-test-")
        self.a_db = os.path.join(self.tmp, "a.db")
        self.b_db = os.path.join(self.tmp, "b.db")
        self.cross_db = os.path.join(self.tmp, "cross.db")
        publish_supplier(self.a_db, {"v1": [
            (1, "https://a/1", 60, 1),
            (2, "https://a/2", 50, 1),
            (9, "https://a/9", 10, 1),
        ]})
        publish_supplier(self.b_db, {"v1": [
            (1, "https://b/1", 80, 1),
            (2, "https://b/2", 200, 1),
        ]})

    def tearDown(self) -> None:
        shutil.rmtree(self.tmp, ignore_errors=True)

    def _list_file(self, name: str, doc: dict) -> str:
        path = os.path.join(self.tmp, name)
        Path(path).write_text(json.dumps(doc), encoding="utf-8")
        return path

    def _base_doc(self) -> dict:
        return {
            "list_id": "recall-2026",
            "version": "r1",
            "entries": [
                {"record_id": "RC-1", "supplier": "alpha", "product_id": 1,
                 "effective_from": "2026-01-01", "effective_to": "2026-06-30"},
                {"record_id": "RC-2", "supplier": "alpha", "product_id": 2,
                 "effective_from": "2026-03-01", "effective_to": None},
            ],
        }

    def _open(self) -> CrossCatalog:
        return CrossCatalog(self.cross_db)

    def _seed(self, cat: CrossCatalog) -> None:
        cat.apply_batch({
            "suppliers": [
                {"name": "alpha", "db_path": self.a_db, "version_id": "v1"},
                {"name": "beta", "db_path": self.b_db, "version_id": "v1"},
            ],
            "claims": [
                {"op": "add", "claim_id": "c1",
                 "a": ("alpha", 1), "b": ("beta", 1)},
                {"op": "add", "claim_id": "c2",
                 "a": ("alpha", 2), "b": ("beta", 2)},
            ],
        }, include_changes=False)

    def test_effective_entries_exclude_quotes_but_keep_claims(self):
        list_path = self._list_file("r1.json", self._base_doc())
        with self._open() as cat:
            self._seed(cat)
            receipt = cat.apply_batch({"restriction_list": {
                "list_id": "recall-2026", "version": "r1",
                "as_of": "2026-05-01", "path": list_path}})
            self.assertEqual(receipt["counts"]["excluded_quotes"], 2)
            rows = {
                (r["group_supplier"], r["group_product_id"]): r
                for r in cat.catalog()
            }
            g1 = rows[("alpha", 1)]
            # alpha/1(60) 被排除，赢家切换为 beta/1(80)；成员关系仍是两个商品。
            self.assertEqual(g1["winning_identity"],
                             {"supplier": "beta", "product_id": 1})
            self.assertEqual(
                [m["identity"] if "identity" in m else
                 {"supplier": m["supplier"], "product_id": m["product_id"]}
                 for m in g1["members"]],
                [{"supplier": "alpha", "product_id": 1},
                 {"supplier": "beta", "product_id": 1}],
            )
            self.assertEqual(g1["restriction_context"]["version"], "r1")
            basis = g1["restrictions"][0]
            self.assertEqual(basis["record_id"], "RC-1")
            self.assertEqual(basis["as_of"], "2026-05-01")
            excluded = cat.excluded_quotes(2)
            self.assertEqual(
                sorted((e["supplier"], e["product_id"]) for e in excluded),
                [("alpha", 1), ("alpha", 2)],
            )
            ctx = cat.batch_restriction_context(2)
            self.assertEqual(ctx["source"], "selected")
            self.assertEqual(ctx["document_sha256"],
                             g1["restriction_context"]["document_sha256"])

    def test_expired_entry_does_not_exclude_and_as_of_change_is_incremental(self):
        list_path = self._list_file("r1.json", self._base_doc())
        with self._open() as cat:
            self._seed(cat)
            cat.apply_batch({"restriction_list": {
                "list_id": "recall-2026", "version": "r1",
                "as_of": "2026-05-01", "path": list_path}}, include_changes=False)
            receipt = cat.apply_batch({"restriction_list": {
                "list_id": "recall-2026", "version": "r1",
                "as_of": "2026-07-15", "path": list_path}})
            # RC-1 已过期，只剩 RC-2 有效。
            self.assertEqual(receipt["counts"]["restriction_entries_effective"], 1)
            rows = {(r["group_supplier"], r["group_product_id"]): r
                    for r in cat.catalog()}
            self.assertEqual(rows[("alpha", 1)]["winning_identity"],
                             {"supplier": "alpha", "product_id": 1})
            self.assertEqual(rows[("alpha", 1)]["price"], 60)
            self.assertEqual(rows[("alpha", 2)]["winning_identity"],
                             {"supplier": "beta", "product_id": 2})
            # 组1赢家实际切换；组2 因记录内嵌的核算时点变化产生 changed 事件，
            # 但赢家与价格保持不变。
            changed = {
                tuple(e["key_tuple"]): e["record"]
                for e in receipt["changes"] if e["change_type"] == "changed"
            }
            self.assertIn(("alpha", 1), changed)
            self.assertEqual(
                changed[("alpha", 1)]["winning_identity"],
                {"supplier": "alpha", "product_id": 1},
            )
            self.assertEqual(
                changed[("alpha", 2)]["winning_identity"],
                {"supplier": "beta", "product_id": 2},
            )

    def test_reuse_context_without_reselecting(self):
        list_path = self._list_file("r1.json", self._base_doc())
        with self._open() as cat:
            self._seed(cat)
            cat.apply_batch({"restriction_list": {
                "list_id": "recall-2026", "version": "r1",
                "as_of": "2026-05-01", "path": list_path}}, include_changes=False)
            # 移走清单文件：沿用上下文时不会重新打开它。
            os.rename(list_path, list_path + ".gone")
            receipt = cat.apply_batch({"claims": []})
            self.assertEqual(receipt["restriction"]["source"], "reused")
            self.assertEqual(receipt["counts"]["excluded_quotes"], 2)
            self.assertEqual(receipt["counts"]["changes_changed"], 0)

    def test_clear_restriction_restores_quotes(self):
        list_path = self._list_file("r1.json", self._base_doc())
        with self._open() as cat:
            self._seed(cat)
            cat.apply_batch({"restriction_list": {
                "list_id": "recall-2026", "version": "r1",
                "as_of": "2026-05-01", "path": list_path}}, include_changes=False)
            receipt = cat.apply_batch({"restriction_list": None})
            self.assertEqual(receipt["restriction"]["source"], "cleared")
            self.assertIsNone(cat.batch_restriction_context(receipt["batch_number"]))
            rows = {(r["group_supplier"], r["group_product_id"]): r
                    for r in cat.catalog()}
            self.assertEqual(rows[("alpha", 1)]["price"], 60)
            self.assertNotIn("restrictions", rows[("alpha", 1)])

    def test_all_quotes_excluded_omits_group_but_keeps_membership(self):
        doc = {
            "list_id": "ban", "version": "v1",
            "entries": [{
                "record_id": "B-9", "supplier": "alpha", "product_id": 9,
                "effective_from": "2026-01-01", "effective_to": None}],
        }
        list_path = self._list_file("ban.json", doc)
        with self._open() as cat:
            # 第一批先让 alpha/9 正常进入目录。
            cat.apply_batch({
                "suppliers": [
                    {"name": "alpha", "db_path": self.a_db, "version_id": "v1"},
                ],
            }, include_changes=False)
            self.assertIn(
                ("alpha", 9),
                [(r["group_supplier"], r["group_product_id"]) for r in cat.catalog()],
            )
            receipt = cat.apply_batch({"restriction_list": {
                "list_id": "ban", "version": "v1",
                "as_of": "2026-05-01", "path": list_path}})
            keys = [(r["group_supplier"], r["group_product_id"])
                    for r in cat.catalog()]
            self.assertNotIn(("alpha", 9), keys)
            # 同款/成员关系不因为限制而删除。
            members = cat.conn.execute(
                "SELECT COUNT(*) c FROM cc_group_members "
                "WHERE group_supplier='alpha' AND group_product_id=9"
            ).fetchone()["c"]
            self.assertEqual(members, 1)
            # 该组以 removed 变更下发。
            changes = receipt["changes"]
            self.assertTrue(
                any(e["change_type"] == "removed" and
                    tuple(e["key_tuple"]) == ("alpha", 9) for e in changes)
            )
            # 排除证据保留在批次审计表中。
            excluded = cat.excluded_quotes(receipt["batch_number"])
            self.assertEqual(
                [(e["supplier"], e["product_id"], e["record_id"]) for e in excluded],
                [("alpha", 9, "B-9")],
            )

    def test_published_list_version_is_immutable(self):
        list_path = self._list_file("r1.json", self._base_doc())
        with self._open() as cat:
            self._seed(cat)
            cat.apply_batch({"restriction_list": {
                "list_id": "recall-2026", "version": "r1",
                "as_of": "2026-05-01", "path": list_path}}, include_changes=False)
            altered = self._base_doc()
            altered["entries"][0]["effective_to"] = "2026-12-31"
            altered_path = self._list_file("r1-altered.json", altered)
            before = cat.latest_batch_number()
            with self.assertRaises(InvalidBatch):
                cat.apply_batch({"restriction_list": {
                    "list_id": "recall-2026", "version": "r1",
                    "as_of": "2026-05-01", "path": altered_path}})
            self.assertEqual(cat.latest_batch_number(), before)

    def test_new_list_version_revises_without_touching_old_batches(self):
        list_path = self._list_file("r1.json", self._base_doc())
        with self._open() as cat:
            self._seed(cat)
            cat.apply_batch({"restriction_list": {
                "list_id": "recall-2026", "version": "r1",
                "as_of": "2026-05-01", "path": list_path}}, include_changes=False)
            r2_doc = self._base_doc()
            r2_doc["version"] = "r2"
            r2_doc["entries"][0]["effective_to"] = "2026-12-31"
            r2_path = self._list_file("r2.json", r2_doc)
            cat.apply_batch({"restriction_list": {
                "list_id": "recall-2026", "version": "r2",
                "as_of": "2026-05-01", "path": r2_path}}, include_changes=False)
            lists = {(x["list_id"], x["version"]) for x in cat.restriction_lists()}
            self.assertIn(("recall-2026", "r1"), lists)
            self.assertIn(("recall-2026", "r2"), lists)
            # 历史批次上下文与快照不变。
            ctx2 = cat.batch_restriction_context(2)
            self.assertEqual(ctx2["version"], "r1")
            snap2 = cat.snapshot(2)["records"]
            self.assertTrue(all(
                r["restriction_context"]["version"] == "r1" for r in snap2))

    def test_invalid_document_fails_batch_atomically(self):
        bad_doc = self._base_doc()
        bad_doc["entries"][0]["effective_to"] = "2025-01-01"
        bad_path = self._list_file("bad.json", bad_doc)
        with self._open() as cat:
            self._seed(cat)
            before = cat.latest_batch_number()
            catalog_before = cat.catalog()
            with self.assertRaises(InvalidBatch):
                cat.apply_batch({"restriction_list": {
                    "list_id": "recall-2026", "version": "bad",
                    "as_of": "2026-05-01", "path": bad_path}})
            self.assertEqual(cat.latest_batch_number(), before)
            self.assertEqual(cat.catalog(), catalog_before)
            self.assertEqual(cat.restriction_lists(), [])

    def test_missing_as_of_or_path_fails(self):
        with self._open() as cat:
            self._seed(cat)
            with self.assertRaises(InvalidBatch):
                cat.apply_batch({"restriction_list": {
                    "list_id": "L", "version": "v1", "as_of": "2026-05-01"}})
            with self.assertRaises(InvalidBatch):
                cat.apply_batch({"restriction_list": {
                    "list_id": "L", "version": "v1",
                    "path": self._list_file("r1.json", self._base_doc())}})

    def test_inline_document_and_alternative_field_names(self):
        doc = self._base_doc()
        with self._open() as cat:
            self._seed(cat)
            receipt = cat.apply_batch({
                "blocklist": {
                    "list": "recall-2026", "revision": "r1",
                    "accounting_date": "2026-05-01",
                    "document": doc,
                }
            })
            self.assertEqual(
                receipt["restriction"]["list_id"], "recall-2026"
            )
            self.assertEqual(receipt["counts"]["excluded_quotes"], 2)

    def test_restriction_combined_with_quote_change(self):
        list_path = self._list_file("r1.json", self._base_doc())
        publish_supplier(self.a_db, {"v2": [
            (1, "https://a/1-v2", 55, 2),   # 仍被 RC-1 排除
            (2, "https://a/2", 50, 1),
            (9, "https://a/9", 10, 1),
        ]})
        with self._open() as cat:
            self._seed(cat)
            cat.apply_batch({"restriction_list": {
                "list_id": "recall-2026", "version": "r1",
                "as_of": "2026-05-01", "path": list_path}}, include_changes=False)
            receipt = cat.apply_batch({
                "suppliers": [
                    {"name": "alpha", "db_path": self.a_db, "version_id": "v2"},
                ],
            })
            rows = {(r["group_supplier"], r["group_product_id"]): r
                    for r in cat.catalog()}
            # 新报价仍受限，赢家保持 beta/1。
            self.assertEqual(rows[("alpha", 1)]["winning_identity"],
                             {"supplier": "beta", "product_id": 1})
            excluded = cat.excluded_quotes(receipt["batch_number"])
            rc1 = next(e for e in excluded if e["record_id"] == "RC-1")
            self.assertEqual(rc1["price"], 55)  # 排除依据指向当前报价行

    def test_reopen_persists_restriction_state_and_history(self):
        list_path = self._list_file("r1.json", self._base_doc())
        with self._open() as cat:
            self._seed(cat)
            cat.apply_batch({"restriction_list": {
                "list_id": "recall-2026", "version": "r1",
                "as_of": "2026-05-01", "path": list_path}}, include_changes=False)
        with self._open() as reopened:
            self.assertEqual(reopened.batch_restriction_context(2)["version"], "r1")
            snap = {
                (r["group_supplier"], r["group_product_id"]): r
                for r in reopened.snapshot(2)["records"]
            }
            self.assertEqual(snap[("alpha", 1)]["winning_identity"],
                             {"supplier": "beta", "product_id": 1})
            self.assertEqual(
                sorted(e["record_id"] for e in reopened.excluded_quotes(2)),
                ["RC-1", "RC-2"],
            )


if __name__ == "__main__":
    unittest.main(verbosity=2)
