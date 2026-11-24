"""接收引擎与发布器的单元测试（不启动 HTTP，直接构造 JSONL 流）。"""

from __future__ import annotations

import io
import json
import os
import sys
import tempfile
import unittest

# 支持 `python3 -I tests/test_ingest_publish.py`：隔离模式不读 PYTHONPATH。
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from catpipe.ingest import IngestionEngine
from catpipe.protocol import (
    NotPublishable,
    RejectedError,
    VersionInvalid,
    dump_jsonl_line,
    page_content_hash,
    page_content_hash_from_sorted,
)
from catpipe.publisher import compute_coverage, coverage_status, publish
from catpipe.repository import Repository


def page_stream(version_id: str, token: str, page_id: str, revision: int,
                lo: int, hi: int, products: list[tuple], *,
                domain=(0, 1000), end_count=None, bad_checksum=False,
                omit_end=False) -> io.BytesIO:
    """构造单页 JSONL 响应字节流。products=(id,url,price,product_revision)。"""
    buf = io.BytesIO()
    header = dump_jsonl_line({
        "type": "page", "version_id": version_id, "token": token,
        "page_id": page_id, "revision": revision,
        "range": {"lo": lo, "hi": hi},
        "domain": {"lo": domain[0], "hi": domain[1]},
    })
    buf.write(header)
    import hashlib
    h = hashlib.sha256()
    h.update(header)
    for pid, url, price, prev in products:
        line = dump_jsonl_line({
            "type": "product", "product_id": pid, "url": url,
            "price": price, "product_revision": prev,
        })
        buf.write(line)
        h.update(line)
    if not omit_end:
        buf.write(dump_jsonl_line({
            "type": "end",
            "count": end_count if end_count is not None else len(products),
            "checksum": "0" * 64 if bad_checksum else h.hexdigest(),
        }))
    buf.seek(0)
    return buf


class BaseCase(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.mkdtemp(prefix="catpipe-test-")
        self.repo = Repository(os.path.join(self.tmp, "t.db"))
        self.engine = IngestionEngine(self.repo)

    def tearDown(self) -> None:
        self.repo.close()

    def ingest(self, *args, **kwargs):
        return self.engine.ingest_stream(page_stream(*args, **kwargs))

    def page_count(self, vid: str) -> int:
        return self.repo.conn.execute(
            "SELECT COUNT(*) c FROM pages WHERE version_id=?", (vid,)).fetchone()["c"]

    def evidence_count(self, vid: str) -> int:
        return self.repo.conn.execute(
            "SELECT COUNT(*) c FROM evidence WHERE version_id=?", (vid,)).fetchone()["c"]


class TestStructuralRejection(BaseCase):
    def test_duplicate_product_id_rejected(self):
        with self.assertRaises(RejectedError) as ctx:
            self.ingest("v1", "tok", "p1", 1, 0, 10,
                        [(1, "u1", 10, 1), (1, "u2", 20, 1)])
        self.assertEqual(ctx.exception.reason_code, "duplicate_product")
        self.assertEqual(self.page_count("v1"), 0)

    def test_product_out_of_range_rejected(self):
        with self.assertRaises(RejectedError) as ctx:
            self.ingest("v1", "tok", "p1", 1, 0, 10,
                        [(99, "u1", 10, 1)])
        self.assertEqual(ctx.exception.reason_code, "product_out_of_range")

    def test_range_out_of_domain_rejected(self):
        with self.assertRaises(RejectedError) as ctx:
            self.ingest("v1", "tok", "p1", 1, 900, 1100,
                        [(950, "u", 1, 1)], domain=(0, 1000))
        self.assertEqual(ctx.exception.reason_code, "out_of_domain")

    def test_bad_checksum_and_truncation_rejected(self):
        with self.assertRaises(RejectedError) as ctx:
            self.ingest("v1", "tok", "p1", 1, 0, 5,
                        [(1, "u", 1, 1)], bad_checksum=True)
        self.assertEqual(ctx.exception.reason_code, "checksum_mismatch")
        with self.assertRaises(RejectedError) as ctx:
            self.ingest("v1", "tok", "p1", 1, 0, 5,
                        [(1, "u", 1, 1)], omit_end=True)
        self.assertEqual(ctx.exception.reason_code, "truncated")
        self.assertEqual(self.page_count("v1"), 0)

    def test_count_mismatch_rejected(self):
        with self.assertRaises(RejectedError) as ctx:
            self.ingest("v1", "tok", "p1", 1, 0, 5,
                        [(1, "u", 1, 1)], end_count=2)
        self.assertEqual(ctx.exception.reason_code, "count_mismatch")


class TestPageRevisionLifecycle(BaseCase):
    def _base(self):
        self.ingest("v1", "tok", "p1", 1, 0, 10,
                    [(1, "u1", 100, 1), (2, "u2", 200, 1)])

    def test_same_revision_same_content_is_noop(self):
        self._base()
        receipt = self.ingest("v1", "tok", "p1", 1, 0, 10,
                              [(2, "u2", 200, 1), (1, "u1", 100, 1)])  # 乱序同内容
        self.assertEqual(receipt.outcome, "noop")
        self.assertEqual(self.evidence_count("v1"), 2)

    def test_same_revision_different_content_rejected(self):
        self._base()
        with self.assertRaises(RejectedError) as ctx:
            self.ingest("v1", "tok", "p1", 1, 0, 10,
                        [(1, "u1", 100, 1), (2, "u2", 999, 1)])
        self.assertEqual(ctx.exception.reason_code, "same_revision_conflict")

    def test_lower_revision_rejected(self):
        self._base()
        with self.assertRaises(RejectedError) as ctx:
            self.ingest("v1", "tok", "p1", 0, 0, 10,
                        [(1, "u1", 100, 1)])
        self.assertEqual(ctx.exception.reason_code, "stale_revision")

    def test_higher_revision_replaces_values_and_coverage(self):
        self._base()
        # 高修订：窗口移动到 [5,15)，商品 1 随旧覆盖消失，新商品 11 进入；6 改价
        receipt = self.ingest("v1", "tok", "p1", 2, 5, 15,
                              [(6, "u6", 250, 1), (11, "u11", 300, 1)],
                              domain=(0, 1000))
        self.assertEqual(receipt.outcome, "replaced")
        page = self.repo.get_page("v1", "p1")
        self.assertEqual((page["range_lo"], page["range_hi"], page["revision"]), (5, 15, 2))
        ids = {r["product_id"] for r in self.repo.conn.execute(
            "SELECT product_id FROM evidence WHERE version_id='v1'")}
        self.assertEqual(ids, {6, 11})
        price = self.repo.conn.execute(
            "SELECT price FROM evidence WHERE version_id='v1' AND product_id=6"
        ).fetchone()["price"]
        self.assertEqual(price, 250)

    def test_rejected_reception_leaves_candidate_intact_and_quarantines_raw(self):
        self._base()
        before = [dict(r) for r in self.repo.list_pages("v1")]
        with self.assertRaises(RejectedError):
            self.ingest("v1", "tok", "p1", 2, 0, 10,
                        [(1, "u1", 100, 1), (2, "u2", 200, 1), (3, "u3", 1, 1)],
                        bad_checksum=True)
        after = [dict(r) for r in self.repo.list_pages("v1")]
        self.assertEqual(before, after)
        q = self.repo.conn.execute("SELECT COUNT(*) c FROM quarantined").fetchone()["c"]
        self.assertEqual(q, 1)
        # 暂存表不允许残留
        left = self.repo.conn.execute("SELECT COUNT(*) c FROM page_staging").fetchone()["c"]
        self.assertEqual(left, 0)


class TestCrossPageContradictions(BaseCase):
    def test_overlapping_pages_consistent_payload_ok(self):
        # p1=[0,10) p2=[5,15)，重叠 [5,10) 载荷一致 → 合法
        self.ingest("v1", "tok", "p1", 1, 0, 10,
                    [(i, f"u{i}", 100 + i, 1) for i in range(0, 10)])
        self.ingest("v1", "tok", "p2", 1, 5, 15,
                    [(i, f"u{i}", 100 + i, 1) for i in range(5, 15)])
        self.assertEqual(self.page_count("v1"), 2)

    def test_overlapping_payload_conflict_rejected(self):
        self.ingest("v1", "tok", "p1", 1, 0, 10,
                    [(5, "u5", 100, 1)])
        with self.assertRaises(RejectedError) as ctx:
            self.ingest("v1", "tok", "p2", 1, 5, 15,
                        [(5, "u5-new", 100, 1)])  # 同 ID URL 不一致 → 矛盾，而非新商品
        self.assertEqual(ctx.exception.reason_code, "payload_conflict")
        self.assertEqual(self.page_count("v1"), 1)

    def test_absence_presence_conflict_rejected(self):
        # p1 宣称 5 在售；p2 完整范围覆盖 5 却缺席 → 矛盾
        self.ingest("v1", "tok", "p1", 1, 0, 10, [(5, "u5", 100, 1)])
        with self.assertRaises(RejectedError) as ctx:
            self.ingest("v1", "tok", "p2", 1, 0, 20,
                        [(i, f"u{i}", 1, 1) for i in range(0, 20) if i != 5])
        self.assertEqual(ctx.exception.reason_code, "presence_conflict")

    def test_revision_replace_withdraw_conflicting_with_neighbor_rejected(self):
        # 两个重叠页都含 610；其中一页高修订把 610 下架 → 出现/缺席矛盾，整次拒绝
        self.ingest("v1", "tok", "pB", 1, 300, 620,
                    [(i, f"u{i}", i, 1) for i in range(300, 620)])
        self.ingest("v1", "tok", "pC", 1, 600, 900,
                    [(i, f"u{i}", i, 1) for i in range(600, 900)])
        with self.assertRaises(RejectedError) as ctx:
            self.ingest("v1", "tok", "pC", 2, 600, 900,
                        [(i, f"u{i}", i, 1) for i in range(600, 900) if i != 610])
        self.assertEqual(ctx.exception.reason_code, "presence_conflict")
        # 旧 pC 原封不动
        self.assertEqual(self.repo.get_page("v1", "pC")["revision"], 1)

    def test_url_migration_keeps_stable_identity(self):
        self.ingest("v1", "tok", "p1", 1, 0, 100,
                    [(42, "old-url", 100, 1), (43, "keep", 1, 1)])
        # 单页高修订：同 ID 换 URL 是合法载荷变更，不是新商品
        self.ingest("v1", "tok", "p1", 2, 0, 100,
                    [(42, "new-url", 100, 2), (43, "keep", 1, 1)])
        rows = self.repo.conn.execute(
            "SELECT * FROM evidence WHERE version_id='v1' ORDER BY product_id").fetchall()
        self.assertEqual([r["product_id"] for r in rows], [42, 43])  # 没有新造商品
        self.assertEqual(rows[0]["url"], "new-url")
        self.assertEqual(rows[0]["product_revision"], 2)


class TestCoverageAndPublish(BaseCase):
    def test_gap_blocks_publish_and_reports_intervals_without_id_expansion(self):
        self.ingest("v1", "tok", "p1", 1, 0, 100,
                    [(i, f"u{i}", 1, 1) for i in range(0, 100)], domain=(0, 300))
        self.ingest("v1", "tok", "p2", 1, 200, 300,
                    [(i, f"u{i}", 1, 1) for i in range(200, 300)], domain=(0, 300))
        cov = compute_coverage(self.repo, "v1")
        self.assertFalse(cov["covered"])
        self.assertEqual(cov["gaps"], [{"lo": 100, "hi": 200, "size": 100}])
        with self.assertRaises(NotPublishable):
            publish(self.repo, "v1")

    def test_full_coverage_publishes_with_overlap_and_absence(self):
        # 域 [0,300)，两页重叠 [200,250)，商品 7 下架（缺席不展开）
        self.ingest("v1", "tok", "p1", 1, 0, 250,
                    [(i, f"u{i}", 1000 + i, 1) for i in range(0, 250) if i != 7],
                    domain=(0, 300))
        self.ingest("v1", "tok", "p2", 1, 200, 300,
                    [(i, f"u{i}", 1000 + i, 1) for i in range(200, 300)],
                    domain=(0, 300))
        summary = publish(self.repo, "v1")
        self.assertEqual(summary["product_count"], 299)  # 300 个 ID 减去下架的 7
        self.assertEqual(summary["diff_counts"],
                         {"added": 299, "changed": 0, "unchanged": 0, "removed": 0})
        # 覆盖证明随版保留，且带原始响应定位
        proof = json.loads(self.repo.get_published("v1")["coverage_proof_json"])
        self.assertTrue(proof["coverage"]["covered"])
        self.assertTrue(all(p["raw_locator"].endswith(".jsonl") for p in proof["pages"]))

    def test_diff_added_changed_unchanged_removed(self):
        # v1 完整覆盖域 [0,10)
        self.ingest("v1", "tok", "p1", 1, 0, 10,
                    [(i, f"u{i}", 100 + i, 1) for i in range(0, 10)],
                    domain=(0, 10))
        publish(self.repo, "v1")
        # v2 域扩展到 [0,11)：下架 0/1，2 改价，3 换 URL，4~9 不变，新增 10
        prods = [(i, f"u{i}", 100 + i, 1) for i in range(4, 10)]
        prods += [(2, "u2", 999, 1), (3, "u3-new", 103, 1),
                  (10, "u10", 110, 1)]
        self.ingest("v2", "tok2", "p1", 1, 0, 11, prods, domain=(0, 11))
        summary = publish(self.repo, "v2")
        counts = summary["diff_counts"]
        self.assertEqual(counts["removed"], 2)   # 0,1
        self.assertEqual(counts["added"], 1)     # 10
        self.assertEqual(counts["changed"], 2)   # 2,3
        self.assertEqual(counts["unchanged"], 6)  # 4..9
        # 旧版仍完整可读
        self.assertEqual(self.repo.count_published_products("v1"), 10)

    def test_empty_page_contributes_no_ids(self):
        self.ingest("v1", "tok", "p1", 1, 0, 100,
                    [(i, f"u{i}", 1, 1) for i in range(0, 100)], domain=(0, 1000))
        self.ingest("v1", "tok", "pEmpty", 1, 500, 500, [], domain=(0, 1000))  # 空窗 lo==hi
        cov = coverage_status(self.repo, "v1")
        self.assertEqual(cov["empty_pages"], ["pEmpty"])
        self.assertEqual(cov["gaps"], [{"lo": 100, "hi": 1000, "size": 900}])


class TestTokenAndVersionLifecycle(BaseCase):
    def test_wrong_token_abandons_round(self):
        self.ingest("v1", "tok", "p1", 1, 0, 10, [(1, "u1", 1, 1)])
        with self.assertRaises(VersionInvalid):
            self.ingest("v1", "WRONG", "p2", 1, 10, 20, [(11, "u11", 1, 1)])

    def test_abandon_removes_candidates_but_keeps_published(self):
        self.ingest("v1", "tok", "p1", 1, 0, 10,
                    [(i, f"u{i}", 1, 1) for i in range(0, 10)], domain=(0, 10))
        publish(self.repo, "v1")
        self.ingest("v2", "tok2", "p1", 1, 0, 10, [(0, "u0", 1, 1)],
                    domain=(0, 10))
        self.repo.abandon_version("v2", "令牌失效")
        self.assertEqual(self.repo.get_version("v2")["state"], "abandoned")
        self.assertEqual(self.evidence_count("v2"), 0)
        # 旧发布版可读
        self.assertEqual(self.repo.count_published_products("v1"), 10)
        # 失效版不能再收页（不能拿旧轮补洞）
        with self.assertRaises(VersionInvalid):
            self.ingest("v2", "tok2", "p2", 1, 10, 20, [(11, "u11", 1, 1)],
                        domain=(0, 20))

    def test_domain_change_rejected(self):
        self.ingest("v1", "tok", "p1", 1, 0, 10, [(1, "u1", 1, 1)],
                    domain=(0, 1000))
        with self.assertRaises(RejectedError) as ctx:
            self.ingest("v1", "tok", "p2", 1, 0, 10,
                        [(1, "u1", 1, 1)], domain=(0, 2000))
        self.assertEqual(ctx.exception.reason_code, "domain_changed")


class TestContentHashEquivalence(unittest.TestCase):
    def test_sorted_stream_matches_unsorted_list_hash(self):
        # 同一组商品：乱序列表经 page_content_hash 排序，
        # 与已排序元组流经 page_content_hash_from_sorted 必须同摘要。
        products = [
            {"product_id": 3, "url": "u3", "price": 30, "product_revision": 1},
            {"product_id": 1, "url": "u1", "price": 10, "product_revision": 2},
            {"product_id": 2, "url": "u2", "price": 20, "product_revision": 1},
        ]
        h_unsorted = page_content_hash("p1", 1, 0, 10, products)
        sorted_rows = sorted(
            ((p["product_id"], p["url"], p["price"], p["product_revision"])
             for p in products),
            key=lambda r: r[0],
        )
        h_sorted = page_content_hash_from_sorted("p1", 1, 0, 10, sorted_rows)
        self.assertEqual(h_unsorted, h_sorted)
        # 改任意载荷必须改变摘要
        changed = [dict(p) for p in products]
        changed[0]["price"] = 999
        self.assertNotEqual(h_unsorted, page_content_hash("p1", 1, 0, 10, changed))
        # 范围变化也必须改变摘要
        self.assertNotEqual(
            h_unsorted, page_content_hash("p1", 1, 0, 11, products),
        )


if __name__ == "__main__":
    unittest.main(verbosity=2)
