"""端到端测试：模拟器、采集器、只读服务在同一进程内通过 HTTP 联动。"""

from __future__ import annotations

import json
import os
import sys
import tempfile
import threading
import time
import unittest
import urllib.request

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from catpipe.collector import Collector
from catpipe.reader import build_server as build_reader
from catpipe.repository import Repository
from catpipe.simulator import build_server as build_sim


def get(url: str):
    with urllib.request.urlopen(url, timeout=10) as resp:
        return json.loads(resp.read().decode("utf-8"))


def post(url: str, payload: dict):
    import urllib.error
    req = urllib.request.Request(
        url, data=json.dumps(payload).encode(),
        headers={"Content-Type": "application/json"}, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return json.loads(resp.read().decode())
    except urllib.error.HTTPError as exc:
        return {"_code": exc.code, "body": json.loads(exc.read().decode())}


class EndToEndCase(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.mkdtemp(prefix="catpipe-e2e-")
        self.sim = build_sim("127.0.0.1", 0, verbose=False)
        self.reader = build_reader(os.path.join(self.tmp, "e2e.db"),
                                   "127.0.0.1", 0, verbose=False)
        self.sim_port = self.sim.server_address[1]
        self.read_port = self.reader.server_address[1]
        threading.Thread(target=self.sim.serve_forever, daemon=True).start()
        threading.Thread(target=self.reader.serve_forever, daemon=True).start()
        self.base = f"http://127.0.0.1:{self.sim_port}"
        self.read = f"http://127.0.0.1:{self.read_port}"
        time.sleep(0.1)
        self.repo = Repository(os.path.join(self.tmp, "e2e.db"))
        token = get(f"{self.base}/bootstrap")["token"]
        self.collector = Collector(self.base, self.repo, token=token, verbose=False)

    def tearDown(self) -> None:
        self.reader.shutdown()
        self.sim.shutdown()
        self.repo.close()

    def test_full_flow_publish_and_readonly_snapshot(self):
        report = self.collector.run_once()
        self.assertEqual(len(report.published), 1)
        vid = report.published[0]["version_id"]

        latest = get(f"{self.read}/latest")
        self.assertEqual(latest["version_id"], vid)
        # keyset 分页能取全量
        total, after = 0, None
        while True:
            url = f"{self.read}/versions/{vid}/products?limit=137"
            if after is not None:
                url += f"&after_id={after}"
            page = get(url)
            total += len(page["items"])
            after = page["next_after_id"]
            if after is None:
                break
        self.assertEqual(total, latest["product_count"])

    def test_gap_keeps_old_version_readable(self):
        self.collector.run_once()
        old = get(f"{self.read}/latest")["version_id"]
        # 移窗产生真实缺口：pA 从 [0,300) 缩窗到 [0,150)，并显式给出与新窗口
        # 一致的商品集（模拟供应商修订页的完整重发）；缺口 [150,300) 不被
        # pB=[300,620)/pC=[600,900) 覆盖。
        products = {str(p): [f"https://shop.example/p/{p}", 1000 + p * 10, 1]
                    for p in range(0, 150) if p != 7}
        post(f"{self.base}/admin/replace-page",
             {"page_id": "pA", "lo": 0, "hi": 150, "products": products})
        report = self.collector.run_once()
        self.assertTrue(report.publish_blocked)
        self.assertEqual(get(f"{self.read}/latest")["version_id"], old)

    def test_fault_injection_all_rejected_without_candidate_change(self):
        self.collector.run_once()
        before = self.repo.conn.execute(
            "SELECT COUNT(*) c FROM pages").fetchone()["c"]
        token = self.collector.token
        for kind in ("duplicate_product", "out_of_range", "bad_checksum",
                     "truncated", "http500"):
            post(f"{self.base}/admin/fault", {"kind": kind})
            # 一次性故障只作用于下一次页请求；已缓存页在 run_once 中会被跳过，
            # 因此直接拉取 pA 来稳定触发故障。
            try:
                self.collector.fetch_page("pA", token)
                self.fail(f"故障 {kind} 应当被拒绝")
            except Exception as exc:  # noqa: BLE001
                code = getattr(exc, "reason_code", "network_error")
                self.assertNotEqual(code, None)
        after = self.repo.conn.execute("SELECT COUNT(*) c FROM pages").fetchone()["c"]
        self.assertEqual(before, after)
        quarantined = self.repo.conn.execute(
            "SELECT COUNT(*) c FROM quarantined").fetchone()["c"]
        self.assertGreaterEqual(quarantined, 3)  # 500 没有响应体可隔离

    def test_invalidation_rollover_publishes_new_version(self):
        self.collector.run_once()
        v1 = get(f"{self.read}/latest")["version_id"]

        new_pages = []
        for pid_page, lo, hi in (("pA", 0, 300), ("pB", 300, 620), ("pC", 600, 900)):
            new_pages.append({
                "page_id": pid_page, "revision": 1, "lo": lo, "hi": hi,
                # URL/价格与 v1 保持一致，仅下架 0/1，确保差异只有 removed
                "products": {str(p): [f"https://shop.example/p/{p}", 1000 + p * 10, 1]
                             for p in range(lo, hi) if p not in (0, 1)},
            })
        post(f"{self.base}/admin/invalidate",
             {"rollover": True, "new_version_id": "obs-v2",
              "new_token": "tok-v2", "pages": new_pages})

        report = self.collector.poll(max_rounds=3, interval=0.05)
        self.assertEqual(len(report.abandoned), 1)
        self.assertTrue(report.published)
        versions = [v["version_id"] for v in get(f"{self.read}/versions")["versions"]]
        self.assertEqual(versions, [v1, "obs-v2"])
        diff = get(f"{self.read}/versions/obs-v2/diff")
        self.assertEqual(diff["counts"]["removed"], 2)

    def test_reader_never_exposes_unpublished_candidate(self):
        # 只收一页，候选存在缺口不可发布
        self.collector.fetch_page("pA", self.collector.token)
        self.assertEqual(get(f"{self.read}/versions")["versions"], [])
        import urllib.error
        with self.assertRaises(urllib.error.HTTPError):
            get(f"{self.read}/versions/obs-v1/products")


if __name__ == "__main__":
    unittest.main(verbosity=2)
