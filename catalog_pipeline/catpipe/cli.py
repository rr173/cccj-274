"""命令行入口与端到端场景演示。

子命令
======
``sim``        启动接口模拟器
``collect``    对运行中的模拟器执行采集轮询
``read``       启动下游只读服务
``cross``      采购端跨供应商目录（apply/catalog/changes/serve/demo）
``demo``       一条龙端到端演示（自动拉起/关闭两个本地服务）
``status``     查看候选覆盖、页清单、接收日志、隔离区、已发布版
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.request
from typing import Any

# 支持 `python3 -I catpipe/cli.py ...` 直接运行（隔离模式不读 PYTHONPATH）。
if __package__ in (None, ""):
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from catpipe.collector import Collector
    from catpipe.publisher import coverage_status
    from catpipe.repository import Repository
    from catpipe.simulator import build_server as build_sim
    from catpipe.reader import build_server as build_reader
else:
    from .collector import Collector
    from .publisher import coverage_status
    from .repository import Repository
    from .simulator import build_server as build_sim
    from .reader import build_server as build_reader


# --------------------------------------------------------------------------- #
# 基础子命令
# --------------------------------------------------------------------------- #

def cmd_sim(args: argparse.Namespace) -> None:
    httpd = build_sim(args.host, args.port, verbose=not args.quiet)
    print(f"模拟器已启动: http://{args.host}:{args.port}")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        httpd.shutdown()


def cmd_read(args: argparse.Namespace) -> None:
    if __package__ in (None, ""):
        from catpipe.reader import serve_forever
    else:
        from .reader import serve_forever
    serve_forever(args.db, args.host, args.port, verbose=not args.quiet)


def cmd_collect(args: argparse.Namespace) -> None:
    repo = Repository(args.db)
    try:
        collector = Collector(args.url, repo, token=args.token,
                              verbose=not args.quiet)
        if not collector.token:
            data = _http_get(f"{args.url}/bootstrap")
            collector.token = data["token"]
        report = collector.poll(max_rounds=args.rounds, interval=args.interval)
        print(json.dumps(_report_dict(report), ensure_ascii=False, indent=2))
    finally:
        repo.close()


def cmd_status(args: argparse.Namespace) -> None:
    repo = Repository(args.db)
    try:
        out: dict[str, Any] = {
            "published_versions": [dict(r) for r in repo.list_published_versions()],
            "candidates": [],
            "deliveries": [dict(r) for r in repo.conn.execute(
                "SELECT delivery_id, version_id, page_id, revision, outcome, "
                "reason_code, detail FROM deliveries ORDER BY received_at")],
            "quarantined": [dict(r) for r in repo.conn.execute(
                "SELECT * FROM quarantined ORDER BY moved_at")],
        }
        for row in repo.conn.execute("SELECT * FROM versions ORDER BY created_at"):
            vid = row["version_id"]
            entry = {"version": dict(row)}
            if row["state"] == "candidate":
                entry["coverage"] = coverage_status(repo, vid)
            out["candidates"].append(entry)
        print(json.dumps(out, ensure_ascii=False, indent=2, default=str))
    finally:
        repo.close()


def cmd_cross(args: argparse.Namespace) -> int:
    """转发到采购端跨供应商目录 CLI：``catpipe cross apply ...``。"""
    if __package__ in (None, ""):
        from catpipe.procurement import main as cross_main
    else:
        from .procurement import main as cross_main
    return cross_main(args.cross_args)


def cmd_audit(args: argparse.Namespace) -> int:
    """转发到审计历史来源包 CLI：``catpipe audit export ...``。"""
    if __package__ in (None, ""):
        from catpipe.audit import main as audit_main
    else:
        from .audit import main as audit_main
    argv = list(getattr(args, "audit_args", None) or [])
    # 支持 catpipe audit --db x --batch 1 --out y 的隐式 export 形式。
    if not argv and getattr(args, "db", None):
        argv = ["export", "--db", args.db]
        if getattr(args, "batch", None) is not None:
            argv += ["--batch", str(args.batch)]
        if getattr(args, "out", None):
            argv += ["--out", args.out]
        if getattr(args, "force", False):
            argv.append("--force")
    return audit_main(argv)


# --------------------------------------------------------------------------- #
# 端到端演示
# --------------------------------------------------------------------------- #

def _http_get(url: str) -> Any:
    with urllib.request.urlopen(url, timeout=10) as resp:
        return json.loads(resp.read().decode("utf-8"))


def _http_post(url: str, payload: dict[str, Any]) -> Any:
    body = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        url, data=body, headers={"Content-Type": "application/json"}, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        return {"_http_error": exc.code, "body": json.loads(exc.read().decode("utf-8"))}


def _report_dict(report: Any) -> dict[str, Any]:
    return {
        "rounds": report.rounds,
        "accepted": report.accepted,
        "replaced": report.replaced,
        "noop": report.noop,
        "rejected": report.rejected,
        "abandoned": report.abandoned,
        "published": report.published,
        "publish_blocked": report.publish_blocked,
        "network_errors": report.network_errors,
    }


def _banner(title: str) -> None:
    print("\n" + "=" * 78)
    print(f"  {title}")
    print("=" * 78)


def cmd_demo(args: argparse.Namespace) -> None:
    import shutil
    import tempfile

    workdir = args.workdir or tempfile.mkdtemp(prefix="catpipe-demo-")
    os.makedirs(workdir, exist_ok=True)
    db_path = os.path.join(workdir, "catpipe.db")
    sim_port, read_port = args.sim_port, args.read_port
    sim_url = f"http://127.0.0.1:{sim_port}"

    sim = build_sim("127.0.0.1", sim_port, verbose=False)
    import threading
    threading.Thread(target=sim.serve_forever, daemon=True).start()
    reader = build_reader(db_path, "127.0.0.1", read_port, verbose=False)
    threading.Thread(target=reader.serve_forever, daemon=True).start()
    time.sleep(0.2)

    repo = Repository(db_path, raw_dir=os.path.join(workdir, "raw"))
    token = _http_get(f"{sim_url}/bootstrap")["token"]
    collector = Collector(sim_url, repo, token=token, verbose=True)

    try:
        # ---- 场景 1：首轮采集，含重叠页与下架缺席，发布 v1 --------------
        _banner("场景 1  首轮采集：重叠页 [600,620)、商品 7 下架；整版发布 v1")
        report = collector.run_once()
        assert report.published, "v1 应发布成功"
        v1 = report.published[0]["version_id"]
        latest = _http_get(f"http://127.0.0.1:{read_port}/latest")
        print(f"读者 /latest => 版本 {latest['version_id']}，商品数 {latest['product_count']}")
        p7 = _try_get(f"http://127.0.0.1:{read_port}/versions/{v1}/product/7")
        print(f"读者查已下架商品 7 => {p7}")

        # ---- 场景 2：高修订移窗产生缺口，禁止发布，旧版仍可读 ----------
        _banner("场景 2  移窗缺口：pA 高修订把覆盖从 [0,300) 缩到 [100,300)")
        # 修订页必须完整重发：新窗口 [100,300) 与之一致的全量在售商品。
        shrink = {str(p): [f"https://shop.example/p/{p}", 1000 + p * 10, 1]
                  for p in range(100, 300)}
        _http_post(f"{sim_url}/admin/replace-page",
                   {"page_id": "pA", "lo": 100, "hi": 300, "products": shrink})
        report2 = collector.run_once()
        assert report2.replaced == 1 and report2.publish_blocked, "应替换但因缺口阻断发布"
        gap = report2.publish_blocked[0]["gaps"]
        print(f"发布阻断，缺口 = {gap}（区间证明，未逐 ID 展开）")
        latest2 = _http_get(f"http://127.0.0.1:{read_port}/latest")
        print(f"读者 /latest 仍为旧版 {latest2['version_id']}（发布失败旧版可读）")

        # ---- 场景 3：补页关闭缺口 + URL 迁移 + 改价，发布 v1 修订后的快照
        _banner("场景 3  补新页 pD 关闭缺口；同稳定 ID 的 URL 迁移与价格变更")
        prods_d = {}
        for pid in range(0, 100):
            prods_d[str(pid)] = [f"https://shop.example/p/{pid}", 1000 + pid * 10, 1]
        # 商品 42 URL 迁移到新路径（ID 不变，不另造商品），商品 43 改价
        prods_d["42"][0] = "https://cdn-new.example/items/42"
        prods_d["43"][1] = 5555
        _http_post(f"{sim_url}/admin/upsert-page",
                   {"page_id": "pD", "revision": 1, "lo": 0, "hi": 100,
                    "products": prods_d})
        report3 = collector.run_once()
        assert report3.published, "补齐后应发布成功"
        diff = _http_get(f"http://127.0.0.1:{read_port}/versions/{v1}/diff")
        changed = [i for i in diff["items"] if i["change_type"] == "changed"]
        print(f"差异计数 {diff['counts']}；变更样例: "
              f"{[c['product_id'] for c in changed][:5]} ... 共 {len(changed)} 条")
        p42 = _http_get(f"http://127.0.0.1:{read_port}/versions/{v1}/product/42")
        p43 = _http_get(f"http://127.0.0.1:{read_port}/versions/{v1}/product/43")
        print(f"商品 42 新载荷（ID 不变，URL 已迁移）: {p42}")
        print(f"商品 43 新载荷（ID 不变，价格已变更）: {p43}")
        assert p42["url"] == "https://cdn-new.example/items/42"
        assert p43["price"] == 5555
        # 稳定身份：迁移后商品总数不因 URL 变化而增减
        assert _http_get(f"http://127.0.0.1:{read_port}/latest")["product_count"] == 900

        # ---- 场景 4：重叠撤回导致缺席矛盾，整次拒绝，候选不变 -----------
        _banner("场景 4  重叠撤回：pC 高修订下架 610，但 pB 仍宣称在售 → 拒绝整次接收")
        # 先让模拟器给出与 pB 矛盾的 pC 高修订（610 从 pC 商品中移除）
        _http_post(f"{sim_url}/admin/replace-page",
                   {"page_id": "pC", "delist": [610]})
        report4 = collector.run_once()
        assert report4.rejected and report4.rejected[0]["reason"] == "presence_conflict"
        print(f"拒绝原因: {report4.rejected[0]['detail']}")
        print("候选区保持原状，读者侧快照完全不变")

        # ---- 场景 5：同号异内容重送拒绝 / 同号同内容无操作 --------------
        _banner("场景 5  重送语义：故障注入（重复 ID、越界、坏校验和）与无操作重送")
        for kind in ("duplicate_product", "out_of_range", "bad_checksum", "truncated"):
            _http_post(f"{sim_url}/admin/fault", {"kind": kind})
            r = collector.run_once()
            print(f"故障 {kind:18s} -> 拒绝: {[x['reason'] for x in r.rejected]}")
        r5 = collector.run_once()
        print(f"无故障再拉一轮: accepted={r5.accepted} replaced={r5.replaced} noop/跳过之外无新数据")

        # ---- 场景 6：观察版+令牌失效，放弃整轮后以新观察版重启 ----------
        _banner("场景 6  失效重启：令牌作废 → 放弃候选轮次 → 新观察版 v2 重新发布")
        new_pages = _build_v2_pages(sim_url)
        _http_post(f"{sim_url}/admin/invalidate",
                   {"rollover": True, "new_version_id": "obs-v2",
                    "new_token": "tok-v2-fresh", "pages": new_pages})
        report6 = collector.poll(max_rounds=3, interval=0.1)
        assert report6.abandoned, "旧轮必须被放弃"
        assert report6.published, "新观察版应完整发布"
        print(f"放弃: {report6.abandoned}")
        print(f"新版发布: {report6.published}")
        versions = _http_get(f"http://127.0.0.1:{read_port}/versions")
        print(f"读者可见版本（快照互不混合）: {[v['version_id'] for v in versions['versions']]}")
        v2 = report6.published[0]["version_id"]
        v2_diff = _http_get(f"http://127.0.0.1:{read_port}/versions/{v2}/diff")
        print(f"v2 相对 v1 的差异计数: {v2_diff['counts']}")
        proof = _http_get(f"http://127.0.0.1:{read_port}/versions/{v2}/coverage-proof")
        print(f"v2 覆盖证明: 域={proof['coverage']['domain']} "
              f"合并区间数={len(proof['coverage']['union_segments'])} "
              f"缺口={proof['coverage']['gaps']} 页数={proof['coverage']['page_count']}")

        _banner("演示完成")
        print(f"工作目录（SQLite + 原始响应 + 隔离区）: {workdir}")
        print(f"可用 `python -m catpipe.cli status --db {db_path}` 查看全部接收证据")
    finally:
        reader.shutdown()
        sim.shutdown()
        repo.close()


def _try_get(url: str) -> Any:
    try:
        return _http_get(url)
    except urllib.error.HTTPError as exc:
        return {"HTTP": exc.code, **json.loads(exc.read().decode("utf-8"))}


def _build_v2_pages(base: str) -> list[dict[str, Any]]:
    """构造第二观察版：域不变，商品 0~2 下架、商品 5 改价、其余沿用。"""
    pages: list[dict[str, Any]] = []
    for pid_page, lo, hi in (("pA", 0, 300), ("pB", 300, 620), ("pC", 600, 900)):
        products = {
            str(pid): [f"https://shop.example/p/{pid}",
                       7777 if pid == 5 else 1000 + pid * 10, 1]
            for pid in range(lo, hi)
            if pid not in (0, 1, 2)
        }
        pages.append({"page_id": pid_page, "revision": 1, "lo": lo, "hi": hi,
                      "products": products})
    return pages


# --------------------------------------------------------------------------- #
# 参数解析
# --------------------------------------------------------------------------- #

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="catpipe", description="分页目录采集与整版发布流水线")
    sub = p.add_subparsers(dest="cmd", required=True)

    s = sub.add_parser("sim", help="启动接口模拟器")
    s.add_argument("--host", default="127.0.0.1")
    s.add_argument("--port", type=int, default=8080)
    s.add_argument("--quiet", action="store_true")
    s.set_defaults(func=cmd_sim)

    c = sub.add_parser("collect", help="采集一轮/多轮")
    c.add_argument("--url", default="http://127.0.0.1:8080")
    c.add_argument("--db", required=True)
    c.add_argument("--token")
    c.add_argument("--rounds", type=int, default=10)
    c.add_argument("--interval", type=float, default=0.2)
    c.add_argument("--quiet", action="store_true")
    c.set_defaults(func=cmd_collect)

    r = sub.add_parser("read", help="启动下游只读服务")
    r.add_argument("--db", required=True)
    r.add_argument("--host", default="127.0.0.1")
    r.add_argument("--port", type=int, default=8090)
    r.add_argument("--quiet", action="store_true")
    r.set_defaults(func=cmd_read)

    d = sub.add_parser("demo", help="端到端场景演示")
    d.add_argument("--workdir")
    d.add_argument("--sim-port", type=int, default=8080)
    d.add_argument("--read-port", type=int, default=8090)
    d.set_defaults(func=cmd_demo)

    st = sub.add_parser("status", help="库状态一览")
    st.add_argument("--db", required=True)
    st.set_defaults(func=cmd_status)

    for cross_command in ("cross", "cross-catalog", "procurement", "cross-supplier"):
        cross = sub.add_parser(
            cross_command,
            help="采购端跨供应商目录；后接 apply/catalog/changes/batches/state/demo/serve",
        )
        cross.add_argument("cross_args", nargs=argparse.REMAINDER)
        cross.set_defaults(func=cmd_cross)

    for audit_command in ("audit", "audit-package", "provenance",
                          "source-package", "history-package"):
        audit = sub.add_parser(
            audit_command,
            help="审计历史来源包；后接 export/verify/status/records",
        )
        audit.add_argument("audit_args", nargs=argparse.REMAINDER)
        # 允许 ``catpipe audit --db ... --batch ... --out ...`` 隐式 export。
        audit.add_argument("--db", help=argparse.SUPPRESS)
        audit.add_argument("--batch", type=int, help=argparse.SUPPRESS)
        audit.add_argument("--out", help=argparse.SUPPRESS)
        audit.add_argument("--force", action="store_true", help=argparse.SUPPRESS)
        audit.set_defaults(func=cmd_audit)
    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    rc = args.func(args)
    return int(rc or 0)


if __name__ == "__main__":
    raise SystemExit(main())
