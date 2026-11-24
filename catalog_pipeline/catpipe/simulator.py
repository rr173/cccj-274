"""本地接口模拟器（HTTP/JSONL）。

用途：在没有真实供应商接口的环境下，提供与规格一致的分页目录接口，
并能注入采集端必须正确处理的各种情形。

接口
====
``GET  /manifest?token=...``
    返回当前观察版 ID、令牌、ID 域、全部页声明（页 ID/修订/半开范围）。
``GET  /pages/{page_id}?token=...``
    流式返回一页的 JSONL：页声明、该范围当版全部在售商品、end 行。
    页可按任意顺序、任意次数拉取（乱序与重送由调用方自由构造）。
``POST /admin/seed``            载入一个完整场景（见 :class:`SimState`）
``POST /admin/replace-page``    推送某页高修订（移窗/改价/URL 迁移/撤回）
``POST /admin/invalidate``      使当前观察版+令牌失效，可选滚动到新观察版
``POST /admin/fault``           故障注入（截断/坏校验和/500/重复商品/越界）
``GET  /health``

故障注入开关是一次性的，触发后自动清除。
"""

from __future__ import annotations

import json
import threading
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import parse_qs, urlparse

from .protocol import dump_jsonl_line, stream_checksum


@dataclass
class PageSpec:
    page_id: str
    revision: int
    lo: int
    hi: int
    # product_id -> (url, price, product_revision)；None 表示该 ID 在范围内下架（缺席）
    products: dict[int, tuple[str, int, int]]

    def to_manifest(self) -> dict[str, Any]:
        return {
            "page_id": self.page_id,
            "revision": self.revision,
            "range": {"lo": self.lo, "hi": self.hi},
            "product_count": len(self.products),
        }


@dataclass
class SimState:
    version_id: str
    token: str
    domain_lo: int
    domain_hi: int
    pages: dict[str, PageSpec] = field(default_factory=dict)
    # 已失效的 (version_id, token)，出现即拒绝，模拟"令牌/观察版失效重启"。
    invalidated: set[tuple[str, str]] = field(default_factory=set)
    fault: dict[str, Any] | None = None
    lock: threading.RLock = field(default_factory=threading.RLock)

    def page_ids(self) -> list[str]:
        return sorted(self.pages)


def make_products(
    lo: int, hi: int, *, url_tpl: str = "https://shop.example/p/{}",
    price_step: int = 10, missing: set[int] | None = None,
) -> dict[int, tuple[str, int, int]]:
    """生成半开范围 [lo,hi) 的在售商品；missing 中的 ID 缺席（表示下架）。"""
    missing = missing or set()
    return {
        pid: (url_tpl.format(pid), 1000 + pid * price_step, 1)
        for pid in range(lo, hi)
        if pid not in missing
    }


def render_page_lines(state: SimState, page: PageSpec) -> list[bytes]:
    """构造一页的 JSONL 字节行（含 end 校验和）。"""
    lines = [
        dump_jsonl_line({
            "type": "page",
            "version_id": state.version_id,
            "token": state.token,
            "page_id": page.page_id,
            "revision": page.revision,
            "range": {"lo": page.lo, "hi": page.hi},
            "domain": {"lo": state.domain_lo, "hi": state.domain_hi},
        })
    ]
    checksum = stream_checksum()
    checksum.update(lines[0])
    count = 0
    for pid in sorted(page.products):
        url, price, prev = page.products[pid]
        line = dump_jsonl_line({
            "type": "product", "product_id": pid,
            "url": url, "price": price, "product_revision": prev,
        })
        lines.append(line)
        checksum.update(line)
        count += 1
    lines.append(dump_jsonl_line({
        "type": "end", "count": count, "checksum": checksum.hexdigest(),
    }))
    return lines


def default_scenario() -> SimState:
    """演示场景的第一观察版：三页覆盖域 [0,900)，含重叠、含空窗与下架。"""
    state = SimState(
        version_id="obs-v1", token="tok-v1-abc", domain_lo=0, domain_hi=900,
    )
    state.pages["pA"] = PageSpec("pA", 1, 0, 300, make_products(0, 300, missing={7}))
    state.pages["pB"] = PageSpec("pB", 1, 300, 620, make_products(300, 620))
    state.pages["pC"] = PageSpec("pC", 1, 600, 900, make_products(600, 900))  # 与 pB 重叠 [600,620)
    return state


class _Handler(BaseHTTPRequestHandler):
    server_version = "catpipe-sim/1.0"

    # ---- 工具 -------------------------------------------------------------

    def _state(self) -> SimState:
        return self.server.state  # type: ignore[attr-defined]

    def _json(self, code: int, obj: Any) -> None:
        body = json.dumps(obj, ensure_ascii=False, indent=2).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _token(self) -> str | None:
        q = parse_qs(urlparse(self.path).query)
        return q.get("token", [None])[0]

    def _auth_ok(self, state: SimState, token: str | None) -> bool:
        # 已作废旧令牌在任何后续状态下都必须拒绝（观察版/令牌失效不可复活）。
        if token is None:
            return False
        for (vid, tok) in state.invalidated:
            if tok == token:
                return False
        return token == state.token

    def log_message(self, fmt: str, *args: Any) -> None:
        if getattr(self.server, "verbose", True):
            super().log_message(fmt, *args)

    # ---- 路由 -------------------------------------------------------------

    def do_GET(self) -> None:  # noqa: N802
        try:
            self._do_get()
        except (BrokenPipeError, ConnectionResetError):
            pass
        except Exception as exc:  # noqa: BLE001 - 兜底返回 500 而非断连
            try:
                self._json(500, {"error": "simulator_bug", "message": repr(exc)})
            except Exception:  # noqa: BLE001
                pass
            raise

    def _do_get(self) -> None:
        parsed = urlparse(self.path)
        path = parsed.path
        state = self._state()

        if path == "/health":
            self._json(200, {"ok": True})
            return

        if path == "/bootstrap":
            # 失效重启后的带外引导：只暴露当前有效观察版与新令牌。
            with state.lock:
                self._json(200, {
                    "version_id": state.version_id,
                    "token": state.token,
                    "domain": {"lo": state.domain_lo, "hi": state.domain_hi},
                })
            return

        if path == "/manifest":
            with state.lock:
                if not self._auth_ok(state, self._token()):
                    self._json(401, {"error": "invalid_token",
                                     "message": "观察版或令牌已失效，须放弃本轮并重新开始"})
                    return
                self._json(200, {
                    "version_id": state.version_id,
                    "token": state.token,
                    "domain": {"lo": state.domain_lo, "hi": state.domain_hi},
                    "pages": [state.pages[p].to_manifest() for p in state.page_ids()],
                })
            return

        if path.startswith("/pages/"):
            page_id = path[len("/pages/"):]
            with state.lock:
                if not self._auth_ok(state, self._token()):
                    self._json(401, {"error": "invalid_token"})
                    return
                page = state.pages.get(page_id)
                if page is None:
                    self._json(404, {"error": "unknown_page", "page_id": page_id})
                    return
                fault = state.fault
                state.fault = None  # 一次性
                lines = render_page_lines(state, page)

            self._apply_fault_or_send(fault, lines)
            return

        self._json(404, {"error": "not_found", "path": path})

    def _apply_fault_or_send(self, fault: dict[str, Any] | None, lines: list[bytes]) -> None:
        if not fault:
            self.send_response(200)
            self.send_header("Content-Type", "application/x-ndjson; charset=utf-8")
            self.end_headers()
            for line in lines:
                self.wfile.write(line)
            return

        kind = fault.get("kind")
        try:
            if kind == "http500":
                self._json(500, {"error": "injected_server_error"})
            elif kind == "truncated":
                self.send_response(200)
                self.send_header("Content-Type", "application/x-ndjson; charset=utf-8")
                self.end_headers()
                for line in lines[: max(1, len(lines) - 1)]:  # 丢掉 end 与末行
                    self.wfile.write(line)
            elif kind == "bad_checksum":
                end_obj = json.loads(lines[-1])
                end_obj["checksum"] = "0" * 64
                lines[-1] = dump_jsonl_line(end_obj)
                self.send_response(200)
                self.send_header("Content-Type", "application/x-ndjson")
                self.end_headers()
                for line in lines:
                    self.wfile.write(line)
            elif kind in ("duplicate_product", "out_of_range"):
                product_lines = [json.loads(b) for b in lines
                                 if json.loads(b).get("type") == "product"]
                if kind == "duplicate_product":
                    if not product_lines:
                        # 空页无法构造重复 ID：退化为坏校验和，仍必须被采集端拒绝
                        self._apply_fault_or_send({"kind": "bad_checksum"}, lines)
                        return
                    base = product_lines[0]
                    injected = dump_jsonl_line({
                        "type": "product", "product_id": base["product_id"],
                        "url": "https://evil/dup", "price": 1, "product_revision": 99,
                    })
                    insert_at = 2 if len(lines) > 2 else 1
                else:
                    injected = dump_jsonl_line({
                        "type": "product", "product_id": 2_000_000_000,
                        "url": "https://evil/oor", "price": 1, "product_revision": 1,
                    })
                    insert_at = 1
                lines.insert(insert_at, injected)
                self.send_response(200)
                self.send_header("Content-Type", "application/x-ndjson")
                self.end_headers()
                for line in lines:
                    self.wfile.write(line)
            else:
                self._json(500, {"error": "unknown_fault", "kind": kind})
        except (BrokenPipeError, ConnectionResetError):
            pass

    def do_POST(self) -> None:  # noqa: N802
        try:
            self._do_post()
        except (BrokenPipeError, ConnectionResetError):
            pass
        except Exception as exc:  # noqa: BLE001
            self._json(500, {"error": "simulator_bug", "message": repr(exc)})

    def _do_post(self) -> None:
        parsed = urlparse(self.path)
        length = int(self.headers.get("Content-Length", "0"))
        try:
            payload = json.loads(self.rfile.read(length) or b"{}")
        except ValueError:
            self._json(400, {"error": "bad_json"})
            return
        state = self._state()

        if parsed.path == "/admin/seed":
            new_state = SimState(
                version_id=payload["version_id"], token=payload["token"],
                domain_lo=int(payload["domain_lo"]), domain_hi=int(payload["domain_hi"]),
            )
            for p in payload.get("pages", []):
                prods = {
                    int(pid): tuple(v) for pid, v in p["products"].items()  # type: ignore[arg-type]
                }
                new_state.pages[p["page_id"]] = PageSpec(
                    p["page_id"], int(p["revision"]),
                    int(p["lo"]), int(p["hi"]), prods,
                )
            self.server.state = new_state  # type: ignore[attr-defined]
            self._json(200, {"ok": True, "version_id": new_state.version_id})
            return

        with state.lock:
            if parsed.path in ("/admin/replace-page", "/admin/upsert-page"):
                p = payload
                page = state.pages.get(p["page_id"])
                if page is None:
                    if parsed.path == "/admin/replace-page":
                        self._json(404, {"error": "unknown_page"})
                        return
                    # upsert：全新页，必须显式给出范围与商品集
                    prods = {int(k): tuple(v) for k, v in p["products"].items()}  # type: ignore[arg-type]
                    state.pages[p["page_id"]] = PageSpec(
                        p["page_id"], int(p.get("revision", 1)),
                        int(p["lo"]), int(p["hi"]), prods,
                    )
                    self._json(200, {"ok": True,
                                     "page": state.pages[p["page_id"]].to_manifest()})
                    return
                new_rev = int(p.get("revision", page.revision + 1))
                if new_rev <= page.revision:
                    self._json(409, {"error": "revision_must_advance",
                                     "current": page.revision})
                    return
                prods = page.products
                if p.get("products") is not None:
                    prods = {int(k): tuple(v) for k, v in p["products"].items()}  # type: ignore[arg-type]
                # URL 迁移：同一稳定 ID 换 URL，不新造商品
                for pid, new_url in (p.get("url_migrations") or {}).items():
                    pid = int(pid)
                    if pid in prods:
                        url, price, prev = prods[pid]
                        prods[pid] = (new_url, price, prev)
                for pid, new_price in (p.get("price_changes") or {}).items():
                    pid = int(pid)
                    if pid in prods:
                        url, _price, prev = prods[pid]
                        prods[pid] = (url, int(new_price), prev)
                for pid in (p.get("delist") or []):
                    prods.pop(int(pid), None)
                state.pages[p["page_id"]] = PageSpec(
                    p["page_id"], new_rev,
                    int(p.get("lo", page.lo)), int(p.get("hi", page.hi)), prods,
                )
                self._json(200, {"ok": True, "page": state.pages[p["page_id"]].to_manifest()})
                return

            if parsed.path == "/admin/invalidate":
                # 使当前观察版/令牌失效；可选滚动到全新观察版（新 ID + 新令牌 + 新页）。
                state.invalidated.add((state.version_id, state.token))
                rolled = None
                if payload.get("rollover"):
                    rolled = SimState(
                        version_id=payload.get("new_version_id", state.version_id + "-next"),
                        token=payload["new_token"],
                        domain_lo=state.domain_lo, domain_hi=state.domain_hi,
                        invalidated=set(state.invalidated),
                    )
                    for spec in payload.get("pages", []):
                        prods = {int(k): tuple(v) for k, v in spec["products"].items()}  # type: ignore[arg-type]
                        rolled.pages[spec["page_id"]] = PageSpec(
                            spec["page_id"], int(spec["revision"]),
                            int(spec["lo"]), int(spec["hi"]), prods,
                        )
                    self.server.state = rolled  # type: ignore[attr-defined]
                self._json(200, {"ok": True,
                                 "invalidated": [state.version_id, state.token],
                                 "rolled_to": rolled.version_id if rolled else None})
                return

            if parsed.path == "/admin/fault":
                state.fault = payload
                self._json(200, {"ok": True, "fault": payload})
                return

        self._json(404, {"error": "not_found", "path": parsed.path})


def build_server(host: str, port: int, state: SimState | None = None,
                 verbose: bool = True) -> ThreadingHTTPServer:
    server = ThreadingHTTPServer((host, port), _Handler)
    server.state = state or default_scenario()  # type: ignore[attr-defined]
    server.verbose = verbose  # type: ignore[attr-defined]
    return server


def serve_forever(host: str, port: int, verbose: bool = True) -> ThreadingHTTPServer:
    httpd = build_server(host, port, verbose=verbose)
    httpd.serve_forever()
    return httpd
