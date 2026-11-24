"""下游只读接口：只能读到完整的已发布观察版快照。

隔离保证
========
1. 读者以 ``mode=ro`` 只读连接打开库，且只查询 ``published_*`` 表；
   采集候选表（versions/pages/evidence/page_staging）从不暴露，
   因此下游不可能读到半成品，也不可能跨轮混合数据。
2. 所有列表接口都以 ``version_id`` 为快照边界；不传则取最新发布版，
   但响应始终显式回带版本 ID，调用方可据此锁定整版读取。
3. 发布是原子单事务，读者要么看到旧版全貌、要么看到新版全貌，
   不存在中间态；发布失败时旧版继续可读。

端点
====
``GET /versions``                       已发布版本清单
``GET /versions/{vid}/coverage-proof``  覆盖证明（区间/缺口/页定位/摘要）
``GET /versions/{vid}/manifest``        页清单与原始响应定位
``GET /versions/{vid}/products?after_id=&limit=``  快照商品（keyset 分页）
``GET /versions/{vid}/product/{pid}``   单商品
``GET /versions/{vid}/diff``            与上一版的新增/变更/未变/下架
``GET /latest``                         最新版摘要
"""

from __future__ import annotations

import json
import os
import sqlite3
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import parse_qs, urlparse

from .protocol import NotFound
from .repository import open_readonly

DEFAULT_LIMIT = 200
MAX_LIMIT = 1000


class ReaderService:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self._lock = threading.Lock()
        self._conn = open_readonly(db_path)

    @property
    def conn(self) -> sqlite3.Connection:
        # 采集端可能在读者启动之后才建库；文件一旦出现就切换到真实只读连接。
        with self._lock:
            if not os.path.exists(self.db_path):
                return self._conn
            if self._conn.execute("PRAGMA database_list").fetchone()[2] == "":
                self._conn.close()
                self._conn = open_readonly(self.db_path)
            return self._conn

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    def require_version(self, version_id: str) -> sqlite3.Row:
        row = self.conn.execute(
            "SELECT * FROM published_versions WHERE version_id=?", (version_id,)
        ).fetchone()
        if row is None:
            raise NotFound(f"已发布观察版 {version_id} 不存在（未发布或已失效的候选不可读）")
        return row

    def latest(self) -> str | None:
        row = self.conn.execute(
            "SELECT version_id FROM published_versions ORDER BY published_at DESC LIMIT 1"
        ).fetchone()
        return row["version_id"] if row else None

    def list_versions(self) -> list[dict[str, Any]]:
        return [dict(r) for r in self.conn.execute(
            "SELECT version_id, domain_lo, domain_hi, published_at "
            "FROM published_versions ORDER BY published_at"
        )]

    def products_page(self, version_id: str, after_id: int | None,
                      limit: int) -> dict[str, Any]:
        self.require_version(version_id)
        rows = list(self.conn.execute(
            "SELECT product_id, url, price, product_revision FROM published_products "
            "WHERE version_id=? " + ("AND product_id>? " if after_id is not None else "")
            + "ORDER BY product_id LIMIT ?",
            ([version_id] + ([after_id] if after_id is not None else []) + [limit]),
        ))
        next_after = rows[-1]["product_id"] if len(rows) == limit else None
        return {
            "version_id": version_id,
            "snapshot": True,
            "limit": limit,
            "after_id": after_id,
            "next_after_id": next_after,
            "items": [dict(r) for r in rows],
        }

    def product(self, version_id: str, product_id: int) -> dict[str, Any]:
        self.require_version(version_id)
        row = self.conn.execute(
            "SELECT * FROM published_products WHERE version_id=? AND product_id=?",
            (version_id, product_id),
        ).fetchone()
        if row is None:
            raise NotFound(f"商品 {product_id} 在版 {version_id} 不存在（范围内缺席即下架）")
        return dict(row)

    def diff(self, version_id: str) -> dict[str, Any]:
        self.require_version(version_id)
        counts = {r["change_type"]: r["c"] for r in self.conn.execute(
            "SELECT change_type, COUNT(*) c FROM published_diff "
            "WHERE version_id=? GROUP BY change_type", (version_id,))}
        for key in ("added", "changed", "unchanged", "removed"):
            counts.setdefault(key, 0)
        items = [dict(r) for r in self.conn.execute(
            "SELECT * FROM published_diff WHERE version_id=? ORDER BY product_id",
            (version_id,),
        )]
        return {"version_id": version_id, "counts": counts, "items": items}


class _Handler(BaseHTTPRequestHandler):
    server_version = "catpipe-reader/1.0"

    def service(self) -> ReaderService:
        return self.server.service  # type: ignore[attr-defined]

    def _json(self, code: int, obj: Any) -> None:
        body = json.dumps(obj, ensure_ascii=False, indent=2).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, fmt: str, *args: Any) -> None:
        if getattr(self.server, "verbose", True):
            super().log_message(fmt, *args)

    def do_GET(self) -> None:  # noqa: N802
        try:
            self._do_get()
        except (BrokenPipeError, ConnectionResetError):
            pass
        except NotFound as exc:
            self._json(404, {"error": "not_found", "message": str(exc)})
        except (ValueError, KeyError) as exc:
            self._json(400, {"error": "bad_request", "message": str(exc)})
        except Exception as exc:  # noqa: BLE001 - 兜底返回 500 而非断连
            self._json(500, {"error": "reader_bug", "message": repr(exc)})

    def _do_get(self) -> None:
        parsed = urlparse(self.path)
        path = parsed.path
        qs = parse_qs(parsed.query)
        svc = self.service()

        if path == "/health":
            self._json(200, {"ok": True})
            return

        if path == "/versions":
            self._json(200, {"versions": svc.list_versions()})
            return

        if path == "/latest":
            vid = svc.latest()
            if vid is None:
                self._json(404, {"error": "no_published_version"})
                return
            row = svc.require_version(vid)
            self._json(200, {
                "version_id": vid,
                "domain": [row["domain_lo"], row["domain_hi"]],
                "published_at": row["published_at"],
                "product_count": svc.conn.execute(
                    "SELECT COUNT(*) c FROM published_products WHERE version_id=?",
                    (vid,)).fetchone()["c"],
            })
            return

        parts = [p for p in path.split("/") if p]
        # /versions/{vid}/...
        if len(parts) >= 2 and parts[0] == "versions":
            vid = parts[1]
            svc.require_version(vid)
            if len(parts) == 2:
                row = svc.require_version(vid)
                self._json(200, {
                    "version_id": vid,
                    "domain": [row["domain_lo"], row["domain_hi"]],
                    "published_at": row["published_at"],
                    "coverage_proof": json.loads(row["coverage_proof_json"]),
                })
                return
            sub = parts[2]
            if sub == "coverage-proof":
                row = svc.require_version(vid)
                self._json(200, json.loads(row["coverage_proof_json"]))
                return
            if sub == "manifest":
                row = svc.require_version(vid)
                self._json(200, json.loads(row["manifest_json"]))
                return
            if sub == "products":
                after_id = int(qs["after_id"][0]) if "after_id" in qs else None
                limit = min(int(qs.get("limit", [DEFAULT_LIMIT])[0]), MAX_LIMIT)
                self._json(200, svc.products_page(vid, after_id, limit))
                return
            if sub == "product" and len(parts) == 4:
                self._json(200, svc.product(vid, int(parts[3])))
                return
            if sub == "diff":
                self._json(200, svc.diff(vid))
                return

        self._json(404, {"error": "not_found", "path": path})


def build_server(db_path: str, host: str, port: int,
                 verbose: bool = True) -> ThreadingHTTPServer:
    server = ThreadingHTTPServer((host, port), _Handler)
    server.service = ReaderService(db_path)  # type: ignore[attr-defined]
    server.verbose = verbose  # type: ignore[attr-defined]
    return server


def serve_forever(db_path: str, host: str, port: int, verbose: bool = True) -> None:
    httpd = build_server(db_path, host, port, verbose=verbose)
    httpd.serve_forever()
