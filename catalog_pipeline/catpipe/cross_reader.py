"""采购端跨供应商目录的下游只读 HTTP 服务。

端点
====
``GET /health``
``GET /batches``（含每批采用的限制清单版本与核算时点）
``GET /latest``
``GET /catalog?after_supplier=&after_product_id=&limit=``
``GET /changes?batch=&after_supplier=&after_product_id=&limit=``
``GET /batches/<n>/changes?...``
``GET /batches/<n>/excluded``（该批被有效限制排除的报价及记录编号依据）

所有响应都显式带批次号。目录和变更均使用 ``(供应商, 数值商品 ID)`` keyset
分页，避免一次输出/载入整个大目录。服务以 SQLite ``mode=ro`` 打开采购库，
不能写入批次或声明。
"""

from __future__ import annotations

import json
import os
import sys
import sqlite3
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import parse_qs, urlparse

if __package__ in (None, ""):
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from catpipe.procurement import CrossCatalog
else:
    from .procurement import CrossCatalog

DEFAULT_LIMIT = 200
MAX_LIMIT = 1000


def open_cross_readonly(db_path: str) -> sqlite3.Connection:
    if not os.path.exists(db_path):
        conn = sqlite3.connect(":memory:", isolation_level=None)
        conn.row_factory = sqlite3.Row
        conn.executescript(
            """
            CREATE TABLE cc_batches(batch_number INTEGER PRIMARY KEY,
                created_at TEXT, input_json TEXT, summary_json TEXT);
            CREATE TABLE cc_catalog(group_supplier TEXT, group_product_id INTEGER,
                winner_supplier TEXT, winner_product_id INTEGER, url TEXT,
                price INTEGER, product_revision INTEGER, record_json TEXT,
                batch_number INTEGER);
            CREATE TABLE cc_batch_changes(batch_number INTEGER, op TEXT,
                change_kind TEXT, group_supplier TEXT, group_product_id INTEGER,
                record_json TEXT);
            CREATE TABLE cc_batch_restriction_context(batch_number INTEGER PRIMARY KEY,
                list_id TEXT, version TEXT, as_of TEXT, document_sha256 TEXT,
                source TEXT, context_changed INTEGER);
            CREATE TABLE cc_batch_excluded_quotes(batch_number INTEGER,
                supplier TEXT, product_id INTEGER, list_id TEXT, version TEXT,
                record_id TEXT, as_of TEXT, effective_from TEXT, effective_to TEXT,
                price INTEGER, url TEXT, product_revision INTEGER);
            """
        )
        return conn
    uri = f"file:{os.path.abspath(db_path)}?mode=ro"
    conn = sqlite3.connect(uri, uri=True, isolation_level=None, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=30000")
    conn.execute("PRAGMA query_only=ON")
    return conn


class CrossReaderService:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.conn = open_cross_readonly(db_path)
        self._lock = threading.RLock()

    def close(self) -> None:
        with self._lock:
            self.conn.close()

    def latest_batch_number(self) -> int | None:
        try:
            row = self.conn.execute(
                "SELECT MAX(batch_number) n FROM cc_batches"
            ).fetchone()
        except sqlite3.OperationalError:
            return None
        return row["n"] if row and row["n"] is not None else None

    def list_batches(self) -> list[dict[str, Any]]:
        try:
            rows = self.conn.execute(
                "SELECT b.batch_number,b.created_at,b.summary_json,"
                "       r.list_id,r.version,r.as_of,r.document_sha256,r.source "
                "FROM cc_batches b "
                "LEFT JOIN cc_batch_restriction_context r "
                "  ON r.batch_number=b.batch_number "
                "ORDER BY b.batch_number"
            ).fetchall()
        except sqlite3.OperationalError:
            return []
        out = []
        for row in rows:
            item = {"batch_number": row["batch_number"],
                    "created_at": row["created_at"]}
            try:
                item["summary"] = json.loads(row["summary_json"])
            except (TypeError, json.JSONDecodeError):
                item["summary"] = None
            if row["list_id"] is not None:
                item["restriction_context"] = {
                    "list_id": row["list_id"],
                    "version": row["version"],
                    "as_of": row["as_of"],
                    "document_sha256": row["document_sha256"],
                    "source": row["source"],
                }
            else:
                item["restriction_context"] = None
            out.append(item)
        return out

    def excluded_page(self, batch_number: int) -> dict[str, Any]:
        try:
            rows = self.conn.execute(
                "SELECT * FROM cc_batch_excluded_quotes WHERE batch_number=? "
                "ORDER BY supplier,product_id,record_id",
                (batch_number,),
            ).fetchall()
        except sqlite3.OperationalError:
            return {"batch_number": batch_number, "items": [], "count": 0}
        return {
            "batch_number": batch_number,
            "count": len(rows),
            "items": [dict(r) for r in rows],
        }

    def _limit(self, query: dict[str, list[str]]) -> int:
        raw = query.get("limit", [str(DEFAULT_LIMIT)])[0]
        try:
            return max(1, min(MAX_LIMIT, int(raw)))
        except ValueError:
            return DEFAULT_LIMIT

    def _after_key(self, query: dict[str, list[str]]) -> tuple[str | None, int | None]:
        supplier = query.get("after_supplier", [None])[0]
        raw_pid = query.get("after_product_id", [None])[0]
        if raw_pid in (None, ""):
            return supplier, None
        try:
            return supplier, int(raw_pid)
        except ValueError:
            return supplier, None

    def catalog_page(self, query: dict[str, list[str]]) -> dict[str, Any]:
        batch_number = self.latest_batch_number()
        limit = self._limit(query)
        after_supplier, after_pid = self._after_key(query)
        where = ""
        params: list[Any] = []
        if after_supplier is not None and after_pid is not None:
            where = ("WHERE (group_supplier > ? OR "
                     "(group_supplier = ? AND group_product_id > ?))")
            params = [after_supplier, after_supplier, after_pid]
        params.append(limit + 1)
        rows = self.conn.execute(
            "SELECT group_supplier,group_product_id,record_json,batch_number "
            f"FROM cc_catalog {where} "
            "ORDER BY group_supplier,group_product_id LIMIT ?",
            params,
        ).fetchall()
        has_more = len(rows) > limit
        rows = rows[:limit]
        items = [
            CrossCatalog._present_record(json.loads(r["record_json"]))
            for r in rows
        ]
        next_key = None
        if has_more and rows:
            last = rows[-1]
            next_key = {"after_supplier": last["group_supplier"],
                        "after_product_id": last["group_product_id"]}
        return {
            "batch_number": batch_number,
            "items": items,
            "count": len(items),
            "limit": limit,
            "next": next_key,
        }

    def changes_page(self, batch_number: int | None,
                     query: dict[str, list[str]]) -> dict[str, Any]:
        if batch_number is None:
            batch_number = self.latest_batch_number()
        if batch_number is None:
            return {"batch_number": None, "items": [], "next": None}
        limit = self._limit(query)
        after_supplier, after_pid = self._after_key(query)
        where = "WHERE batch_number=?"
        params: list[Any] = [batch_number]
        if after_supplier is not None and after_pid is not None:
            where += (" AND (group_supplier > ? OR "
                      "(group_supplier = ? AND group_product_id > ?))")
            params.extend([after_supplier, after_supplier, after_pid])
        params.append(limit + 1)
        rows = self.conn.execute(
            "SELECT * FROM cc_batch_changes "
            f"{where} ORDER BY group_supplier,group_product_id LIMIT ?",
            params,
        ).fetchall()
        has_more = len(rows) > limit
        rows = rows[:limit]
        items: list[dict[str, Any]] = []
        for row in rows:
            key = {"supplier": row["group_supplier"],
                   "product_id": row["group_product_id"]}
            item: dict[str, Any] = {
                "batch_number": batch_number,
                "op": row["op"],
                "type": row["op"],
                "action": "upsert" if row["op"] == "put" else "delete",
                "change_type": row["change_kind"],
                "key": key,
                "key_tuple": [key["supplier"], key["product_id"]],
            }
            if row["record_json"]:
                item["record"] = CrossCatalog._present_record(
                    json.loads(row["record_json"]))
            items.append(item)

        next_key = None
        if has_more and rows:
            last = rows[-1]
            next_key = {"after_supplier": last["group_supplier"],
                        "after_product_id": last["group_product_id"]}
        return {
            "batch_number": batch_number,
            "previous_batch_number": batch_number - 1 if batch_number > 1 else None,
            "items": items,
            "count": len(items),
            "limit": limit,
            "next": next_key,
        }

    def build_handler(self) -> type[BaseHTTPRequestHandler]:
        service = self

        class Handler(BaseHTTPRequestHandler):
            server_version = "catpipe-cross-reader/1.0"

            def log_message(self, fmt: str, *args: Any) -> None:
                if os.environ.get("CATPIE_HTTP_VERBOSE"):
                    super().log_message(fmt, *args)

            def _json(self, status: int, payload: dict[str, Any]) -> None:
                body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
                self.send_response(status)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def do_GET(self) -> None:  # noqa: N802
                parsed = urlparse(self.path)
                path = parsed.path.rstrip("/") or "/"
                query = parse_qs(parsed.query)
                try:
                    if path == "/health":
                        self._json(200, {"ok": True})
                    elif path == "/batches":
                        self._json(200, {"batches": service.list_batches()})
                    elif path == "/latest":
                        n = service.latest_batch_number()
                        self._json(200, {"batch_number": n})
                    elif path in ("/catalog", "/snapshot"):
                        self._json(200, service.catalog_page(query))
                    elif path == "/changes":
                        raw_batch = query.get("batch", [None])[0]
                        n = int(raw_batch) if raw_batch else None
                        self._json(200, service.changes_page(n, query))
                    elif path.startswith("/batches/") and path.endswith("/changes"):
                        parts = path.split("/")
                        n = int(parts[2])
                        self._json(200, service.changes_page(n, query))
                    elif path.startswith("/batches/") and path.endswith("/excluded"):
                        parts = path.split("/")
                        n = int(parts[2])
                        self._json(200, service.excluded_page(n))
                    else:
                        self._json(404, {"error": "not_found", "path": path})
                except (ValueError, IndexError):
                    self._json(400, {"error": "bad_request"})
                except Exception as exc:  # pragma: no cover - HTTP boundary
                    self._json(500, {"error": "internal_error", "detail": str(exc)})

        return Handler


def build_server(db_path: str, host: str = "127.0.0.1",
                 port: int = 8091) -> ThreadingHTTPServer:
    service = CrossReaderService(db_path)
    httpd = ThreadingHTTPServer((host, port), service.build_handler())
    httpd.cross_service = service  # type: ignore[attr-defined]
    return httpd


def serve_forever(db_path: str, host: str = "127.0.0.1",
                  port: int = 8091, verbose: bool = True) -> None:
    httpd = build_server(db_path, host, port)
    if verbose:
        print(f"跨供应商下游只读服务: http://{host}:{port}")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        httpd.cross_service.close()  # type: ignore[attr-defined]
        httpd.server_close()


def main(argv: list[str] | None = None) -> int:
    import argparse

    parser = argparse.ArgumentParser(description="跨供应商目录下游只读服务")
    parser.add_argument("--db", required=True)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8091)
    parser.add_argument("--quiet", action="store_true")
    args = parser.parse_args(argv)
    serve_forever(args.db, args.host, args.port, verbose=not args.quiet)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
