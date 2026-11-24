"""SQLite 存储层。

设计要点
========
1. **两套世界分离**：``versions/pages/evidence`` 是当前轮候选区，
   ``published_versions/published_products`` 是已发布快照。下游读者
   永远只能看到后者，未发布候选不可见，因此天然不可能"混轮"。
2. **缺席不展开**：不存任何"缺席商品"行。某商品在页半开范围内的
   缺席由 ``pages.range_lo/range_hi`` 与 ``evidence`` 的存在性
   通过范围 SQL 推导，空窗口不会逐 ID 生成记录。
3. **证据可定位**：每条在售证据带 ``page_id``，每页带原始响应
   落盘定位 ``raw_locator``，覆盖证明随版本持久化。
4. **修订不重扫原文**：高修订替换时，矛盾检查全部走证据表的
   集合 SQL，不重新解析任何旧页原始响应。
"""

from __future__ import annotations

import json
import os
import shutil
import sqlite3
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any, Iterator

SCHEMA = """
CREATE TABLE IF NOT EXISTS versions (
    version_id    TEXT PRIMARY KEY,
    token         TEXT NOT NULL,
    domain_lo     INTEGER NOT NULL,
    domain_hi     INTEGER NOT NULL,
    state         TEXT NOT NULL CHECK (state IN ('candidate','published','abandoned')),
    created_at    TEXT NOT NULL,
    abandoned_at  TEXT,
    abandon_reason TEXT
);

CREATE TABLE IF NOT EXISTS pages (
    version_id   TEXT NOT NULL,
    page_id      TEXT NOT NULL,
    revision     INTEGER NOT NULL,
    range_lo     INTEGER NOT NULL,
    range_hi     INTEGER NOT NULL,
    content_hash TEXT NOT NULL,
    raw_locator  TEXT NOT NULL,
    received_at  TEXT NOT NULL,
    PRIMARY KEY (version_id, page_id),
    FOREIGN KEY (version_id) REFERENCES versions(version_id)
);

-- 在售证据：(版本, 商品, 页) 一行；缺席不建行，由页范围推导。
CREATE TABLE IF NOT EXISTS evidence (
    version_id       TEXT NOT NULL,
    product_id       INTEGER NOT NULL,
    page_id          TEXT NOT NULL,
    url              TEXT NOT NULL,
    price            INTEGER NOT NULL,
    product_revision INTEGER NOT NULL,
    updated_revision INTEGER NOT NULL,  -- 最近一次改动该证据的页修订号
    PRIMARY KEY (version_id, product_id, page_id),
    FOREIGN KEY (version_id) REFERENCES versions(version_id)
);

-- 单次接收的流式暂存（事务内写入，接收结束必然清空）。
CREATE TABLE IF NOT EXISTS page_staging (
    delivery_id      TEXT NOT NULL,
    version_id       TEXT NOT NULL,
    product_id       INTEGER NOT NULL,
    url              TEXT NOT NULL,
    price            INTEGER NOT NULL,
    product_revision INTEGER NOT NULL,
    PRIMARY KEY (delivery_id, product_id)
) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS deliveries (
    delivery_id  TEXT PRIMARY KEY,
    version_id   TEXT,
    page_id      TEXT,
    revision     INTEGER,
    received_at  TEXT NOT NULL,
    outcome      TEXT NOT NULL CHECK (outcome IN ('accepted','replaced','noop','rejected','abandoned')),
    reason_code  TEXT,
    detail       TEXT,
    raw_locator  TEXT,
    content_hash TEXT
);

CREATE TABLE IF NOT EXISTS quarantined (
    raw_locator TEXT PRIMARY KEY,
    delivery_id TEXT NOT NULL,
    reason_code TEXT NOT NULL,
    detail      TEXT,
    moved_at    TEXT NOT NULL
);

-- 已发布世界 ----------------------------------------------------------------

CREATE TABLE IF NOT EXISTS published_versions (
    version_id            TEXT PRIMARY KEY,
    domain_lo             INTEGER NOT NULL,
    domain_hi             INTEGER NOT NULL,
    published_at          TEXT NOT NULL,
    coverage_proof_json   TEXT NOT NULL,
    manifest_json         TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS published_products (
    version_id       TEXT NOT NULL,
    product_id       INTEGER NOT NULL,
    url              TEXT NOT NULL,
    price            INTEGER NOT NULL,
    product_revision INTEGER NOT NULL,
    PRIMARY KEY (version_id, product_id)
);

CREATE TABLE IF NOT EXISTS published_diff (
    version_id             TEXT NOT NULL,
    product_id             INTEGER NOT NULL,
    change_type            TEXT NOT NULL CHECK (change_type IN ('added','changed','unchanged','removed')),
    old_url                TEXT, new_url                TEXT,
    old_price              INTEGER, new_price              INTEGER,
    old_product_revision   INTEGER, new_product_revision   INTEGER,
    PRIMARY KEY (version_id, product_id)
);

CREATE TABLE IF NOT EXISTS ingest_meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
"""


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def new_delivery_id() -> str:
    return uuid.uuid4().hex


class Repository:
    """单连接仓储；采集/发布进程内复用，读者进程以只读模式另开连接。"""

    def __init__(self, db_path: str, raw_dir: str | None = None):
        self.db_path = db_path
        self.base_dir = os.path.dirname(os.path.abspath(db_path))
        self.raw_dir = raw_dir or os.path.join(self.base_dir, "raw")
        self.quarantine_dir = os.path.join(self.base_dir, "quarantine")
        os.makedirs(self.raw_dir, exist_ok=True)
        os.makedirs(self.quarantine_dir, exist_ok=True)
        self.conn = sqlite3.connect(db_path, isolation_level=None)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA busy_timeout=10000")
        self.conn.execute("PRAGMA foreign_keys=ON")
        self.conn.execute("PRAGMA synchronous=NORMAL")
        # 限定页缓存（约 16MB）并把临时结构放内存，使峰值内存与表规模脱钩。
        self.conn.execute("PRAGMA cache_size=-16000")
        self.conn.execute("PRAGMA temp_store=MEMORY")
        self.conn.executescript(SCHEMA)

    def close(self) -> None:
        self.conn.close()

    # ---- 事务 -------------------------------------------------------------

    @contextmanager
    def transaction(self) -> Iterator[sqlite3.Connection]:
        self.conn.execute("BEGIN IMMEDIATE")
        try:
            yield self.conn
            self.conn.execute("COMMIT")
        except BaseException:
            self.conn.execute("ROLLBACK")
            raise

    # ---- 原始响应落盘/隔离 -------------------------------------------------

    def spool_path(self, delivery_id: str) -> str:
        return os.path.join(self.raw_dir, f"{delivery_id}.jsonl")

    def quarantine(self, delivery_id: str, reason_code: str, detail: str) -> str:
        """把拒绝接收的原始响应移入隔离区并登记（不改动候选区）。"""
        src = self.spool_path(delivery_id)
        dst = os.path.join(self.quarantine_dir, f"{delivery_id}.jsonl")
        if os.path.exists(src):
            shutil.move(src, dst)
        locator = dst
        with self.transaction() as conn:
            conn.execute(
                "INSERT OR REPLACE INTO quarantined VALUES (?,?,?,?,?)",
                (locator, delivery_id, reason_code, detail, utc_now()),
            )
        return locator

    # ---- 版本 -------------------------------------------------------------

    def get_version(self, version_id: str) -> sqlite3.Row | None:
        return self.conn.execute(
            "SELECT * FROM versions WHERE version_id=?", (version_id,)
        ).fetchone()

    def create_version(
        self, conn: sqlite3.Connection, version_id: str, token: str,
        domain_lo: int, domain_hi: int,
    ) -> None:
        conn.execute(
            "INSERT INTO versions VALUES (?,?,?,?, 'candidate', ?, NULL, NULL)",
            (version_id, token, domain_lo, domain_hi, utc_now()),
        )

    def abandon_version(self, version_id: str, reason: str) -> None:
        """放弃整轮：删除该版全部候选，旧已发布版保持可读。"""
        with self.transaction() as conn:
            row = conn.execute(
                "SELECT state FROM versions WHERE version_id=?", (version_id,)
            ).fetchone()
            if row is None:
                return
            conn.execute("DELETE FROM evidence WHERE version_id=?", (version_id,))
            conn.execute("DELETE FROM pages WHERE version_id=?", (version_id,))
            conn.execute("DELETE FROM page_staging WHERE version_id=?", (version_id,))
            conn.execute(
                "UPDATE versions SET state='abandoned', abandoned_at=?, abandon_reason=? "
                "WHERE version_id=?",
                (utc_now(), reason, version_id),
            )

    # ---- 页/证据查询 -------------------------------------------------------

    def get_page(self, version_id: str, page_id: str) -> sqlite3.Row | None:
        return self.conn.execute(
            "SELECT * FROM pages WHERE version_id=? AND page_id=?",
            (version_id, page_id),
        ).fetchone()

    def list_pages(self, version_id: str) -> list[sqlite3.Row]:
        return list(self.conn.execute(
            "SELECT * FROM pages WHERE version_id=? ORDER BY range_lo, range_hi, page_id",
            (version_id,),
        ))

    def iter_staging_products_sorted(self, delivery_id: str) -> Iterator[tuple[int, str, int, int]]:
        """按 product_id 有序流式读出暂存商品（供流式内容摘要，不缓存整页）。"""
        cur = self.conn.execute(
            "SELECT product_id, url, price, product_revision FROM page_staging "
            "WHERE delivery_id=? ORDER BY product_id",
            (delivery_id,),
        )
        for r in cur:
            yield (r["product_id"], r["url"], r["price"], r["product_revision"])

    def staging_count(self, delivery_id: str) -> int:
        return self.conn.execute(
            "SELECT COUNT(*) c FROM page_staging WHERE delivery_id=?", (delivery_id,)
        ).fetchone()["c"]

    def clear_staging(self, conn: sqlite3.Connection, delivery_id: str) -> None:
        conn.execute("DELETE FROM page_staging WHERE delivery_id=?", (delivery_id,))

    def clear_staging_safe(self, delivery_id: str) -> None:
        """事务回滚后的兜底清理（独立事务）。"""
        with self.transaction() as conn:
            conn.execute("DELETE FROM page_staging WHERE delivery_id=?", (delivery_id,))

    def iter_page_evidence_sorted(self, version_id: str, page_id: str) -> Iterator[sqlite3.Row]:
        """有序读某页既有证据（供重算内容摘要，不碰原文）。"""
        return iter(self.conn.execute(
            "SELECT product_id, url, price, product_revision FROM evidence "
            "WHERE version_id=? AND page_id=? ORDER BY product_id",
            (version_id, page_id),
        ))

    # ---- 接收日志 ----------------------------------------------------------

    def log_delivery(
        self, delivery_id: str, version_id: str | None, page_id: str | None,
        revision: int | None, outcome: str, reason_code: str | None,
        detail: str | None, raw_locator: str | None, content_hash: str | None,
    ) -> None:
        with self.transaction() as conn:
            conn.execute(
                "INSERT OR REPLACE INTO deliveries VALUES (?,?,?,?,?,?,?,?,?,?)",
                (delivery_id, version_id, page_id, revision, utc_now(),
                 outcome, reason_code, detail, raw_locator, content_hash),
            )

    # ---- 发布世界 ----------------------------------------------------------

    def get_published(self, version_id: str) -> sqlite3.Row | None:
        return self.conn.execute(
            "SELECT * FROM published_versions WHERE version_id=?", (version_id,)
        ).fetchone()

    def latest_published_version(self) -> str | None:
        row = self.conn.execute(
            "SELECT version_id FROM published_versions ORDER BY published_at DESC LIMIT 1"
        ).fetchone()
        return row["version_id"] if row else None

    def list_published_versions(self) -> list[sqlite3.Row]:
        return list(self.conn.execute(
            "SELECT version_id, domain_lo, domain_hi, published_at, manifest_json "
            "FROM published_versions ORDER BY published_at"
        ))

    def iter_published_products(
        self, version_id: str, after_id: int | None = None, limit: int | None = None
    ) -> Iterator[sqlite3.Row]:
        sql = ("SELECT product_id, url, price, product_revision FROM published_products "
               "WHERE version_id=? ")
        params: list[Any] = [version_id]
        if after_id is not None:
            sql += "AND product_id>? "
            params.append(after_id)
        sql += "ORDER BY product_id "
        if limit is not None:
            sql += "LIMIT ?"
            params.append(limit)
        return iter(self.conn.execute(sql, params))

    def count_published_products(self, version_id: str) -> int:
        return self.conn.execute(
            "SELECT COUNT(*) c FROM published_products WHERE version_id=?", (version_id,)
        ).fetchone()["c"]

    def list_diff(self, version_id: str) -> list[sqlite3.Row]:
        return list(self.conn.execute(
            "SELECT * FROM published_diff WHERE version_id=? "
            "ORDER BY change_type, product_id",
            (version_id,),
        ))

    def diff_counts(self, version_id: str) -> dict[str, int]:
        rows = self.conn.execute(
            "SELECT change_type, COUNT(*) c FROM published_diff WHERE version_id=? "
            "GROUP BY change_type",
            (version_id,),
        ).fetchall()
        out = {"added": 0, "changed": 0, "unchanged": 0, "removed": 0}
        for r in rows:
            out[r["change_type"]] = r["c"]
        return out

    def get_meta(self, key: str) -> str | None:
        row = self.conn.execute(
            "SELECT value FROM ingest_meta WHERE key=?", (key,)
        ).fetchone()
        return row["value"] if row else None

    def set_meta(self, key: str, value: str) -> None:
        with self.transaction() as conn:
            conn.execute(
                "INSERT INTO ingest_meta(key,value) VALUES(?,?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (key, value),
            )


def open_readonly(db_path: str) -> sqlite3.Connection:
    """读者进程使用的只读连接；候选区物理上不可写、不可见发布前数据。

    数据库文件尚不存在时（采集端还没建库）连接到一个带只读侧空表的
    临时库，使全部查询返回空集；文件出现后由 :class:`ReaderService`
    自动重连到真实库（以 ``mode=ro`` 打开，物理上无法写入）。

    ``check_same_thread=False``：ThreadingHTTPServer 下连接被多线程
    共享；只读查询 + WAL 模式下并发安全，写者另有独立连接。
    """
    if not os.path.exists(db_path):
        conn = sqlite3.connect(":memory:", isolation_level=None, check_same_thread=False)
        conn.executescript(
            """
            CREATE TABLE published_versions (
                version_id TEXT PRIMARY KEY, domain_lo INTEGER, domain_hi INTEGER,
                published_at TEXT, coverage_proof_json TEXT, manifest_json TEXT);
            CREATE TABLE published_products (
                version_id TEXT, product_id INTEGER, url TEXT,
                price INTEGER, product_revision INTEGER);
            CREATE TABLE published_diff (
                version_id TEXT, product_id INTEGER, change_type TEXT,
                old_url TEXT, new_url TEXT, old_price INTEGER, new_price INTEGER,
                old_product_revision INTEGER, new_product_revision INTEGER);
            """
        )
    else:
        conn = sqlite3.connect(
            f"file:{os.path.abspath(db_path)}?mode=ro",
            uri=True,
            isolation_level=None,
            check_same_thread=False,
        )
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=10000")
    conn.execute("PRAGMA query_only=ON")
    return conn


def coverage_manifest(repo: Repository, version_id: str) -> dict[str, Any]:
    """候选页清单（含原始响应定位），供发布清单与排查使用。"""
    return {
        "version_id": version_id,
        "pages": [
            {
                "page_id": r["page_id"],
                "revision": r["revision"],
                "range": [r["range_lo"], r["range_hi"]],
                "content_hash": r["content_hash"],
                "raw_locator": r["raw_locator"],
                "received_at": r["received_at"],
            }
            for r in repo.list_pages(version_id)
        ],
    }
