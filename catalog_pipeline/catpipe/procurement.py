"""采购端跨供应商同款目录与批次变更。

本模块只读取各供应商已经发布的 catpipe SQLite 库（其中的
``published_versions`` / ``published_products``），不接触采集协议、候选区或
源库原文。跨供应商库保存：

* 各供应商上次成功引入的在售报价（源库之后变化不会自动跟随）；
* 人工同款声明及其生命周期；
* 由有效声明传递连通得到的同款组与当前目录快照；
* 每个成功批次相对上一成功批次的键级 put/delete 变更。

批次应用是单 SQLite 事务：源版本读取、报价替换、声明编辑、受影响组重算、
快照与批次变更要么一起提交，要么一起回滚。
"""

from __future__ import annotations

import json
import os
import sys
import sqlite3
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Iterator, Sequence

if __package__ in (None, ""):
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from catpipe.repository import utc_now
else:
    from .repository import utc_now

__all__ = [
    "ProductRef",
    "CrossCatalog",
    "ProcurementCatalog",
    "CrossSupplierCatalog",
    "CrossVendorCatalog",
    "CatalogBatchStore",
    "ProcurementService",
    "CrossCatalogError",
    "InvalidBatch",
    "BatchValidationError",
    "BatchError",
    "ValidationError",
    "SourceVersionNotFound",
    "apply_batch",
    "commit_batch",
    "process_batch",
    "main",
]


class CrossCatalogError(Exception):
    """跨供应商目录功能的基础异常。"""


class InvalidBatch(CrossCatalogError):
    """批次内容或声明编辑无效。"""


class BatchValidationError(InvalidBatch):
    """兼容可能的命名习惯。"""


class BatchError(InvalidBatch):
    """兼容可能的命名习惯。"""


class ValidationError(InvalidBatch):
    """兼容可能的命名习惯。"""


class SourceVersionNotFound(InvalidBatch):
    """批次指定的供应商库中找不到已发布观察版。"""


SCHEMA = """
CREATE TABLE IF NOT EXISTS cc_meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS cc_batches (
    batch_number INTEGER PRIMARY KEY,
    created_at   TEXT NOT NULL,
    input_json   TEXT,
    summary_json TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS cc_supplier_versions (
    supplier      TEXT PRIMARY KEY,
    db_path       TEXT NOT NULL,
    version_id    TEXT NOT NULL,
    selected_batch INTEGER NOT NULL
) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS cc_supplier_quotes (
    supplier         TEXT NOT NULL,
    product_id       INTEGER NOT NULL,
    url              TEXT NOT NULL,
    price            INTEGER NOT NULL,
    product_revision INTEGER NOT NULL,
    batch_number     INTEGER NOT NULL,
    PRIMARY KEY (supplier, product_id)
) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS cc_claims (
    claim_id         TEXT PRIMARY KEY,
    supplier_a       TEXT NOT NULL,
    product_a        INTEGER NOT NULL,
    supplier_b       TEXT NOT NULL,
    product_b        INTEGER NOT NULL,
    status           TEXT NOT NULL CHECK (status IN ('active', 'withdrawn')),
    created_batch    INTEGER NOT NULL,
    updated_batch    INTEGER NOT NULL,
    withdrawn_batch  INTEGER
);

-- 每条有效声明展开成两条有向边，供递归连通遍历使用。
CREATE TABLE IF NOT EXISTS cc_claim_endpoints (
    claim_id        TEXT NOT NULL,
    supplier        TEXT NOT NULL,
    product_id      INTEGER NOT NULL,
    other_supplier  TEXT NOT NULL,
    other_product_id INTEGER NOT NULL,
    PRIMARY KEY (supplier, product_id, claim_id)
) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS idx_cc_endpoint_claim
    ON cc_claim_endpoints(claim_id);

-- 所有当前身份的分组成员关系，包括暂无报价的声明组。
CREATE TABLE IF NOT EXISTS cc_group_members (
    group_supplier TEXT NOT NULL,
    group_product_id INTEGER NOT NULL,
    supplier       TEXT NOT NULL,
    product_id     INTEGER NOT NULL,
    batch_number   INTEGER NOT NULL,
    PRIMARY KEY (group_supplier, group_product_id, supplier, product_id)
) WITHOUT ROWID;
CREATE UNIQUE INDEX IF NOT EXISTS idx_cc_member_identity
    ON cc_group_members(supplier, product_id);

CREATE TABLE IF NOT EXISTS cc_catalog (
    group_supplier    TEXT NOT NULL,
    group_product_id  INTEGER NOT NULL,
    winner_supplier   TEXT NOT NULL,
    winner_product_id INTEGER NOT NULL,
    url               TEXT NOT NULL,
    price             INTEGER NOT NULL,
    product_revision  INTEGER NOT NULL,
    record_json       TEXT NOT NULL,
    batch_number      INTEGER NOT NULL,
    PRIMARY KEY (group_supplier, group_product_id)
) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS idx_cc_catalog_price
    ON cc_catalog(price, winner_supplier, winner_product_id);

CREATE TABLE IF NOT EXISTS cc_batch_changes (
    batch_number    INTEGER NOT NULL,
    op              TEXT NOT NULL CHECK (op IN ('put', 'delete')),
    change_kind     TEXT NOT NULL CHECK (change_kind IN ('added', 'changed', 'removed')),
    group_supplier  TEXT NOT NULL,
    group_product_id INTEGER NOT NULL,
    record_json     TEXT,
    PRIMARY KEY (batch_number, group_supplier, group_product_id)
) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS idx_cc_changes_batch_op
    ON cc_batch_changes(batch_number, op);

-- 以下均为批次内工作表；成功结束时清空，失败随事务回滚。
CREATE TABLE IF NOT EXISTS cc_batch_staging (
    supplier         TEXT NOT NULL,
    product_id       INTEGER NOT NULL,
    url              TEXT NOT NULL,
    price            INTEGER NOT NULL,
    product_revision INTEGER NOT NULL,
    PRIMARY KEY (supplier, product_id)
) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS cc_batch_quote_changes (
    supplier    TEXT NOT NULL,
    product_id  INTEGER NOT NULL,
    change_kind TEXT NOT NULL CHECK (change_kind IN ('inserted', 'deleted', 'changed')),
    PRIMARY KEY (supplier, product_id)
) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS cc_batch_claim_roots (
    batch_number INTEGER NOT NULL,
    supplier     TEXT NOT NULL,
    product_id   INTEGER NOT NULL,
    PRIMARY KEY (batch_number, supplier, product_id)
) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS cc_batch_old_keys (
    batch_number    INTEGER NOT NULL,
    group_supplier  TEXT NOT NULL,
    group_product_id INTEGER NOT NULL,
    PRIMARY KEY (batch_number, group_supplier, group_product_id)
) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS cc_batch_claim_old_keys (
    batch_number    INTEGER NOT NULL,
    group_supplier  TEXT NOT NULL,
    group_product_id INTEGER NOT NULL,
    PRIMARY KEY (batch_number, group_supplier, group_product_id)
) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS cc_batch_new_keys (
    batch_number    INTEGER NOT NULL,
    group_supplier  TEXT NOT NULL,
    group_product_id INTEGER NOT NULL,
    PRIMARY KEY (batch_number, group_supplier, group_product_id)
) WITHOUT ROWID;
"""


# --------------------------------------------------------------------------- #
# 输入解析
# --------------------------------------------------------------------------- #

@dataclass(frozen=True, slots=True)
class ProductRef:
    """商品稳定身份：供应商名 + 该供应商的数值稳定商品 ID。"""

    supplier: str
    product_id: int

    def __post_init__(self) -> None:
        if not isinstance(self.supplier, str) or not self.supplier.strip():
            raise InvalidBatch("商品身份缺少非空 supplier")
        if isinstance(self.product_id, bool) or not isinstance(self.product_id, int):
            raise InvalidBatch("product_id 必须是整数")
        if self.product_id < 0:
            # catpipe 的稳定 ID 域为非负整数；这里保持同样约束。
            raise InvalidBatch("product_id 不能为负数")

    @property
    def key(self) -> tuple[str, int]:
        return (self.supplier, self.product_id)

    def to_dict(self) -> dict[str, Any]:
        return {"supplier": self.supplier, "product_id": self.product_id}


def _norm_key(key: Any) -> str:
    return "".join(ch for ch in str(key).lower() if ch not in "_- ")


def _get_field(obj: dict[str, Any], names: Sequence[str]) -> Any:
    if not isinstance(obj, dict):
        return None
    normalized = {_norm_key(k): v for k, v in obj.items()}
    for name in names:
        key = _norm_key(name)
        if key in normalized and normalized[key] is not None:
            return normalized[key]
    return None


def parse_product_id(value: Any) -> int:
    if isinstance(value, ProductRef):
        return value.product_id
    if isinstance(value, bool):
        raise InvalidBatch("product_id 不能是布尔值")
    if isinstance(value, int):
        pid = value
    elif isinstance(value, str) and value.strip():
        try:
            pid = int(value.strip())
        except ValueError as exc:
            raise InvalidBatch(f"无法解析数值 product_id: {value!r}") from exc
    elif isinstance(value, float) and value.is_integer():
        pid = int(value)
    else:
        raise InvalidBatch(f"无法解析数值 product_id: {value!r}")
    if pid < 0:
        raise InvalidBatch("product_id 不能为负数")
    return pid


_SUPPLIER_FIELDS = ("supplier", "supplier_name", "vendor", "vendor_name", "seller", "name")
_ID_FIELDS = ("product_id", "stable_product_id", "stable_id", "id", "pid", "productid")


def parse_ref(value: Any, *, default_supplier: str | None = None) -> ProductRef:
    """把多种合理的身份表示归一化为 :class:`ProductRef`。"""
    if isinstance(value, ProductRef):
        return value
    if isinstance(value, (tuple, list)) and len(value) == 2:
        supplier = value[0] if value[0] is not None else default_supplier
        if supplier is None:
            raise InvalidBatch("身份二元组缺少供应商名")
        return ProductRef(str(supplier), parse_product_id(value[1]))
    if isinstance(value, str):
        for sep in ("#", ":", "/"):
            if sep in value:
                s, p = value.split(sep, 1)
                return ProductRef(s, parse_product_id(p))
        if default_supplier is not None:
            return ProductRef(default_supplier, parse_product_id(value))
        raise InvalidBatch(f"无法解析商品身份: {value!r}")
    if isinstance(value, dict):
        supplier = _get_field(value, _SUPPLIER_FIELDS)
        if supplier is None:
            supplier = default_supplier
        pid = _get_field(value, _ID_FIELDS)
        if pid is None:
            raise InvalidBatch(f"商品身份缺少 product_id: {value!r}")
        if supplier is None:
            raise InvalidBatch(f"商品身份缺少 supplier: {value!r}")
        return ProductRef(str(supplier), parse_product_id(pid))
    if default_supplier is not None:
        return ProductRef(default_supplier, parse_product_id(value))
    raise InvalidBatch(f"无法解析商品身份: {value!r}")


_SIDE_FIELD_SETS = {
    "a": (
        ("a", "left", "source", "first", "product_a", "item_a", "identity_a", "x", "from"),
        ("supplier_a", "a_supplier", "left_supplier", "source_supplier",
         "supplier_name_a", "vendor_a"),
        ("product_id_a", "a_product_id", "left_product_id", "source_product_id",
         "product_a_id", "pid_a", "a_id", "id_a", "left_id", "source_id"),
    ),
    "b": (
        ("b", "right", "target", "second", "product_b", "item_b", "identity_b", "y", "to"),
        ("supplier_b", "b_supplier", "right_supplier", "target_supplier",
         "supplier_name_b", "vendor_b"),
        ("product_id_b", "b_product_id", "right_product_id", "target_product_id",
         "product_b_id", "pid_b", "b_id", "id_b", "right_id", "target_id"),
    ),
}


def _parse_claim_side(obj: dict[str, Any], side: str) -> ProductRef | None:
    container_names, supplier_names, id_names = _SIDE_FIELD_SETS[side]
    container = _get_field(obj, container_names)
    supplier = _get_field(obj, supplier_names)
    if isinstance(container, (dict, tuple, list, str, ProductRef)):
        return parse_ref(container)
    if container is not None and supplier is not None:
        return parse_ref(container, default_supplier=str(supplier))
    pid = _get_field(obj, id_names)
    if pid is not None:
        return parse_ref({"supplier": supplier, "product_id": pid})
    # 例如 {"supplier_a":"A","product_a": 1}，product_a 可能直接是 ID。
    direct = _get_field(obj, (f"product_{side}", f"item_{side}"))
    if direct is not None:
        return parse_ref(
            direct,
            default_supplier=str(supplier) if supplier is not None else None,
        )
    return None


_ADD_OPS = {"add", "create", "insert", "new", "declare", "link"}


def _parse_db_version_string(value: str) -> tuple[str | None, str | None]:
    """解析 ``/path/a.db#v1``、``/path/a.db::v1`` 或纯版本号。"""
    text = value.strip()
    if "?version=" in text or "?version_id=" in text:
        from urllib.parse import parse_qs, urlparse

        parsed = urlparse(text)
        query = parse_qs(parsed.query)
        version = (query.get("version") or query.get("version_id") or [None])[0]
        return parsed.path or text, version
    for sep in ("::", "#", "@"):
        if sep in text:
            db_path, version = text.split(sep, 1)
            return db_path, version
    if text.endswith((".db", ".sqlite", ".sqlite3")):
        return text, None
    return None, text


_REPLACE_OPS = {"replace", "update", "revise", "revision", "edit", "put", "upsert"}
_WITHDRAW_OPS = {"withdraw", "retract", "revoke", "remove", "delete", "delist", "cancel"}


def normalize_op(value: Any) -> str:
    op = _norm_key(value or "upsert")
    if op in _ADD_OPS:
        return "add"
    if op in _REPLACE_OPS:
        return "replace" if op != "upsert" else "upsert"
    if op in _WITHDRAW_OPS:
        return "withdraw"
    raise InvalidBatch(f"未知声明操作: {value!r}")


@dataclass(slots=True)
class SupplierSelection:
    name: str
    db_path: str | None
    version_id: str | None
    explicit_version: bool = True


@dataclass(slots=True)
class ClaimOp:
    op: str
    claim_id: str
    a: ProductRef | None = None
    b: ProductRef | None = None
    old_a: ProductRef | None = None
    old_b: ProductRef | None = None


def _load_batch_dict(batch: Any) -> dict[str, Any]:
    if batch is None:
        return {}
    if isinstance(batch, (str, os.PathLike)):
        path = Path(batch)
        try:
            with path.open("r", encoding="utf-8") as fh:
                batch = json.load(fh)
        except (OSError, json.JSONDecodeError) as exc:
            raise InvalidBatch(f"无法读取批次文件 {path}: {exc}") from exc
    elif isinstance(batch, (bytes, bytearray)):
        try:
            batch = json.loads(bytes(batch).decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise InvalidBatch(f"无法解析批次 JSON: {exc}") from exc
    if not isinstance(batch, dict):
        raise InvalidBatch("批次必须是 JSON 对象/dict")
    return batch


_SUPPLIER_KEYS = (
    "suppliers", "supplier", "supplier_versions", "selections",
    "selected_versions", "version_selections", "sources", "source",
    "catalogs", "catalog", "vendor_catalogs", "vendors", "vendor",
)
_CLAIM_KEYS = (
    "claims", "same_product_claims", "same_products", "same_as",
    "match_claims", "matches", "assertions", "declarations",
    "manual_claims", "links", "relations",
)
_ADD_CLAIM_KEYS = (
    "add", "add_claims", "claims_add", "new", "new_claims",
    "added_claims", "create", "create_claims",
)
_REPLACE_CLAIM_KEYS = (
    "replace", "replace_claims", "claims_replace", "update",
    "updated_claims", "update_claims",
)
_WITHDRAW_CLAIM_KEYS = (
    "withdraw", "withdraw_claims", "withdraw_claim_ids", "claims_withdraw",
    "retract", "retract_claims", "retract_claim_ids", "remove",
    "remove_claims", "remove_claim_ids", "withdrawn_claims",
    "retracted_claims", "delete", "delete_claims", "deleted_claim_ids",
)


def _as_list(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    if isinstance(value, tuple):
        return list(value)
    return [value]


def parse_suppliers(batch: dict[str, Any], explicit: Any = None) -> list[SupplierSelection]:
    raw: Any = explicit
    if raw is None:
        for key in _SUPPLIER_KEYS:
            if key in batch:
                raw = batch[key]
                break
    if raw is None:
        return []

    entries: list[tuple[Any, Any]] = []
    if isinstance(raw, dict):
        for name, value in raw.items():
            if isinstance(value, dict):
                value = dict(value)
                value.setdefault("name", name)
            entries.append((name, value))
    else:
        for value in _as_list(raw):
            entries.append((None, value))

    out: list[SupplierSelection] = []
    seen: set[str] = set()
    for dict_name, value in entries:
        if isinstance(value, str):
            # {"A": "/path/a.db#v1"} / {"A": "/path/a.db::v1"} 的紧凑形式。
            parsed_db, parsed_version = _parse_db_version_string(value)
            if dict_name is not None:
                name = str(dict_name)
                db_path, version = parsed_db, parsed_version
            elif parsed_db and parsed_version:
                name = Path(parsed_db).stem
                db_path, version = parsed_db, parsed_version
            else:
                raise InvalidBatch(f"供应商选择缺少名称和版本/库信息: {value!r}")
        elif isinstance(value, (tuple, list)):
            if len(value) == 2:
                if dict_name is not None:
                    name = str(dict_name)
                    db_path, version = value
                else:
                    name, db_path, version = str(value[0]), value[1], None
                    # 支持 [name, db, version] 三元组。
                    if len(value) == 3:
                        db_path, version = value[1], value[2]
            elif len(value) == 3:
                name, db_path, version = str(value[0]), value[1], value[2]
            else:
                raise InvalidBatch(f"无法解析供应商选择: {value!r}")
        elif isinstance(value, dict):
            name_value = _get_field(value, _SUPPLIER_FIELDS) or dict_name
            if name_value is None:
                raise InvalidBatch(f"供应商选择缺少 name: {value!r}")
            name = str(name_value)
            db_path = _get_field(value, (
                "db", "db_path", "database", "database_path", "catalog_db",
                "catalog_db_path", "catalog_path", "path", "sqlite",
                "sqlite_path", "source_db", "source_db_path",
                "supplier_db", "supplier_db_path", "directory_db",
                "catalog_database", "catalog_database_path",
            ))
            version = _get_field(value, (
                "version", "version_id", "published_version",
                "published_version_id", "observed_version", "observation_version",
                "observation_version_id", "selected_version",
                "selected_version_id", "published_observation_version",
                "published_observation_version_id",
            ))
        else:
            raise InvalidBatch(f"无法解析供应商选择: {value!r}")

        if not name.strip():
            raise InvalidBatch("供应商名不能为空")
        if name in seen:
            raise InvalidBatch(f"一批内供应商只允许指定一次: {name}")
        seen.add(name)
        out.append(SupplierSelection(
            name=name,
            db_path=None if db_path is None else str(db_path),
            version_id=None if version is None else str(version),
            explicit_version=version is not None,
        ))
    return out


def _claim_id(obj: Any) -> str:
    if isinstance(obj, dict):
        cid = _get_field(obj, ("claim_id", "assertion_id", "declaration_id", "match_id", "id"))
    else:
        cid = obj
    if cid is None or str(cid).strip() == "":
        raise InvalidBatch("声明缺少非空 claim_id")
    return str(cid)


def _parse_claim_object(value: Any, forced_op: str | None = None) -> ClaimOp:
    if not isinstance(value, dict):
        # 撤回列表可以直接放 claim ID。
        if forced_op == "withdraw":
            return ClaimOp(op="withdraw", claim_id=_claim_id(value))
        raise InvalidBatch(f"声明必须是对象: {value!r}")

    cid = _claim_id(value)
    if forced_op is not None:
        op = forced_op
    else:
        raw_op = _get_field(value, ("op", "action", "operation", "verb", "method"))
        if raw_op is None:
            if _get_field(value, ("withdraw", "withdrawn", "retract", "retracted",
                                  "removed", "deleted", "revoked")) is True:
                op = "withdraw"
            else:
                op = "upsert"
        else:
            op = normalize_op(raw_op)

    if op == "withdraw":
        return ClaimOp(op="withdraw", claim_id=cid)

    a = _parse_claim_side(value, "a")
    b = _parse_claim_side(value, "b")
    if a is None or b is None:
        identities = _get_field(value, ("identities", "products", "items", "pair", "refs"))
        if isinstance(identities, (list, tuple)) and len(identities) == 2:
            a = a or parse_ref(identities[0])
            b = b or parse_ref(identities[1])
    if a is None or b is None:
        raise InvalidBatch(f"声明 {cid} 必须连接两个商品: {value!r}")
    if a == b:
        raise InvalidBatch(f"声明 {cid} 不能连接同一个商品: {a.to_dict()}")
    return ClaimOp(op=op, claim_id=cid, a=a, b=b)


def parse_claims(batch: dict[str, Any], explicit: Any = None) -> list[ClaimOp]:
    ops: list[ClaimOp] = []
    if explicit is not None:
        for value in _as_list(explicit):
            ops.append(_parse_claim_object(value))
    else:
        raw_claims = None
        for key in _CLAIM_KEYS:
            if key in batch:
                raw_claims = batch[key]
                break
        for value in _as_list(raw_claims):
            ops.append(_parse_claim_object(value))

    for key in _ADD_CLAIM_KEYS:
        for value in _as_list(batch.get(key)):
            ops.append(_parse_claim_object(value, "add"))
    for key in _REPLACE_CLAIM_KEYS:
        for value in _as_list(batch.get(key)):
            ops.append(_parse_claim_object(value, "replace"))
    for key in _WITHDRAW_CLAIM_KEYS:
        for value in _as_list(batch.get(key)):
            ops.append(_parse_claim_object(value, "withdraw"))

    seen: set[str] = set()
    for op in ops:
        if op.claim_id in seen:
            raise InvalidBatch(f"一批内同一声明只允许指定一次: {op.claim_id}")
        seen.add(op.claim_id)
    return ops


# --------------------------------------------------------------------------- #
# 跨供应商目录
# --------------------------------------------------------------------------- #

class CrossCatalog:
    """一个 SQLite 文件中的采购端跨供应商目录。"""

    def __init__(self, db_path: str | os.PathLike[str] = "cross_catalog.db"):
        self.db_path = os.fspath(db_path)
        if self.db_path != ":memory:":
            parent = os.path.dirname(os.path.abspath(self.db_path))
            os.makedirs(parent, exist_ok=True)
        self.conn = sqlite3.connect(self.db_path, isolation_level=None)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA busy_timeout=30000")
        self.conn.execute("PRAGMA foreign_keys=ON")
        if self.db_path != ":memory:":
            self.conn.execute("PRAGMA journal_mode=WAL")
            self.conn.execute("PRAGMA synchronous=NORMAL")
        # 限制页缓存并让临时结构落盘，使总目录可以大于进程内存。
        self.conn.execute("PRAGMA cache_size=-20000")  # ~20 MiB
        self.conn.execute("PRAGMA temp_store=FILE")
        self.conn.executescript(SCHEMA)
        self.conn.execute(
            "INSERT OR IGNORE INTO cc_meta(key,value) VALUES('last_batch_number','0')"
        )

    def close(self) -> None:
        self.conn.close()

    def __enter__(self) -> "CrossCatalog":
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        self.close()

    @contextmanager
    def transaction(self) -> Iterator[sqlite3.Connection]:
        self.conn.execute("BEGIN IMMEDIATE")
        try:
            yield self.conn
            self.conn.execute("COMMIT")
        except BaseException:
            self.conn.execute("ROLLBACK")
            raise

    # ---- 批次应用 ---------------------------------------------------------

    def apply_batch(
        self,
        batch: Any = None,
        suppliers: Any = None,
        claims: Any = None,
        *,
        include_changes: bool = True,
    ) -> dict[str, Any]:
        """应用一个采购批次；失败时不留下任何半批结果。"""
        data = _load_batch_dict(batch)
        supplier_ops = parse_suppliers(data, suppliers)
        claim_ops = parse_claims(data, claims)
        raw_input = json.dumps(data, ensure_ascii=False, sort_keys=True, default=str)

        with self.transaction() as conn:
            row = conn.execute(
                "SELECT value FROM cc_meta WHERE key='last_batch_number'"
            ).fetchone()
            batch_number = int(row["value"]) + 1
            previous_batch_number = batch_number - 1 if batch_number > 1 else None
            conn.execute(
                "UPDATE cc_meta SET value=? WHERE key='last_batch_number'",
                (str(batch_number),),
            )

            self._clear_work_tables(conn)
            self._validate_and_prepare_claims(conn, claim_ops, supplier_ops)
            self._read_selected_sources(conn, supplier_ops)
            quote_counts = self._build_quote_changes(conn, supplier_ops)
            self._collect_old_keys(conn, batch_number, claim_ops)
            self._apply_supplier_snapshots(conn, batch_number, supplier_ops)
            self._apply_claim_ops(conn, batch_number, claim_ops)
            new_key_count = self._collect_new_keys(conn, batch_number, claim_ops)
            change_counts = self._recompute_affected_groups(conn, batch_number)

            counts = {
                "suppliers_selected": len(supplier_ops),
                "claims_submitted": len(claim_ops),
                "new_keys": new_key_count,
                **quote_counts,
                **change_counts,
            }
            summary = {
                "batch_number": batch_number,
                "previous_batch_number": previous_batch_number,
                "counts": counts,
            }
            external_batch_id = _get_field(
                data, ("batch_id", "external_batch_id", "label", "batch_label")
            )
            if external_batch_id is not None:
                summary["batch_id"] = external_batch_id
            conn.execute(
                "INSERT INTO cc_batches(batch_number,created_at,input_json,summary_json) "
                "VALUES(?,?,?,?)",
                (batch_number, utc_now(), raw_input,
                 json.dumps(summary, ensure_ascii=False, sort_keys=True)),
            )
            self._clear_work_tables(conn)

        receipt: dict[str, Any] = dict(summary)
        # 常见批次号别名；若调用方提供了外部 batch_id，则保留在 batch_id 字段。
        receipt["batch"] = batch_number
        receipt["seq"] = batch_number
        receipt.setdefault("batch_id", batch_number)
        if include_changes:
            receipt["changes"] = self.get_changes(batch_number)["changes"]
        return receipt

    def _clear_work_tables(self, conn: sqlite3.Connection) -> None:
        for table in (
            "cc_batch_staging",
            "cc_batch_quote_changes",
            "cc_batch_claim_roots",
            "cc_batch_old_keys",
            "cc_batch_claim_old_keys",
            "cc_batch_new_keys",
        ):
            conn.execute(f"DELETE FROM {table}")

    def _supplier_known(
        self, conn: sqlite3.Connection, name: str, selections: Sequence[SupplierSelection]
    ) -> bool:
        if any(s.name == name and s.version_id is not None for s in selections):
            return True
        return conn.execute(
            "SELECT 1 FROM cc_supplier_versions WHERE supplier=?", (name,)
        ).fetchone() is not None

    def _validate_and_prepare_claims(
        self,
        conn: sqlite3.Connection,
        ops: list[ClaimOp],
        selections: Sequence[SupplierSelection],
    ) -> None:
        for op in ops:
            if op.a is not None and not self._supplier_known(conn, op.a.supplier, selections):
                raise InvalidBatch(
                    f"声明 {op.claim_id} 的供应商 {op.a.supplier!r} 尚未选版，"
                    "也未在本批选版"
                )
            if op.b is not None and not self._supplier_known(conn, op.b.supplier, selections):
                raise InvalidBatch(
                    f"声明 {op.claim_id} 的供应商 {op.b.supplier!r} 尚未选版，"
                    "也未在本批选版"
                )

            existing = conn.execute(
                "SELECT * FROM cc_claims WHERE claim_id=?", (op.claim_id,)
            ).fetchone()

            if op.op == "add":
                if existing is not None:
                    raise InvalidBatch(f"新增声明编号已存在: {op.claim_id}")
            elif op.op in ("replace", "withdraw"):
                if existing is None:
                    raise InvalidBatch(f"替换/撤回的声明编号不存在: {op.claim_id}")
                if existing["status"] != "active":
                    raise InvalidBatch(
                        f"声明 {op.claim_id} 已撤回，不能重复{ '撤回' if op.op == 'withdraw' else '替换'}"
                    )
                op.old_a = ProductRef(existing["supplier_a"], existing["product_a"])
                op.old_b = ProductRef(existing["supplier_b"], existing["product_b"])
            elif op.op == "upsert":
                if existing is None:
                    op.op = "add"
                elif existing["status"] == "active":
                    op.op = "replace"
                    op.old_a = ProductRef(existing["supplier_a"], existing["product_a"])
                    op.old_b = ProductRef(existing["supplier_b"], existing["product_b"])
                else:
                    raise InvalidBatch(
                        f"声明编号 {op.claim_id} 已撤回；新关系必须使用新编号"
                    )

            # 把所有声明端点（包括替换/撤回前的旧端点）记为图变化根。
            refs: list[ProductRef] = []
            if op.old_a is not None and op.old_b is not None:
                refs.extend((op.old_a, op.old_b))
            if op.a is not None and op.b is not None:
                refs.extend((op.a, op.b))
            for ref in refs:
                conn.execute(
                    "INSERT OR IGNORE INTO cc_batch_claim_roots"
                    "(batch_number,supplier,product_id) VALUES(?,?,?)",
                    (0, ref.supplier, ref.product_id),
                )

        # 供应商选择本身也需要能解析到有效目标库/版本。
        for sel in selections:
            existing = conn.execute(
                "SELECT * FROM cc_supplier_versions WHERE supplier=?", (sel.name,)
            ).fetchone()
            if sel.version_id is None and existing is None:
                raise InvalidBatch(f"供应商 {sel.name!r} 首次选版必须提供 version")
            if sel.db_path is None and existing is None:
                raise InvalidBatch(f"供应商 {sel.name!r} 首次选版必须提供目录库 db_path")

    def _effective_source(self, conn: sqlite3.Connection, sel: SupplierSelection) -> tuple[str, str]:
        existing = conn.execute(
            "SELECT * FROM cc_supplier_versions WHERE supplier=?", (sel.name,)
        ).fetchone()
        db_path = sel.db_path or (existing["db_path"] if existing else None)
        version_id = sel.version_id or (existing["version_id"] if existing else None)
        if not db_path or not version_id:
            raise InvalidBatch(f"供应商 {sel.name!r} 缺少目录库或已发布版本")
        if db_path != ":memory:":
            db_path = str(Path(db_path).expanduser().resolve())
        return db_path, version_id

    def _source_uri(self, db_path: str) -> str:
        if db_path == ":memory:":
            # 调用方通常不会在批次 JSON 中给 :memory:；保留显式错误比误读 cwd 好。
            raise InvalidBatch("无法以只读 URI 重新打开 :memory: 供应商库")
        path = Path(db_path).expanduser()
        if not path.exists():
            raise SourceVersionNotFound(f"供应商目录库不存在: {db_path}")
        return path.resolve().as_uri() + "?mode=ro"

    def _read_selected_sources(
        self, conn: sqlite3.Connection, selections: Sequence[SupplierSelection]
    ) -> None:
        """只打开并读取本批明确选版的供应商源库。"""
        for sel in selections:
            if sel.version_id is None:
                continue
            db_path, version_id = self._effective_source(conn, sel)
            uri = self._source_uri(db_path)
            try:
                source = sqlite3.connect(uri, uri=True, timeout=30)
            except sqlite3.OperationalError as exc:
                raise InvalidBatch(f"无法打开供应商库 {db_path}: {exc}") from exc
            try:
                source.row_factory = sqlite3.Row
                source.execute("PRAGMA query_only=ON")
                source.execute("BEGIN")
                try:
                    row = source.execute(
                        "SELECT version_id FROM published_versions WHERE version_id=?",
                        (version_id,),
                    ).fetchone()
                    if row is None:
                        raise SourceVersionNotFound(
                            f"供应商 {sel.name!r} 的库 {db_path} 找不到已发布观察版 "
                            f"{version_id!r}"
                        )
                    try:
                        cursor = source.execute(
                            "SELECT product_id,url,price,product_revision "
                            "FROM published_products WHERE version_id=? "
                            "ORDER BY product_id",
                            (version_id,),
                        )
                    except sqlite3.OperationalError as exc:
                        raise InvalidBatch(
                            f"供应商库 {db_path} 缺少 catpipe published_products 表或结构不兼容"
                        ) from exc

                    while True:
                        rows = cursor.fetchmany(1000)
                        if not rows:
                            break
                        conn.executemany(
                            "INSERT INTO cc_batch_staging("
                            "supplier,product_id,url,price,product_revision) "
                            "VALUES(?,?,?,?,?)",
                            [
                                (sel.name, r["product_id"], r["url"], r["price"],
                                 r["product_revision"])
                                for r in rows
                            ],
                        )
                    source.execute("COMMIT")
                except BaseException:
                    source.execute("ROLLBACK")
                    raise
            except sqlite3.Error as exc:
                raise InvalidBatch(f"读取供应商库 {db_path} 失败: {exc}") from exc
            finally:
                source.close()

    def _build_quote_changes(
        self, conn: sqlite3.Connection, selections: Sequence[SupplierSelection]
    ) -> dict[str, int]:
        selected_names = [s.name for s in selections if s.version_id is not None]
        if not selected_names:
            return {"quotes_inserted": 0, "quotes_changed": 0, "quotes_deleted": 0}

        conn.execute(
            """
            INSERT INTO cc_batch_quote_changes(supplier,product_id,change_kind)
            SELECT n.supplier,n.product_id,
                   CASE WHEN o.product_id IS NULL THEN 'inserted'
                        ELSE 'changed'
                   END
            FROM cc_batch_staging n
            LEFT JOIN cc_supplier_quotes o
              ON o.supplier=n.supplier AND o.product_id=n.product_id
            WHERE o.product_id IS NULL
               OR o.url <> n.url
               OR o.price <> n.price
               OR o.product_revision <> n.product_revision
            """
        )

        for name in selected_names:
            conn.execute(
                """
                INSERT INTO cc_batch_quote_changes(supplier,product_id,change_kind)
                SELECT o.supplier,o.product_id,'deleted'
                FROM cc_supplier_quotes o
                WHERE o.supplier=?
                  AND NOT EXISTS (
                      SELECT 1 FROM cc_batch_staging n
                      WHERE n.supplier=o.supplier AND n.product_id=o.product_id
                  )
                """,
                (name,),
            )

        counts = {
            "quotes_inserted": 0,
            "quotes_changed": 0,
            "quotes_deleted": 0,
        }
        for row in conn.execute(
            "SELECT change_kind,COUNT(*) c FROM cc_batch_quote_changes GROUP BY change_kind"
        ):
            key = {
                "inserted": "quotes_inserted",
                "changed": "quotes_changed",
                "deleted": "quotes_deleted",
            }[row["change_kind"]]
            counts[key] = row["c"]
        return counts

    def _collect_old_keys(
        self, conn: sqlite3.Connection, batch_number: int, ops: Sequence[ClaimOp]
    ) -> None:
        # 报价变化影响到的现有组。
        conn.execute(
            """
            INSERT OR IGNORE INTO cc_batch_old_keys(
                batch_number,group_supplier,group_product_id)
            SELECT ?,m.group_supplier,m.group_product_id
            FROM cc_batch_quote_changes q
            JOIN cc_group_members m
              ON m.supplier=q.supplier AND m.product_id=q.product_id
            """,
            (batch_number,),
        )

        # 声明端点影响到的现有组；同时单独标记，因为同批图编辑后旧键可能合并消失。
        conn.execute(
            """
            INSERT OR IGNORE INTO cc_batch_old_keys(
                batch_number,group_supplier,group_product_id)
            SELECT ?,m.group_supplier,m.group_product_id
            FROM cc_batch_claim_roots r
            JOIN cc_group_members m
              ON m.supplier=r.supplier AND m.product_id=r.product_id
            """,
            (batch_number,),
        )
        conn.execute(
            """
            INSERT OR IGNORE INTO cc_batch_claim_old_keys(
                batch_number,group_supplier,group_product_id)
            SELECT ?,m.group_supplier,m.group_product_id
            FROM cc_batch_claim_roots r
            JOIN cc_group_members m
              ON m.supplier=r.supplier AND m.product_id=r.product_id
            """,
            (batch_number,),
        )
        conn.execute(
            "UPDATE cc_batch_claim_roots SET batch_number=? WHERE batch_number=0",
            (batch_number,),
        )

    def _apply_supplier_snapshots(
        self,
        conn: sqlite3.Connection,
        batch_number: int,
        selections: Sequence[SupplierSelection],
    ) -> None:
        for sel in selections:
            if sel.version_id is None:
                continue
            db_path, version_id = self._effective_source(conn, sel)
            # _effective_source may fall back to stored path; persist effective path.
            conn.execute(
                """
                INSERT INTO cc_supplier_versions(
                    supplier,db_path,version_id,selected_batch)
                VALUES(?,?,?,?)
                ON CONFLICT(supplier) DO UPDATE SET
                    db_path=excluded.db_path,
                    version_id=excluded.version_id,
                    selected_batch=excluded.selected_batch
                """,
                (sel.name, db_path, version_id, batch_number),
            )
            conn.execute("DELETE FROM cc_supplier_quotes WHERE supplier=?", (sel.name,))

        conn.execute(
            """
            INSERT INTO cc_supplier_quotes(
                supplier,product_id,url,price,product_revision,batch_number)
            SELECT supplier,product_id,url,price,product_revision,?
            FROM cc_batch_staging
            """,
            (batch_number,),
        )

    def _apply_claim_ops(
        self, conn: sqlite3.Connection, batch_number: int, ops: Sequence[ClaimOp]
    ) -> None:
        for op in ops:
            if op.op == "withdraw":
                continue
            assert op.a is not None and op.b is not None and op.op in ("add", "replace")
            conn.execute(
                """
                INSERT INTO cc_claims(
                    claim_id,supplier_a,product_a,supplier_b,product_b,status,
                    created_batch,updated_batch,withdrawn_batch)
                VALUES(?,?,?,?,?, 'active', ?, ?, NULL)
                ON CONFLICT(claim_id) DO UPDATE SET
                    supplier_a=excluded.supplier_a,
                    product_a=excluded.product_a,
                    supplier_b=excluded.supplier_b,
                    product_b=excluded.product_b,
                    status='active',
                    updated_batch=excluded.updated_batch,
                    withdrawn_batch=NULL
                """,
                (op.claim_id, op.a.supplier, op.a.product_id,
                 op.b.supplier, op.b.product_id, batch_number, batch_number),
            )
            conn.execute("DELETE FROM cc_claim_endpoints WHERE claim_id=?", (op.claim_id,))
            self._insert_endpoints(conn, op.claim_id, op.a, op.b)

        # 撤回操作也可能出现在 op 列表中；上面的循环只处理 add/replace。
        for op in ops:
            if op.op != "withdraw":
                continue
            conn.execute(
                """
                UPDATE cc_claims
                   SET status='withdrawn', updated_batch=?, withdrawn_batch=?
                 WHERE claim_id=? AND status='active'
                """,
                (batch_number, batch_number, op.claim_id),
            )
            conn.execute("DELETE FROM cc_claim_endpoints WHERE claim_id=?", (op.claim_id,))

    @staticmethod
    def _insert_endpoints(
        conn: sqlite3.Connection, claim_id: str, a: ProductRef, b: ProductRef
    ) -> None:
        conn.execute(
            "INSERT INTO cc_claim_endpoints VALUES(?,?,?,?,?)",
            (claim_id, a.supplier, a.product_id, b.supplier, b.product_id),
        )
        conn.execute(
            "INSERT INTO cc_claim_endpoints VALUES(?,?,?,?,?)",
            (claim_id, b.supplier, b.product_id, a.supplier, a.product_id),
        )

    def _component_key(
        self, conn: sqlite3.Connection, supplier: str, product_id: int
    ) -> ProductRef:
        row = conn.execute(
            """
            WITH RECURSIVE walk(supplier, product_id) AS (
                SELECT ?, ?
                UNION
                SELECT e.other_supplier, e.other_product_id
                  FROM cc_claim_endpoints e
                  JOIN walk w
                    ON e.supplier=w.supplier AND e.product_id=w.product_id
            )
            SELECT supplier, product_id
              FROM walk
             ORDER BY supplier ASC, product_id ASC
             LIMIT 1
            """,
            (supplier, product_id),
        ).fetchone()
        return ProductRef(row["supplier"], row["product_id"])

    def _collect_new_keys(
        self, conn: sqlite3.Connection, batch_number: int, ops: Sequence[ClaimOp]
    ) -> int:
        # 1) 声明根：图已改完，递归求新连通组的最小身份。声明数量通常很少。
        roots = conn.execute(
            "SELECT DISTINCT supplier,product_id FROM cc_batch_claim_roots "
            "WHERE batch_number=? ORDER BY supplier,product_id",
            (batch_number,),
        ).fetchall()
        for root in roots:
            key = self._component_key(conn, root["supplier"], root["product_id"])
            conn.execute(
                "INSERT OR IGNORE INTO cc_batch_new_keys VALUES(?,?,?)",
                (batch_number, key.supplier, key.product_id),
            )

        # 2) 未被声明编辑触及的旧组：图不变，键也不变。
        conn.execute(
            """
            INSERT OR IGNORE INTO cc_batch_new_keys(
                batch_number,group_supplier,group_product_id)
            SELECT ?,k.group_supplier,k.group_product_id
              FROM cc_batch_old_keys k
             WHERE k.batch_number=?
               AND NOT EXISTS (
                   SELECT 1 FROM cc_batch_claim_old_keys c
                    WHERE c.batch_number=?
                      AND c.group_supplier=k.group_supplier
                      AND c.group_product_id=k.group_product_id
               )
            """,
            (batch_number, batch_number, batch_number),
        )

        # 3) 本批新引入、且没有旧成员关系、也不在有效声明边上的报价：新单商品组。
        conn.execute(
            """
            INSERT OR IGNORE INTO cc_batch_new_keys(
                batch_number,group_supplier,group_product_id)
            SELECT ?,q.supplier,q.product_id
              FROM cc_batch_quote_changes q
             WHERE NOT EXISTS (
                       SELECT 1 FROM cc_group_members m
                        WHERE m.supplier=q.supplier AND m.product_id=q.product_id
                   )
               AND NOT EXISTS (
                       SELECT 1 FROM cc_claim_endpoints e
                        WHERE e.supplier=q.supplier AND e.product_id=q.product_id
                   )
            """,
            (batch_number,),
        )

        return conn.execute(
            "SELECT COUNT(*) c FROM cc_batch_new_keys WHERE batch_number=?",
            (batch_number,),
        ).fetchone()["c"]

    def _recompute_affected_groups(
        self, conn: sqlite3.Connection, batch_number: int
    ) -> dict[str, int]:
        # 删除所有受影响旧组的成员关系；下面按新组件重建。
        conn.execute(
            """
            DELETE FROM cc_group_members
             WHERE EXISTS (
                SELECT 1 FROM cc_batch_old_keys k
                 WHERE k.batch_number=?
                   AND k.group_supplier=cc_group_members.group_supplier
                   AND k.group_product_id=cc_group_members.group_product_id
             )
            """,
            (batch_number,),
        )

        put_added = put_changed = deleted = 0
        key_cursor = conn.execute(
            "SELECT group_supplier,group_product_id FROM cc_batch_new_keys "
            "WHERE batch_number=? ORDER BY group_supplier,group_product_id",
            (batch_number,),
        )
        while True:
            key_rows = key_cursor.fetchmany(1000)
            if not key_rows:
                break
            for key_row in key_rows:
                key = ProductRef(key_row["group_supplier"], key_row["group_product_id"])
                self._recompute_one_group(conn, batch_number, key)

        # 旧键没有出现在新键集合中：对下游发删除（仅当它曾是目录键）。
        old_gone = conn.execute(
            """
            SELECT o.group_supplier,o.group_product_id,c.record_json
              FROM cc_batch_old_keys o
              JOIN cc_catalog c
                ON c.group_supplier=o.group_supplier
               AND c.group_product_id=o.group_product_id
             WHERE o.batch_number=?
               AND NOT EXISTS (
                   SELECT 1 FROM cc_batch_new_keys n
                    WHERE n.batch_number=o.batch_number
                      AND n.group_supplier=o.group_supplier
                      AND n.group_product_id=o.group_product_id
               )
            """,
            (batch_number,),
        ).fetchall()
        for row in old_gone:
            conn.execute(
                "INSERT INTO cc_batch_changes VALUES(?,?,?,?,?,?)",
                (batch_number, "delete", "removed",
                 row["group_supplier"], row["group_product_id"], None),
            )
            conn.execute(
                "DELETE FROM cc_catalog WHERE group_supplier=? AND group_product_id=?",
                (row["group_supplier"], row["group_product_id"]),
            )
            deleted += 1

        row = conn.execute(
            "SELECT change_kind,COUNT(*) c FROM cc_batch_changes "
            "WHERE batch_number=? GROUP BY change_kind",
            (batch_number,),
        ).fetchall()
        counts = {
            "changes_added": 0,
            "changes_changed": 0,
            "changes_removed": 0,
            "changes_deleted": 0,
        }
        mapping = {
            "added": "changes_added",
            "changed": "changes_changed",
            "removed": "changes_removed",
        }
        for r in row:
            counts[mapping[r["change_kind"]]] = r["c"]
        counts["changes_deleted"] = counts["changes_removed"]
        return counts

    def _recompute_one_group(
        self, conn: sqlite3.Connection, batch_number: int, key: ProductRef
    ) -> None:
        # 用递归 CTE 重建该组全部成员。UNION（非 UNION ALL）去重并终止环。
        conn.execute(
            """
            WITH RECURSIVE walk(supplier, product_id) AS (
                SELECT ?, ?
                UNION
                SELECT e.other_supplier, e.other_product_id
                  FROM cc_claim_endpoints e
                  JOIN walk w
                    ON e.supplier=w.supplier AND e.product_id=w.product_id
            )
            INSERT INTO cc_group_members(
                group_supplier,group_product_id,supplier,product_id,batch_number)
            SELECT ?,?,supplier,product_id,?
              FROM walk
            """,
            (key.supplier, key.product_id,
             key.supplier, key.product_id, batch_number),
        )

        members = [
            ProductRef(r["supplier"], r["product_id"]).to_dict()
            for r in conn.execute(
                "SELECT supplier,product_id FROM cc_group_members "
                "WHERE group_supplier=? AND group_product_id=? "
                "ORDER BY supplier ASC,product_id ASC",
                key.key,
            )
        ]

        winner = conn.execute(
            """
            SELECT q.supplier,q.product_id,q.url,q.price,q.product_revision
              FROM cc_group_members g
              JOIN cc_supplier_quotes q
                ON q.supplier=g.supplier AND q.product_id=g.product_id
             WHERE g.group_supplier=? AND g.group_product_id=?
             ORDER BY q.price ASC, q.supplier ASC, q.product_id ASC
             LIMIT 1
            """,
            key.key,
        ).fetchone()

        old = conn.execute(
            "SELECT record_json FROM cc_catalog "
            "WHERE group_supplier=? AND group_product_id=?",
            key.key,
        ).fetchone()

        if winner is None:
            conn.execute(
                "DELETE FROM cc_catalog WHERE group_supplier=? AND group_product_id=?",
                key.key,
            )
            if old is not None:
                conn.execute(
                    "INSERT INTO cc_batch_changes VALUES(?,?,?,?,?,?)",
                    (batch_number, "delete", "removed",
                     key.supplier, key.product_id, None),
                )
            return

        winning_ref = ProductRef(winner["supplier"], winner["product_id"])
        record = {
            "group_key": key.to_dict(),
            "group_supplier": key.supplier,
            "group_product_id": key.product_id,
            "members": members,
            "winning_identity": winning_ref.to_dict(),
            "supplier": winner["supplier"],
            "product_id": winner["product_id"],
            "url": winner["url"],
            "price": winner["price"],
            "product_revision": winner["product_revision"],
        }
        record_json = json.dumps(
            record, ensure_ascii=False, sort_keys=True, separators=(",", ":")
        )

        old = conn.execute(
            "SELECT record_json FROM cc_catalog "
            "WHERE group_supplier=? AND group_product_id=?",
            key.key,
        ).fetchone()

        if old is None:
            kind = "added"
        elif old["record_json"] != record_json:
            kind = "changed"
        else:
            kind = "unchanged"

        if kind != "unchanged":
            conn.execute(
                """
                INSERT INTO cc_catalog(
                    group_supplier,group_product_id,winner_supplier,winner_product_id,
                    url,price,product_revision,record_json,batch_number)
                VALUES(?,?,?,?,?,?,?,?,?)
                ON CONFLICT(group_supplier,group_product_id) DO UPDATE SET
                    winner_supplier=excluded.winner_supplier,
                    winner_product_id=excluded.winner_product_id,
                    url=excluded.url,
                    price=excluded.price,
                    product_revision=excluded.product_revision,
                    record_json=excluded.record_json,
                    batch_number=excluded.batch_number
                """,
                (key.supplier, key.product_id, winner["supplier"], winner["product_id"],
                 winner["url"], winner["price"], winner["product_revision"],
                 record_json, batch_number),
            )

        if kind in ("added", "changed"):
            # 对外完整记录不应含内部比较用的批次字段差异？保留 batch_number 是合理的。
            conn.execute(
                "INSERT OR REPLACE INTO cc_batch_changes VALUES(?,?,?,?,?,?)",
                (batch_number, "put", kind, key.supplier, key.product_id, record_json),
            )

    # ---- 读取 -------------------------------------------------------------

    def latest_batch_number(self) -> int | None:
        row = self.conn.execute(
            "SELECT value FROM cc_meta WHERE key='last_batch_number'"
        ).fetchone()
        n = int(row["value"])
        return n or None

    def list_batches(self) -> list[dict[str, Any]]:
        rows = self.conn.execute("SELECT * FROM cc_batches ORDER BY batch_number").fetchall()
        out = []
        for r in rows:
            item = dict(r)
            item["summary"] = json.loads(item.pop("summary_json"))
            if item.get("input_json"):
                try:
                    item["input"] = json.loads(item.pop("input_json"))
                except json.JSONDecodeError:
                    pass
            out.append(item)
        return out

    def get_batch(self, batch_number: int | None = None) -> dict[str, Any]:
        n = self._resolve_batch_number(batch_number)
        row = self.conn.execute(
            "SELECT * FROM cc_batches WHERE batch_number=?", (n,)
        ).fetchone()
        if row is None:
            raise CrossCatalogError(f"批次 {n} 不存在")
        item = dict(row)
        item["summary"] = json.loads(item.pop("summary_json"))
        item["changes"] = self.get_changes(n)["changes"]
        return item

    def _resolve_batch_number(self, batch_number: int | None) -> int:
        if batch_number is None:
            n = self.latest_batch_number()
            if n is None:
                raise CrossCatalogError("还没有成功批次")
            return n
        n = int(batch_number)
        exists = self.conn.execute(
            "SELECT 1 FROM cc_batches WHERE batch_number=?", (n,)
        ).fetchone()
        if exists is None:
            raise CrossCatalogError(f"批次 {n} 不存在")
        return n

    @staticmethod
    def _present_record(record: dict[str, Any]) -> dict[str, Any]:
        """补充常见命名字段别名；不改变已持久化的规范 JSON。"""
        key = record.get("group_key") or {
            "supplier": record.get("group_supplier"),
            "product_id": record.get("group_product_id"),
        }
        members = record.get("members", [])
        winner = record.get("winning_identity") or {
            "supplier": record.get("supplier"),
            "product_id": record.get("product_id"),
        }
        best_offer = {
            "identity": winner,
            "supplier": record.get("supplier"),
            "product_id": record.get("product_id"),
            "url": record.get("url"),
            "price": record.get("price"),
            "product_revision": record.get("product_revision"),
        }
        presented = dict(record)
        presented.update({
            "key": key,
            "key_tuple": [key["supplier"], key["product_id"]],
            "identities": members,
            "group": members,
            "member_count": len(members),
            "winner": winner,
            "winning_product": winner,
            "best_offer": best_offer,
            "best_quote": best_offer,
            "offer": best_offer,
            "lowest_price": record.get("price"),
        })
        return presented

    def iter_catalog(self) -> Iterator[dict[str, Any]]:
        """流式读取当前目录；不会一次把整个目录载入内存。"""
        cursor = self.conn.execute(
            "SELECT record_json FROM cc_catalog "
            "ORDER BY group_supplier ASC,group_product_id ASC"
        )
        while True:
            rows = cursor.fetchmany(1000)
            if not rows:
                break
            for row in rows:
                yield self._present_record(json.loads(row["record_json"]))

    def catalog(self) -> list[dict[str, Any]]:
        return list(self.iter_catalog())

    def snapshot(self, batch_number: int | None = None) -> dict[str, Any]:
        """返回当前或重放指定批次的快照。

        不传批次号时直接读当前 SQLite 快照。传历史批次号时，用该批次之前的
        全部 put/delete 变更在临时表中流式重放，因此不需要把所有批次或目录
        一次性放入内存。
        """
        if batch_number is None:
            # 在同一个 SQLite 读事务中取批次号和快照，避免并发提交把二者错开。
            self.conn.execute("BEGIN")
            try:
                n = self.latest_batch_number()
                if n is None:
                    self.conn.execute("COMMIT")
                    return {"batch_number": None, "records": [],
                            "catalog": [], "count": 0}
                records = self.catalog()
                self.conn.execute("COMMIT")
            except BaseException:
                self.conn.execute("ROLLBACK")
                raise
            return {
                "batch_number": n,
                "records": records,
                "catalog": records,
                "count": len(records),
            }

        n = self._resolve_batch_number(batch_number)
        # 使用当前连接的 TEMP 表；连接已设置 temp_store=FILE，因此重放一个
        # 大于内存的历史快照时不会把整张快照放进 Python 或内存 SQLite。
        self.conn.execute("DROP TABLE IF EXISTS temp.cc_replay_catalog")
        self.conn.execute(
            "CREATE TEMP TABLE cc_replay_catalog("
            "group_supplier TEXT,group_product_id INTEGER,record_json TEXT,"
            "PRIMARY KEY(group_supplier,group_product_id)) WITHOUT ROWID"
        )
        try:
            cursor = self.conn.execute(
                "SELECT op,group_supplier,group_product_id,record_json "
                "FROM cc_batch_changes WHERE batch_number<=? "
                "ORDER BY batch_number, group_supplier,group_product_id",
                (n,),
            )
            while True:
                rows = cursor.fetchmany(1000)
                if not rows:
                    break
                for row in rows:
                    if row["op"] == "delete":
                        self.conn.execute(
                            "DELETE FROM cc_replay_catalog WHERE group_supplier=? "
                            "AND group_product_id=?",
                            (row["group_supplier"], row["group_product_id"]),
                        )
                    else:
                        self.conn.execute(
                            "INSERT OR REPLACE INTO cc_replay_catalog VALUES(?,?,?)",
                            (row["group_supplier"], row["group_product_id"],
                             row["record_json"]),
                        )
            records = [
                self._present_record(json.loads(r[0]))
                for r in self.conn.execute(
                    "SELECT record_json FROM cc_replay_catalog "
                    "ORDER BY group_supplier,group_product_id"
                )
            ]
            return {
                "batch_number": n,
                "records": records,
                "catalog": records,
                "count": len(records),
                "replayed": True,
            }
        finally:
            self.conn.execute("DROP TABLE IF EXISTS temp.cc_replay_catalog")

    def iter_changes(self, batch_number: int | None = None) -> Iterator[dict[str, Any]]:
        n = self._resolve_batch_number(batch_number)
        cursor = self.conn.execute(
            "SELECT * FROM cc_batch_changes WHERE batch_number=? "
            "ORDER BY group_supplier ASC,group_product_id ASC",
            (n,),
        )
        while True:
            rows = cursor.fetchmany(1000)
            if not rows:
                break
            for row in rows:
                key = {
                    "supplier": row["group_supplier"],
                    "product_id": row["group_product_id"],
                }
                is_put = row["op"] == "put"
                event: dict[str, Any] = {
                    "batch_number": n,
                    "batch": n,
                    # op/type 是下游同步动作；change_type 是相对上一批的语义差异。
                    "op": row["op"],
                    "type": row["op"],
                    "action": "upsert" if is_put else "delete",
                    "sync_op": "upsert" if is_put else "delete",
                    "change_type": row["change_kind"],
                    "change_kind": row["change_kind"],
                    "kind": row["op"] if not is_put else "upsert",
                    "key": key,
                    "group_key": key,
                    "key_tuple": [key["supplier"], key["product_id"]],
                }
                record = (
                    self._present_record(json.loads(row["record_json"]))
                    if is_put else None
                )
                event["record"] = record
                if is_put and record is not None:
                    # 既支持 change["record"]，也支持直接 change["price"] 等字段。
                    event.update(record)
                    event["record"] = record
                    event["new_record"] = record
                    event["full_record"] = record
                    event["value"] = record
                    event["after"] = record
                else:
                    event["deleted_key"] = key
                    event["group_supplier"] = key["supplier"]
                    event["group_product_id"] = key["product_id"]
                yield event

    def get_changes(self, batch_number: int | None = None) -> dict[str, Any]:
        n = self._resolve_batch_number(batch_number)
        events = list(self.iter_changes(n))
        counts = {"added": 0, "changed": 0, "removed": 0, "deleted": 0}
        for e in events:
            if e["change_type"] in ("added", "changed", "removed"):
                counts[e["change_type"]] += 1
        counts["deleted"] = counts["removed"]
        batch_row = self.conn.execute(
            "SELECT created_at FROM cc_batches WHERE batch_number=?", (n,)
        ).fetchone()
        return {
            "batch_number": n,
            "previous_batch_number": n - 1 if n > 1 else None,
            "created_at": batch_row["created_at"] if batch_row else None,
            "changes": events,
            "counts": counts,
            "upserts": [e["record"] for e in events if e["op"] == "put"],
            "puts": [e["record"] for e in events if e["op"] == "put"],
            "deletes": [e["key"] for e in events if e["op"] == "delete"],
            "removals": [e["key"] for e in events if e["op"] == "delete"],
        }

    def changes(self, batch_number: int | None = None) -> list[dict[str, Any]]:
        """直接返回某批次的变更事件列表。"""
        return self.get_changes(batch_number)["changes"]

    def batch_changes(self, batch_number: int | None = None) -> dict[str, Any]:
        return self.get_changes(batch_number)

    def latest_batch(self) -> int | None:
        return self.latest_batch_number()

    # ---- 状态/审计 --------------------------------------------------------

    def suppliers(self) -> list[dict[str, Any]]:
        return [dict(r) for r in self.conn.execute(
            "SELECT supplier,db_path,version_id,selected_batch "
            "FROM cc_supplier_versions ORDER BY supplier"
        )]

    def claims(self, *, include_withdrawn: bool = True) -> list[dict[str, Any]]:
        sql = "SELECT * FROM cc_claims"
        if not include_withdrawn:
            sql += " WHERE status='active'"
        sql += " ORDER BY claim_id"
        return [dict(r) for r in self.conn.execute(sql)]


# 常见命名别名，方便不同调用习惯。
ProcurementCatalog = CrossCatalog
CrossSupplierCatalog = CrossCatalog
CrossVendorCatalog = CrossCatalog
CatalogBatchStore = CrossCatalog
ProcurementService = CrossCatalog

# 方法级常见别名。
CrossCatalog.commit_batch = CrossCatalog.apply_batch  # type: ignore[attr-defined]
CrossCatalog.process_batch = CrossCatalog.apply_batch  # type: ignore[attr-defined]
CrossCatalog.apply = CrossCatalog.apply_batch  # type: ignore[attr-defined]
CrossCatalog.get_batch_changes = CrossCatalog.get_changes  # type: ignore[attr-defined]
CrossCatalog.batch_changes = CrossCatalog.get_changes  # type: ignore[attr-defined]
CrossCatalog.get_snapshot = CrossCatalog.snapshot  # type: ignore[attr-defined]
CrossCatalog.get_catalog = CrossCatalog.catalog  # type: ignore[attr-defined]
CrossCatalog.list_records = CrossCatalog.catalog  # type: ignore[attr-defined]
CrossCatalog.latest_batch = CrossCatalog.latest_batch_number  # type: ignore[attr-defined]


def commit_batch(*args: Any, **kwargs: Any) -> dict[str, Any]:
    return apply_batch(*args, **kwargs)


def process_batch(*args: Any, **kwargs: Any) -> dict[str, Any]:
    return apply_batch(*args, **kwargs)


def apply_batch(
    catalog_db: str | os.PathLike[str] | dict[str, Any] | None = "cross_catalog.db",
    batch: Any = None,
    *,
    suppliers: Any = None,
    claims: Any = None,
    include_changes: bool = True,
) -> dict[str, Any]:
    """一次性 API：打开跨供应商库、应用批次并关闭。

    支持两种常见位置参数顺序：``apply_batch(db_path, batch)`` 和
    ``apply_batch(batch, db_path)``。
    """
    # 兼容把 batch 作为第一个参数、db 作为第二个参数的调用习惯。
    if isinstance(catalog_db, dict) and (
        batch is None or isinstance(batch, (str, os.PathLike))
    ):
        if batch is None:
            batch = catalog_db
            catalog_db = "cross_catalog.db"
        else:
            batch, catalog_db = catalog_db, batch
    elif catalog_db is None:
        catalog_db = "cross_catalog.db"

    catalog = CrossCatalog(os.fspath(catalog_db))
    try:
        return catalog.apply_batch(
            batch,
            suppliers=suppliers,
            claims=claims,
            include_changes=include_changes,
        )
    finally:
        catalog.close()


# --------------------------------------------------------------------------- #
# 命令行入口
# --------------------------------------------------------------------------- #

def _emit_json(value: Any) -> None:
    print(json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True))


def _read_batch_arg(batch: str | None) -> Any:
    if not batch or batch == "-":
        import sys
        raw = sys.stdin.read()
        return json.loads(raw)
    return batch


def cli_apply(args: Any) -> int:
    batch = _read_batch_arg(args.batch)
    try:
        receipt = apply_batch(args.db, batch, include_changes=not args.summary_only)
    except InvalidBatch as exc:
        print(f"批次失败，未留下半批结果: {exc}", file=__import__("sys").stderr)
        return 2
    if args.summary_only:
        _emit_json({k: v for k, v in receipt.items() if k != "changes"})
    else:
        _emit_json(receipt)
    return 0


def cli_catalog(args: Any) -> int:
    catalog = CrossCatalog(args.db)
    try:
        if args.format == "jsonl":
            for record in catalog.iter_catalog():
                print(json.dumps(record, ensure_ascii=False, sort_keys=True))
        else:
            snapshot = catalog.snapshot(args.batch)
            _emit_json(snapshot)
    finally:
        catalog.close()
    return 0


def cli_changes(args: Any) -> int:
    catalog = CrossCatalog(args.db)
    try:
        if getattr(args, "out", None):
            out_path = Path(args.out)
            out_path.parent.mkdir(parents=True, exist_ok=True)
            if args.format == "jsonl" or out_path.suffix == ".jsonl":
                count = 0
                with out_path.open("w", encoding="utf-8") as fh:
                    for event in catalog.iter_changes(args.batch):
                        fh.write(json.dumps(event, ensure_ascii=False, sort_keys=True) + "\n")
                        count += 1
                _emit_json({"out": str(out_path), "events": count, "format": "jsonl"})
            else:
                payload = catalog.get_changes(args.batch)
                out_path.write_text(
                    json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True),
                    encoding="utf-8",
                )
                _emit_json({"out": str(out_path), "events": len(payload["changes"]),
                            "format": "json"})
            return 0

        if args.format == "jsonl":
            for event in catalog.iter_changes(args.batch):
                print(json.dumps(event, ensure_ascii=False, sort_keys=True))
        else:
            _emit_json(catalog.get_changes(args.batch))
    finally:
        catalog.close()
    return 0


def cli_batches(args: Any) -> int:
    catalog = CrossCatalog(args.db)
    try:
        _emit_json({"batches": catalog.list_batches()})
    finally:
        catalog.close()
    return 0


def cli_suppliers(args: Any) -> int:
    catalog = CrossCatalog(args.db)
    try:
        _emit_json({"suppliers": catalog.suppliers(), "claims": catalog.claims()})
    finally:
        catalog.close()
    return 0


def cli_demo(args: Any) -> int:
    if __package__ in (None, ""):
        from catpipe.cross_demo import run_demo
    else:
        from .cross_demo import run_demo
    run_demo(args.workdir)
    return 0


def cli_serve(args: Any) -> int:
    if __package__ in (None, ""):
        from catpipe.cross_reader import serve_forever
    else:
        from .cross_reader import serve_forever
    serve_forever(args.db, args.host, args.port, verbose=not args.quiet)
    return 0


def build_parser() -> Any:
    import argparse

    parser = argparse.ArgumentParser(
        prog="catpipe-procurement",
        description="采购端跨供应商同款目录与批次变更",
    )
    sub = parser.add_subparsers(dest="cross_cmd", required=True)

    a = sub.add_parser("apply", help="应用一个采购批次 JSON 文件（- 表示 stdin）")
    a.add_argument("--db", required=True, help="跨供应商目录 SQLite 库")
    a.add_argument("--batch", required=True, help="批次 JSON 文件，或 - 从 stdin 读取")
    a.add_argument("--summary-only", action="store_true", help="只输出批次回执，不内嵌全部变更")
    a.set_defaults(func=cli_apply)

    c = sub.add_parser("catalog", help="读取当前或指定批次重放快照")
    c.add_argument("--db", required=True)
    c.add_argument("--batch", type=int)
    c.add_argument("--format", choices=("json", "jsonl"), default="json")
    c.set_defaults(func=cli_catalog)

    ch = sub.add_parser("changes", help="读取某成功批次相对上一批的变更")
    ch.add_argument("--db", required=True)
    ch.add_argument("--batch", type=int, help="默认最新成功批次")
    ch.add_argument("--format", choices=("json", "jsonl"), default="json")
    ch.add_argument("--out", help="把变更流式写入文件；.jsonl 时逐行 JSON")
    ch.set_defaults(func=cli_changes)

    b = sub.add_parser("batches", help="列出成功批次及输入/摘要")
    b.add_argument("--db", required=True)
    b.set_defaults(func=cli_batches)

    s = sub.add_parser("state", help="列出已选版供应商和声明")
    s.add_argument("--db", required=True)
    s.set_defaults(func=cli_suppliers)

    d = sub.add_parser("demo", help="生成供应商样例库并运行可复现跨目录示例")
    d.add_argument("--workdir", help="示例输出目录（默认临时目录）")
    d.set_defaults(func=cli_demo)

    srv = sub.add_parser("serve", help="启动下游只读 HTTP 服务")
    srv.add_argument("--db", required=True)
    srv.add_argument("--host", default="127.0.0.1")
    srv.add_argument("--port", type=int, default=8091)
    srv.add_argument("--quiet", action="store_true")
    srv.set_defaults(func=cli_serve)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return int(args.func(args) or 0)


if __name__ == "__main__":
    raise SystemExit(main())

