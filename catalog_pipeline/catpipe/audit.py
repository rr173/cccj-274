"""面向审计的历史来源包：锁定某个成功采购批次时点的分组、报价与依据。

本模块只读取：

* 现有采购端 SQLite 库（``catpipe.procurement`` 写的 ``cc_*`` 表，以
  ``mode=ro`` 只读打开，临时重放表放在 TEMP 中，绝不写主库）；
* 各供应商 catpipe 库中**不可变**的 ``published_versions`` /
  ``published_products``（同样只读，不接触采集协议与候选区）。

导出物是一个目录包（或一组同名旁车文件）：

* ``evidence.jsonl``：每个该时点目录组一行的自包含审计证据；
* ``manifest.json``：批次事实、文件 SHA-256/字节数/行数与全量校验结论；
* ``DONE``：完成标记，只在证据全量复核通过且清单落盘后最后原子写入。

关键语义
========
1. **锁定批次时点**：供应商选版与声明状态通过重放
   ``cc_batches.input_json``（只增不改的成功批次事实）重建到第 N 批，
   并用 ``cc_supplier_versions`` / ``cc_claims`` 的批次生命周期列兜底/交叉
   校验。之后撤回、替换声明或供应商发布新版本都不会改变导出内容。
2. **不降级**：目标时点的发布版找不到、最低报价成员的完整来源行缺失或与
   批次记录不一致、或有效声明不能完整连通整组时，直接抛错，绝不产出
   ``DONE``。普通成员允许“明确缺席”：发布版存在、商品在版本域内、完整覆盖
   证明可查且 ``published_products`` 无行。
3. **超内存安全**：批次快照用 TEMP 表流式重放，组记录按 1000 行块读取，
   证据逐行写入并增量计算 SHA-256；Python 进程只持有一个组的内容。
4. **崩溃安全/可重启读取**：先写隐藏暂存目录并对成品文件做全量复核，再写
   清单，最后写 ``DONE``，目录整体原子改名。没有 ``DONE`` 的结果一律不是
   成品；已完成的包之后可用 :func:`verify_package` /
   :func:`iter_package_records` 在全新进程中离线读取与复核。

下游只凭导出物（不需要任何数据库）即可重算：成员身份、组键、声明图连通
性、最低报价选择以及与内嵌完整来源行的一致性。
"""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import sqlite3
import sys
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterator, Sequence

if __package__ in (None, ""):
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from catpipe.procurement import (  # noqa: E402
        CrossCatalogError,
        InvalidBatch,
        parse_claims,
        parse_suppliers,
    )
    from catpipe.repository import utc_now  # noqa: E402
else:
    from .procurement import (  # noqa: E402
        CrossCatalogError,
        InvalidBatch,
        parse_claims,
        parse_suppliers,
    )
    from .repository import utc_now  # noqa: E402

__all__ = [
    # 规范名称
    "AuditError",
    "AuditBatchNotFound",
    "AuditHistoryError",
    "AuditSourceMissing",
    "AuditConnectionProofMissing",
    "AuditConsistencyError",
    "PackageError",
    "PackageIncomplete",
    "PackageVerificationError",
    "AuditSourcePackage",
    "AuditExporter",
    "export_package",
    "verify_package",
    "iter_package_records",
    "read_package",
    "package_status",
    "verify_evidence_record",
    "main",
    # 描述性别名（兼容不同命名习惯）
    "AuditPackage",
    "HistoricalSourcePackage",
    "SourcePackage",
    "AuditPackageBuilder",
    "AuditTrailPackage",
    "ProvenancePackage",
    "export_audit_package",
    "export_source_package",
    "build_audit_package",
    "build_source_package",
    "create_audit_package",
    "export_audit_trail",
    "export_history_package",
    "export_provenance_package",
    "build_provenance_package",
    "export_evidence_package",
    "write_audit_package",
    "verify_audit_package",
    "read_audit_records",
    "iter_audit_records",
]

PACKAGE_SCHEMA = "catpipe-audit-package/v1"
RECORD_TYPE = "catpipe.audit.group_evidence/v1"
EVIDENCE_FILE_NAME = "evidence.jsonl"
MANIFEST_FILE_NAME = "manifest.json"
DONE_FILE_NAME = "DONE"
# 其它常见“完成标记”命名；与 DONE 同内容、同顺序最后写入，任一存在都表示完成。
SUCCESS_MARKER_ALIASES = ("_SUCCESS",)
# 文件模式（--out 以 .jsonl 结尾）时的旁车后缀。
MANIFEST_SUFFIX = ".manifest.json"
DONE_SUFFIX = ".done"
DONE_SUFFIX_ALIASES = (".success",)

_READ_CHUNK = 1000


# --------------------------------------------------------------------------- #
# 异常（含一组描述性别名）
# --------------------------------------------------------------------------- #

class AuditError(CrossCatalogError):
    """审计来源包功能的基础异常（也是采购目录错误的一种）。"""


class AuditBatchNotFound(AuditError):
    """指定的成功目录批次不存在。"""


class AuditHistoryError(AuditError):
    """无法从现有采购库重建目标批次时点的选版/声明事实。"""


class AuditSourceMissing(AuditError):
    """目标时点的供应商发布版或必需来源行缺失/不可读。"""


class AuditConnectionProofMissing(AuditError):
    """缺少足以连通整组的当时有效声明证据，或证据与批次组成不一致。"""


class AuditConsistencyError(AuditError):
    """导出证据与批次持久化记录/来源行之间不一致。"""


class PackageError(AuditError):
    """来源包路径或打包过程错误。"""


class PackageIncomplete(PackageError):
    """包缺少 DONE 完成标记（仍在生成中或已失败的临时结果）。"""


class PackageVerificationError(PackageError):
    """已导出内容未通过全量独立复核。"""

    def __init__(self, message: str, report: dict[str, Any] | None = None):
        super().__init__(message)
        self.report = report or {}


# 兼容可能的命名习惯。
AuditExportError = AuditError
SourcePackageError = AuditError
AuditPackageNotFound = AuditBatchNotFound
MissingPublishedVersion = AuditSourceMissing
MissingSourceRow = AuditSourceMissing
MissingConnectionProof = AuditConnectionProofMissing


# --------------------------------------------------------------------------- #
# 小工具
# --------------------------------------------------------------------------- #

def _canonical_line(obj: Any) -> bytes:
    """证据行的规范字节形式：排序键、紧凑分隔、UTF-8、末尾换行。"""
    return (
        json.dumps(obj, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        + "\n"
    ).encode("utf-8")


def _sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _sha256_file(path: Path) -> tuple[str, int]:
    h = hashlib.sha256()
    size = 0
    with path.open("rb") as fh:
        while True:
            chunk = fh.read(1024 * 1024)
            if not chunk:
                break
            h.update(chunk)
            size += len(chunk)
    return h.hexdigest(), size


def _fsync_fileobj(fh: Any) -> None:
    fh.flush()
    os.fsync(fh.fileno())


def _fsync_dir(path: Path) -> None:
    try:
        fd = os.open(os.fspath(path), os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(fd)
    except OSError:
        pass
    finally:
        os.close(fd)


def _atomic_write_json(path: Path, value: dict[str, Any]) -> str:
    """临时文件 + fsync + 同目录原子改名；返回写入字节的 SHA-256。"""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + f".tmp-{uuid.uuid4().hex[:8]}")
    payload = json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True).encode("utf-8")
    with tmp.open("wb") as fh:
        fh.write(payload)
        _fsync_fileobj(fh)
    os.replace(tmp, path)
    _fsync_dir(path.parent)
    return _sha256_bytes(payload)


def _identity_key(ident: dict[str, Any] | tuple[str, int]) -> tuple[str, int]:
    if isinstance(ident, tuple):
        return ident
    return (str(ident["supplier"]), int(ident["product_id"]))


def _identity_dict(key: tuple[str, int]) -> dict[str, Any]:
    return {"supplier": key[0], "product_id": key[1]}


def _min_identity(keys: Sequence[tuple[str, int]]) -> tuple[str, int]:
    # 供应商名按 Unicode/字典序，product_id 按数值；tuple 比较正好如此。
    return min(keys, key=lambda k: (k[0], k[1]))


# --------------------------------------------------------------------------- #
# 批次时点事实重建
# --------------------------------------------------------------------------- #

@dataclass(slots=True)
class Pin:
    """某供应商在目标批次时点实际采用的不可变发布版。"""

    supplier: str
    version_id: str
    selected_batch: int
    db_path_recorded: str | None = None
    db_path_table: str | None = None
    selection_source: str = "batch_input"  # batch_input | cc_supplier_versions


@dataclass(slots=True)
class ClaimFact:
    claim_id: str
    a: tuple[str, int] | None
    b: tuple[str, int] | None
    active: bool
    introduced_batch: int | None
    last_op_batch: int | None
    withdrawn_batch: int | None = None
    endpoints_source: str = "batch_input"  # batch_input | cc_claims
    endpoint_resolvable: bool = True


@dataclass(slots=True)
class BatchFacts:
    batch_number: int
    created_at: str | None
    input_obj: dict[str, Any]
    input_json: str | None
    input_sha256: str
    summary_obj: dict[str, Any]
    pins: dict[str, Pin] = field(default_factory=dict)
    claims: dict[str, ClaimFact] = field(default_factory=dict)
    unresolved_claims: list[dict[str, Any]] = field(default_factory=list)


def _open_catalog_readonly(db_path: str | os.PathLike[str]) -> sqlite3.Connection:
    path = Path(db_path).expanduser()
    if not path.exists():
        raise AuditBatchNotFound(f"采购库不存在: {db_path}")
    uri = path.resolve().as_uri() + "?mode=ro"
    try:
        conn = sqlite3.connect(uri, uri=True, timeout=30)
    except sqlite3.OperationalError as exc:
        raise AuditError(f"无法以只读方式打开采购库 {db_path}: {exc}") from exc
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=30000")
    # 注意：不能设置 query_only=ON —— 该 PRAGMA 在当前 SQLite 上会连 TEMP
    # 表写入一起拒绝；mode=ro 已物理禁止写主库，TEMP 重放表不受影响。
    conn.execute("PRAGMA temp_store=FILE")
    conn.execute("PRAGMA cache_size=-20000")  # ~20 MiB，重放临时表可落盘
    return conn


def resolve_batch_number(conn: sqlite3.Connection, value: Any) -> int:
    """把整数/数字串/外部 batch_id 解析为连续成功批次号；None 取最新。"""
    if value is None:
        row = conn.execute("SELECT MAX(batch_number) m FROM cc_batches").fetchone()
        if row["m"] is None:
            raise AuditBatchNotFound("采购库中还没有任何成功批次")
        return int(row["m"])
    if isinstance(value, int) and not isinstance(value, bool):
        return int(value)
    text = str(value).strip()
    if text.lstrip("-").isdigit():
        return int(text)
    # 外部批次 ID：匹配 summary.batch_id 或原始输入中的常见字段。
    for r in conn.execute(
        "SELECT batch_number,summary_json,input_json FROM cc_batches"
    ).fetchall():
        for blob in (r["summary_json"], r["input_json"]):
            if not blob:
                continue
            try:
                obj = json.loads(blob)
            except json.JSONDecodeError:
                continue
            if not isinstance(obj, dict):
                continue
            candidates = [obj.get("batch_id"), obj.get("external_batch_id"),
                          obj.get("label"), obj.get("batch_label")]
            if isinstance(obj.get("summary"), dict):
                candidates.append(obj["summary"].get("batch_id"))
            if any(c is not None and str(c) == text for c in candidates):
                return int(r["batch_number"])
    raise AuditBatchNotFound(f"无法把 {value!r} 解析为成功批次号或已知 batch_id")


def reconstruct_batch_facts(
    conn: sqlite3.Connection, batch_number: int
) -> BatchFacts:
    """只凭采购库重建第 N 个成功批次时点的选版与声明状态。"""
    n = int(batch_number)
    row = conn.execute(
        "SELECT batch_number,created_at,input_json,summary_json "
        "FROM cc_batches WHERE batch_number=?",
        (n,),
    ).fetchone()
    if row is None:
        latest = conn.execute(
            "SELECT MAX(batch_number) m FROM cc_batches"
        ).fetchone()["m"]
        raise AuditBatchNotFound(
            f"成功目录批次 {n} 不存在"
            + (f"（现有成功批次 1..{latest}）" if latest else "（库中没有成功批次）")
        )

    input_raw = row["input_json"]
    try:
        input_obj = json.loads(input_raw) if input_raw else {}
    except json.JSONDecodeError as exc:
        raise AuditHistoryError(f"批次 {n} 的 input_json 无法解析: {exc}") from exc
    if not isinstance(input_obj, dict):
        input_obj = {}
    try:
        summary_obj = json.loads(row["summary_json"]) if row["summary_json"] else {}
    except json.JSONDecodeError:
        summary_obj = {}

    batch_rows = conn.execute(
        "SELECT batch_number,input_json FROM cc_batches "
        "WHERE batch_number<=? ORDER BY batch_number",
        (n,),
    ).fetchall()

    # ---- 重放供应商选版 --------------------------------------------------
    replay_pins: dict[str, Pin] = {}
    for br in batch_rows:
        b = int(br["batch_number"])
        try:
            data = json.loads(br["input_json"]) if br["input_json"] else {}
        except json.JSONDecodeError:
            continue
        if not isinstance(data, dict):
            continue
        try:
            selections = parse_suppliers(data)
        except InvalidBatch as exc:
            raise AuditHistoryError(f"重放批次 {b} 的供应商选择失败: {exc}") from exc
        for sel in selections:
            if sel.version_id is None:
                # 与采购流水线一致：未显式给 version 的条目不触发换版。
                continue
            prev = replay_pins.get(sel.name)
            db_raw = sel.db_path or (prev.db_path_recorded if prev else None)
            replay_pins[sel.name] = Pin(
                supplier=sel.name,
                version_id=sel.version_id,
                selected_batch=b,
                db_path_recorded=db_raw,
                selection_source="batch_input",
            )

    # 用 cc_supplier_versions 当前行兜底/交叉校验。
    pins: dict[str, Pin] = dict(replay_pins)
    table_rows = conn.execute(
        "SELECT supplier,db_path,version_id,selected_batch FROM cc_supplier_versions"
    ).fetchall()
    for tr in table_rows:
        name = tr["supplier"]
        sb = int(tr["selected_batch"])
        table_pin = Pin(
            supplier=name,
            version_id=tr["version_id"],
            selected_batch=sb,
            db_path_table=tr["db_path"],
            db_path_recorded=tr["db_path"],
            selection_source="cc_supplier_versions",
        )
        if sb <= n:
            # 当前行就是第 N 批时点生效的选择（之后没有再选版）。
            rp = replay_pins.get(name)
            if rp is not None and rp.version_id != table_pin.version_id:
                raise AuditHistoryError(
                    f"供应商 {name!r} 的批次输入重放版本 {rp.version_id!r} 与采购库 "
                    f"实际选版 {table_pin.version_id!r}（selected_batch={sb}）不一致"
                )
            if rp is None:
                pins[name] = table_pin
            else:
                # 保留输入中的原始路径，同时补上解析后的表内绝对路径。
                rp.db_path_table = tr["db_path"]
        else:
            # 表内是 N 之后的新选版，绝不能当作 N 时点版本；只借其库路径定位文件。
            rp = replay_pins.get(name)
            if rp is not None:
                rp.db_path_table = tr["db_path"]
            # rp is None：该供应商在 N 时点的版本无法从现有事实重建；
            # 仅当它出现在导出组中时才会触发严格失败。

    # ---- 重放声明生命周期 ------------------------------------------------
    claims: dict[str, ClaimFact] = {}
    for br in batch_rows:
        b = int(br["batch_number"])
        try:
            data = json.loads(br["input_json"]) if br["input_json"] else {}
        except json.JSONDecodeError:
            continue
        if not isinstance(data, dict):
            continue
        try:
            ops = parse_claims(data)
        except InvalidBatch as exc:
            raise AuditHistoryError(f"重放批次 {b} 的声明失败: {exc}") from exc
        for op in ops:
            cid = op.claim_id
            if op.op == "withdraw":
                fact = claims.get(cid)
                if fact is None:
                    # 输入历史缺失（例如批次以 kwargs 应用且未存输入）时的占位，
                    # 下面会用 cc_claims 补全。
                    claims[cid] = ClaimFact(
                        claim_id=cid, a=None, b=None, active=False,
                        introduced_batch=None, last_op_batch=b,
                        withdrawn_batch=b, endpoint_resolvable=False,
                    )
                else:
                    fact.active = False
                    fact.last_op_batch = b
                    fact.withdrawn_batch = b
                continue
            a_key = op.a.key if op.a is not None else None
            b_key = op.b.key if op.b is not None else None
            fact = claims.get(cid)
            if fact is None or fact.a is None:
                claims[cid] = ClaimFact(
                    claim_id=cid, a=a_key, b=b_key, active=True,
                    introduced_batch=b, last_op_batch=b,
                )
            else:
                fact.a = a_key
                fact.b = b_key
                fact.active = True
                fact.last_op_batch = b
                fact.endpoint_resolvable = True

    # 用 cc_claims 的生命周期列补全/兜底。
    unresolved: list[dict[str, Any]] = []
    claim_rows = conn.execute("SELECT * FROM cc_claims").fetchall()
    for cr in claim_rows:
        cid = cr["claim_id"]
        created_b = int(cr["created_batch"])
        updated_b = int(cr["updated_batch"])
        withdrawn_b = cr["withdrawn_batch"]
        withdrawn_b = int(withdrawn_b) if withdrawn_b is not None else None
        active_at_n = not (withdrawn_b is not None and withdrawn_b <= n)
        sa = cr["supplier_a"]
        pa = int(cr["product_a"])
        sb = cr["supplier_b"]
        pb = int(cr["product_b"])
        table_a, table_b = (sa, pa), (sb, pb)

        fact = claims.get(cid)
        if fact is not None and fact.endpoint_resolvable and fact.a is not None:
            # 交叉校验重放结果与生命周期列（仅当最后更新不晚于 N 时，
            # 表内端点才是 N 时点端点）。
            fact.introduced_batch = fact.introduced_batch or created_b
            if updated_b <= n:
                if active_at_n and (fact.a != table_a or fact.b != table_b):
                    raise AuditHistoryError(
                        f"声明 {cid!r} 重放端点 {fact.a}/{fact.b} 与采购库实际端点 "
                        f"{table_a}/{table_b} 不一致"
                    )
            if fact.active != active_at_n:
                # 以不可变批次输入重放为准，但记录该分歧；最终连通性校验会裁决。
                fact.active = active_at_n
                if not active_at_n:
                    fact.withdrawn_batch = withdrawn_b
            continue

        # 输入重放没有端点（空 input_json / kwargs 批次）：尝试用表内当前行。
        if created_b > n:
            continue
        if not active_at_n:
            claims[cid] = ClaimFact(
                claim_id=cid, a=table_a, b=table_b, active=False,
                introduced_batch=created_b, last_op_batch=updated_b,
                withdrawn_batch=withdrawn_b, endpoints_source="cc_claims",
            )
            continue
        if updated_b <= n:
            # 最后一次端点变更不晚于 N：当前端点就是 N 时点端点。
            claims[cid] = ClaimFact(
                claim_id=cid, a=table_a, b=table_b, active=True,
                introduced_batch=created_b, last_op_batch=updated_b,
                endpoints_source="cc_claims",
            )
        else:
            # 声明在 N 之后被替换过，N 时点端点已无法从现有事实还原。
            claims[cid] = ClaimFact(
                claim_id=cid, a=None, b=None, active=True,
                introduced_batch=created_b, last_op_batch=updated_b,
                endpoints_source="cc_claims", endpoint_resolvable=False,
            )
            unresolved.append({
                "claim_id": cid,
                "introduced_batch": created_b,
                "last_update_batch": updated_b,
                "reason": "endpoints_at_batch_unavailable_later_replaced",
            })

    input_sha = _sha256_bytes((row["input_json"] or "").encode("utf-8"))
    return BatchFacts(
        batch_number=n,
        created_at=row["created_at"],
        input_obj=input_obj,
        input_json=row["input_json"],
        input_sha256=input_sha,
        summary_obj=summary_obj,
        pins=pins,
        claims=claims,
        unresolved_claims=unresolved,
    )


# --------------------------------------------------------------------------- #
# 第 N 批目录快照的磁盘 TEMP 重放（流式，不把目录放进 Python 内存）
# --------------------------------------------------------------------------- #

def replay_batch_snapshot(conn: sqlite3.Connection, batch_number: int) -> int:
    """把 <=N 的全部 put/delete 重放到 TEMP 表；返回快照组数。"""
    conn.execute("DROP TABLE IF EXISTS temp.cc_audit_replay")
    conn.execute(
        "CREATE TEMP TABLE cc_audit_replay ("
        "group_supplier TEXT NOT NULL,"
        "group_product_id INTEGER NOT NULL,"
        "record_json TEXT NOT NULL,"
        "PRIMARY KEY(group_supplier,group_product_id)"
        ") WITHOUT ROWID"
    )
    cursor = conn.execute(
        "SELECT op,group_supplier,group_product_id,record_json "
        "FROM cc_batch_changes WHERE batch_number<=? "
        "ORDER BY batch_number",
        (batch_number,),
    )
    while True:
        rows = cursor.fetchmany(_READ_CHUNK)
        if not rows:
            break
        for r in rows:
            if r["op"] == "delete":
                conn.execute(
                    "DELETE FROM temp.cc_audit_replay "
                    "WHERE group_supplier=? AND group_product_id=?",
                    (r["group_supplier"], r["group_product_id"]),
                )
            else:
                conn.execute(
                    "INSERT OR REPLACE INTO temp.cc_audit_replay VALUES(?,?,?)",
                    (r["group_supplier"], r["group_product_id"], r["record_json"]),
                )
    return int(conn.execute(
        "SELECT COUNT(*) c FROM temp.cc_audit_replay"
    ).fetchone()["c"])


def build_active_claim_tables(conn: sqlite3.Connection, facts: BatchFacts) -> int:
    """把 N 时点有效且端点可证的声明写入 TEMP 边表。"""
    conn.execute("DROP TABLE IF EXISTS temp.cc_audit_claims")
    conn.execute(
        "CREATE TEMP TABLE cc_audit_claims ("
        "claim_id TEXT PRIMARY KEY,"
        "supplier_a TEXT, product_a INTEGER,"
        "supplier_b TEXT, product_b INTEGER,"
        "introduced_batch INTEGER, last_op_batch INTEGER,"
        "endpoints_source TEXT"
        ")"
    )
    conn.execute("DROP TABLE IF EXISTS temp.cc_audit_edges")
    conn.execute(
        "CREATE TEMP TABLE cc_audit_edges ("
        "claim_id TEXT NOT NULL,"
        "supplier TEXT NOT NULL,"
        "product_id INTEGER NOT NULL,"
        "other_supplier TEXT NOT NULL,"
        "other_product_id INTEGER NOT NULL,"
        "PRIMARY KEY(supplier,product_id,claim_id)"
        ") WITHOUT ROWID"
    )
    count = 0
    for cf in facts.claims.values():
        if not cf.active or not cf.endpoint_resolvable or cf.a is None or cf.b is None:
            continue
        conn.execute(
            "INSERT OR REPLACE INTO temp.cc_audit_claims VALUES(?,?,?,?,?,?,?,?)",
            (cf.claim_id, cf.a[0], cf.a[1], cf.b[0], cf.b[1],
             cf.introduced_batch, cf.last_op_batch, cf.endpoints_source),
        )
        conn.execute(
            "INSERT OR IGNORE INTO temp.cc_audit_edges VALUES(?,?,?,?,?)",
            (cf.claim_id, cf.a[0], cf.a[1], cf.b[0], cf.b[1]),
        )
        conn.execute(
            "INSERT OR IGNORE INTO temp.cc_audit_edges VALUES(?,?,?,?,?)",
            (cf.claim_id, cf.b[0], cf.b[1], cf.a[0], cf.a[1]),
        )
        count += 1
    return count


# --------------------------------------------------------------------------- #
# 供应商不可变发布版的只读访问（按供应商缓存连接）
# --------------------------------------------------------------------------- #

class SourceProvider:
    def __init__(self, catalog_db_path: str | os.PathLike[str], facts: BatchFacts):
        self.catalog_db_path = str(Path(catalog_db_path).expanduser().resolve())
        self.catalog_dir = Path(self.catalog_db_path).parent
        self.facts = facts
        self._handles: dict[str, sqlite3.Connection] = {}
        self._versions: dict[str, dict[str, Any]] = {}
        self._resolved_paths: dict[str, str] = {}

    def close(self) -> None:
        for c in self._handles.values():
            c.close()
        self._handles.clear()

    def __enter__(self) -> "SourceProvider":
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()

    def _candidate_paths(self, pin: Pin | None, supplier: str) -> list[Path]:
        raw_values: list[str | None] = []
        if pin is not None:
            raw_values.append(pin.db_path_recorded)
            raw_values.append(pin.db_path_table)
        else:
            # 没有 N 时点选版记录时，仍尝试采购库内当前路径兜底定位文件，
            # 但版本号无权威来源 —— 下面会严格失败。
            row = None
            # facts.pins 里可能有 selected_batch>N 的记录（带 table 路径）。
            existing = self.facts.pins.get(supplier)
            if existing is not None:
                raw_values.append(existing.db_path_table)
        candidates: list[Path] = []
        seen: set[str] = set()

        def add(p: Path) -> None:
            key = os.path.normpath(os.fspath(p))
            if key not in seen:
                seen.add(key)
                candidates.append(p)

        for raw in raw_values:
            if not raw or raw == ":memory:":
                continue
            p = Path(raw).expanduser()
            if p.is_absolute():
                add(p)
            else:
                add(Path.cwd() / p)
                add(self.catalog_dir / p)
        return candidates

    def _open_candidate(self, path: Path) -> sqlite3.Connection | None:
        if not path.is_file():
            return None
        try:
            uri = path.resolve().as_uri() + "?mode=ro"
            conn = sqlite3.connect(uri, uri=True, timeout=30)
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA busy_timeout=30000")
            conn.execute("PRAGMA query_only=ON")
            # 验证结构兼容。
            conn.execute("SELECT version_id FROM published_versions LIMIT 1")
            return conn
        except (sqlite3.OperationalError, sqlite3.DatabaseError):
            try:
                conn.close()  # type: ignore[possibly-undefined]
            except Exception:
                pass
            return None

    def version_info(self, supplier: str) -> dict[str, Any]:
        """返回（并缓存）该供应商 N 时点发布版的只读句柄与版本行。

        找不到选版事实、库文件或目标 ``published_versions`` 行时严格失败。
        """
        if supplier in self._versions:
            return self._versions[supplier]

        pin = self.facts.pins.get(supplier)
        if pin is None or not pin.version_id:
            raise AuditSourceMissing(
                f"供应商 {supplier!r} 在批次 {self.facts.batch_number} 时点实际采用的 "
                "观察版无法重建（缺少选版事实）；拒绝降级导出"
            )

        candidates = self._candidate_paths(pin, supplier)
        if not candidates:
            raise AuditSourceMissing(
                f"供应商 {supplier!r} 的选版 {pin.version_id!r} 没有可用的源库路径"
            )

        attempted: list[str] = []
        last_schema_error: str | None = None
        for path in candidates:
            attempted.append(os.fspath(path))
            conn = self._open_candidate(path)
            if conn is None:
                continue
            try:
                row = conn.execute(
                    "SELECT version_id,domain_lo,domain_hi,published_at,"
                    "coverage_proof_json,manifest_json "
                    "FROM published_versions WHERE version_id=?",
                    (pin.version_id,),
                ).fetchone()
            except sqlite3.OperationalError as exc:
                last_schema_error = str(exc)
                conn.close()
                continue
            if row is None:
                # 库能打开，但目标不可变发布版不在这个文件里；继续尝试其它候选路径。
                conn.close()
                continue
            self._handles[supplier] = conn
            self._resolved_paths[supplier] = os.fspath(path.resolve())
            proof_raw = row["coverage_proof_json"] or ""
            proof_obj: dict[str, Any] | None
            coverage: dict[str, Any] | None = None
            if proof_raw.strip():
                try:
                    proof_obj = json.loads(proof_raw)
                except json.JSONDecodeError:
                    proof_obj = None
                if isinstance(proof_obj, dict):
                    cov = proof_obj.get("coverage")
                    if isinstance(cov, dict):
                        coverage = cov
            info = {
                "supplier": supplier,
                "version_id": pin.version_id,
                "selected_batch": pin.selected_batch,
                "selection_source": pin.selection_source,
                "source_db_path_recorded": pin.db_path_recorded,
                "source_db_path_table": pin.db_path_table,
                "source_db_path_opened": os.fspath(path.resolve()),
                "published_at": row["published_at"],
                "domain_lo": int(row["domain_lo"]),
                "domain_hi": int(row["domain_hi"]),
                "coverage": coverage,
                "coverage_proof_available": coverage is not None,
                "coverage_proof_sha256": _sha256_bytes(proof_raw.encode("utf-8")),
                "manifest_sha256": _sha256_bytes(
                    (row["manifest_json"] or "").encode("utf-8")
                ),
            }
            self._versions[supplier] = info
            return info

        detail = f"；结构错误: {last_schema_error}" if last_schema_error else ""
        raise AuditSourceMissing(
            f"供应商 {supplier!r} 在批次 {self.facts.batch_number} 时点采用的不可变发布版 "
            f"{pin.version_id!r} 在任何候选源库中都不存在（尝试: {attempted}）{detail}"
        )

    def product_row(self, supplier: str, product_id: int) -> dict[str, Any] | None:
        info = self.version_info(supplier)
        conn = self._handles[supplier]
        row = conn.execute(
            "SELECT version_id,product_id,url,price,product_revision "
            "FROM published_products WHERE version_id=? AND product_id=?",
            (info["version_id"], int(product_id)),
        ).fetchone()
        if row is None:
            return None
        return {
            "version_id": row["version_id"],
            "product_id": int(row["product_id"]),
            "url": row["url"],
            "price": int(row["price"]),
            "product_revision": int(row["product_revision"]),
        }


# --------------------------------------------------------------------------- #
# 区间覆盖 / 图连通的纯函数（导出端与离线复核端共用）
# --------------------------------------------------------------------------- #

def merge_segments(segments: Sequence[Sequence[int]]) -> list[list[int]]:
    ordered = sorted((int(lo), int(hi)) for lo, hi in segments if hi > lo)
    union: list[list[int]] = []
    for lo, hi in ordered:
        if not union or lo > union[-1][1]:
            union.append([lo, hi])
        elif hi > union[-1][1]:
            union[-1][1] = hi
    return union


def pid_covered_by_segments(product_id: int, coverage: dict[str, Any] | None) -> bool | None:
    if not isinstance(coverage, dict):
        return None
    segments = coverage.get("union_segments")
    if not isinstance(segments, list):
        return None
    for seg in segments:
        try:
            lo, hi = int(seg[0]), int(seg[1])
        except (TypeError, ValueError, IndexError):
            continue
        if lo <= product_id < hi:
            return True
    return False


def segments_cover_domain(
    coverage: dict[str, Any] | None, domain_lo: int, domain_hi: int
) -> bool | None:
    """根据内嵌 union_segments 独立判断是否完整覆盖域；无 segments 时返回 None。"""
    if not isinstance(coverage, dict) or not isinstance(
        coverage.get("union_segments"), list
    ):
        return None
    union = merge_segments(coverage["union_segments"])
    cursor = domain_lo
    for lo, hi in union:
        if lo > cursor:
            return False
        cursor = max(cursor, hi)
    return cursor >= domain_hi


def component_from_edges(
    start: tuple[str, int], edges: Sequence[dict[str, Any]]
) -> set[tuple[str, int]]:
    adjacency: dict[tuple[str, int], set[tuple[str, int]]] = {}
    for e in edges:
        a = _identity_key(e["a"])
        b = _identity_key(e["b"])
        adjacency.setdefault(a, set()).add(b)
        adjacency.setdefault(b, set()).add(a)
    seen = {start}
    stack = [start]
    while stack:
        cur = stack.pop()
        for nxt in adjacency.get(cur, ()):  # type: ignore[arg-type]
            if nxt not in seen:
                seen.add(nxt)
                stack.append(nxt)
    return seen


def spanning_tree_claim_ids(
    vertices: Sequence[tuple[str, int]],
    claim_edges: Sequence[tuple[str, tuple[str, int], tuple[str, int]]],
) -> tuple[list[str], list[dict[str, Any]]]:
    """确定性 BFS/并查集：返回能连通全部顶点的生成树声明编号与边。

    claim_edges: (claim_id, a_key, b_key)。同一条无向边可能有多个声明，
    按 claim_id 排序后优先选小编号，保证结果可复现。
    """
    vertex_set = set(vertices)
    parent = {v: v for v in vertices}

    def find(x: tuple[str, int]) -> tuple[str, int]:
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    ordered = sorted(claim_edges, key=lambda ce: ce[0])
    tree_ids: list[str] = []
    tree_edges: list[dict[str, Any]] = []
    for cid, a, b in ordered:
        if a not in vertex_set or b not in vertex_set:
            continue
        ra, rb = find(a), find(b)
        if ra == rb:
            continue
        parent[rb] = ra
        tree_ids.append(cid)
        tree_edges.append({"claim_id": cid, "a": _identity_dict(a), "b": _identity_dict(b)})
    roots = {find(v) for v in vertices}
    if len(roots) != 1:
        return tree_ids, tree_edges  # 调用方依据覆盖情况判定失败
    return tree_ids, tree_edges


# --------------------------------------------------------------------------- #
# 单组证据构造
# --------------------------------------------------------------------------- #

def _component_keys_for_group(
    conn: sqlite3.Connection, key: tuple[str, int]
) -> list[tuple[str, int]]:
    rows = conn.execute(
        """
        WITH RECURSIVE walk(supplier, product_id) AS (
            SELECT ?, ?
            UNION
            SELECT e.other_supplier, e.other_product_id
              FROM temp.cc_audit_edges e
              JOIN walk w
                ON e.supplier=w.supplier AND e.product_id=w.product_id
        )
        SELECT supplier, product_id FROM walk
        ORDER BY supplier ASC, product_id ASC
        """,
        key,
    ).fetchall()
    return [(r["supplier"], int(r["product_id"])) for r in rows]


def _claims_for_component(
    conn: sqlite3.Connection, component: set[tuple[str, int]]
) -> list[dict[str, Any]]:
    """取出两端都在组件内的全部当时有效声明（每条声明只输出一次）。"""
    claim_ids: set[str] = set()
    for supplier, pid in component:
        for r in conn.execute(
            "SELECT claim_id FROM temp.cc_audit_edges WHERE supplier=? AND product_id=?",
            (supplier, pid),
        ):
            claim_ids.add(r["claim_id"])

    out: list[dict[str, Any]] = []
    for cid in sorted(claim_ids):
        r = conn.execute(
            "SELECT * FROM temp.cc_audit_claims WHERE claim_id=?", (cid,)
        ).fetchone()
        if r is None:
            continue
        a = (r["supplier_a"], int(r["product_a"]))
        b = (r["supplier_b"], int(r["product_b"]))
        # 递归组件在闭包内，理论上两端都在；防御性再判一次。
        if a not in component or b not in component:
            continue
        out.append({
            "claim_id": cid,
            "status_at_batch": "active",
            "a": _identity_dict(a),
            "b": _identity_dict(b),
            "introduced_batch": r["introduced_batch"],
            "last_op_batch": r["last_op_batch"],
            "endpoints_source": r["endpoints_source"],
        })
    return out


def build_group_evidence(
    conn: sqlite3.Connection,
    facts: BatchFacts,
    provider: SourceProvider,
    record: dict[str, Any],
) -> dict[str, Any]:
    batch_n = facts.batch_number
    group_key = _identity_key(record["group_key"])
    member_keys = [_identity_key(m) for m in record.get("members", [])]
    if not member_keys:
        raise AuditConsistencyError(
            f"批次 {batch_n} 组 {group_key} 的持久化记录没有成员"
        )
    if len(set(member_keys)) != len(member_keys):
        raise AuditConsistencyError(
            f"批次 {batch_n} 组 {group_key} 的成员身份重复: {member_keys}"
        )

    expected_key = _min_identity(member_keys)
    if group_key != expected_key:
        raise AuditConsistencyError(
            f"批次 {batch_n} 组键 {group_key} 不是最小成员身份 {expected_key}"
        )
    if (record.get("group_supplier"), int(record.get("group_product_id", -1))) != group_key:
        raise AuditConsistencyError(
            f"批次 {batch_n} 记录的 group_supplier/group_product_id 与 group_key 不一致"
        )

    # ---- 声明图连通性：重建组件必须与批次组成完全一致 --------------------
    component = set(_component_keys_for_group(conn, group_key))
    if component != set(member_keys):
        missing_proof = sorted(set(member_keys) - component)
        extra = sorted(component - set(member_keys))
        raise AuditConnectionProofMissing(
            f"批次 {batch_n} 组 {group_key} 的当时有效声明不能完整连通整组："
            f"缺少连接证明的成员={missing_proof}，图中额外身份={extra}。"
            "拒绝在缺少有效连接证明时导出。"
        )

    claims_out = _claims_for_component(conn, component)
    claim_edges = [
        (c["claim_id"], _identity_key(c["a"]), _identity_key(c["b"]))
        for c in claims_out
    ]
    tree_ids, tree_edges = spanning_tree_claim_ids(member_keys, claim_edges)
    if len(member_keys) > 1:
        tree_component = component_from_edges(
            group_key, [{"a": te["a"], "b": te["b"]} for te in tree_edges]
        )
        tree_connected = tree_component == set(member_keys)
    else:
        tree_connected = True
    if len(member_keys) > 1 and not tree_connected:
        raise AuditConnectionProofMissing(
            f"批次 {batch_n} 组 {group_key} 找到的声明仍不足以生成连通整组的边树"
        )

    edges_out = [
        {"claim_id": cid, "a": _identity_dict(a), "b": _identity_dict(b)}
        for cid, a, b in sorted(claim_edges, key=lambda x: x[0])
    ]
    connection_evidence = {
        "vertices": [_identity_dict(k) for k in sorted(member_keys)],
        "edge_count": len(edges_out),
        "edges": edges_out,
        "claim_ids": [c["claim_id"] for c in claims_out],
        "connecting_claim_ids": tree_ids,
        "spanning_tree_edges": tree_edges,
        "all_vertices_connected": True,
        "verification_hint": (
            "由 claims[].a/b 建无向图；从 group_key 出发的连通分量必须恰好等于 "
            "members；connecting_claim_ids 对应边必须能连通全部顶点"
        ),
    }

    # ---- 各成员的 N 时点观察版 + 在售行/明确缺席 --------------------------
    version_blocks: dict[str, dict[str, Any]] = {}
    member_entries: list[dict[str, Any]] = []
    on_sale: list[dict[str, Any]] = []

    for key in sorted(member_keys):
        supplier, pid = key
        info = provider.version_info(supplier)  # 缺发布版会严格失败
        version_blocks[supplier] = {
            k: info[k]
            for k in (
                "supplier", "version_id", "selected_batch", "selection_source",
                "source_db_path_recorded", "source_db_path_table",
                "source_db_path_opened", "published_at", "domain_lo", "domain_hi",
                "coverage", "coverage_proof_available",
                "coverage_proof_sha256", "manifest_sha256",
            )
        }
        source_row = provider.product_row(supplier, pid)
        entry: dict[str, Any] = {
            "supplier": supplier,
            "product_id": pid,
            "identity": _identity_dict(key),
            "adopted_version_id": info["version_id"],
            # 常用短名/别名，方便不同阅读习惯。
            "version_id": info["version_id"],
            "observed_version_id": info["version_id"],
            "adopted_version": info["version_id"],
        }
        if source_row is not None:
            entry["listing_status"] = "on_sale"
            entry["on_sale"] = True
            entry["available"] = True
            entry["source_row"] = source_row
            entry["absence_evidence"] = None
            on_sale.append({"key": key, "row": source_row})
        else:
            dlo, dhi = info["domain_lo"], info["domain_hi"]
            within = dlo <= pid < dhi
            if not within:
                raise AuditSourceMissing(
                    f"批次 {batch_n} 组 {group_key} 成员 {key} 在发布版 "
                    f"{info['version_id']!r} 中无在售行，且商品 ID 不在版本域 "
                    f"[{dlo},{dhi}) 内，无法给出“明确缺席”证明；拒绝降级导出"
                )
            covered_by_segments = pid_covered_by_segments(pid, info["coverage"])
            if info["coverage"] is not None and covered_by_segments is False:
                raise AuditConsistencyError(
                    f"批次 {batch_n} 成员 {key} 在版本 {info['version_id']!r} 无在售行，"
                    "但持久化覆盖证明的合并区间不覆盖该 ID，缺席不能成立"
                )
            domain_covered = segments_cover_domain(info["coverage"], dlo, dhi)
            entry["listing_status"] = "absent"
            entry["on_sale"] = False
            entry["available"] = False
            entry["source_row"] = None
            entry["absence_evidence"] = {
                "basis": "no_published_product_row_within_complete_published_domain",
                "version_id": info["version_id"],
                "domain": [dlo, dhi],
                "product_within_domain": True,
                "coverage_proof_available": info["coverage_proof_available"],
                "coverage_covered_flag": (
                    info["coverage"].get("covered")
                    if isinstance(info["coverage"], dict) else None
                ),
                "union_segments_cover_full_domain": domain_covered,
                "id_covered_by_union_segments": covered_by_segments,
                "semantics": (
                    "catpipe 发布版要求完整覆盖 ID 域；域内无 published_products 行"
                    "即该观察版中的明确下架/缺席"
                ),
            }
        member_entries.append(entry)

    if not on_sale:
        # 能进入 cc_catalog / 快照的组必然有赢家；没有在售行说明批次事实损坏。
        raise AuditConsistencyError(
            f"批次 {batch_n} 组 {group_key} 在源发布版中找不到任何在售行，"
            "与该组存在于目录快照矛盾"
        )

    # 规格明确要求：最低报价成员的“完整来源行”必须存在；记录赢家的来源行
    # 缺失属于“缺少来源行”，严格失败而不是把赢家改判给别人后降级导出。
    recorded_winner_key = _identity_key(record["winning_identity"])
    winner_entry = next(
        (m for m in member_entries if _identity_key(m) == recorded_winner_key), None
    )
    if winner_entry is None:
        raise AuditConsistencyError(
            f"批次 {batch_n} 组 {group_key} 的记录赢家 {recorded_winner_key} 不在成员中"
        )
    if winner_entry["listing_status"] != "on_sale" or winner_entry["source_row"] is None:
        raise AuditSourceMissing(
            f"批次 {batch_n} 组 {group_key} 的最低报价成员 {recorded_winner_key} "
            "在其锁定观察版中缺少完整来源行（记录为缺席或行已丢失）；"
            "拒绝在缺少赢家来源行时导出"
        )

    # ---- 独立重算最低报价赢家，并与批次记录/完整来源行核对 ----------------
    winner_choice = min(
        on_sale,
        key=lambda item: (
            item["row"]["price"], item["key"][0], item["key"][1]
        ),
    )
    winner_key = winner_choice["key"]
    winner_row = winner_choice["row"]

    recorded_winner = recorded_winner_key
    if recorded_winner != winner_key:
        raise AuditConsistencyError(
            f"批次 {batch_n} 组 {group_key} 重算赢家 {winner_key} 与批次记录赢家 "
            f"{recorded_winner} 不一致"
        )
    record_values = {
        "supplier": winner_key[0],
        "product_id": winner_row["product_id"],
        "url": winner_row["url"],
        "price": winner_row["price"],
        "product_revision": winner_row["product_revision"],
    }
    for field_name, source_value in record_values.items():
        if record.get(field_name) != source_value:
            raise AuditConsistencyError(
                f"批次 {batch_n} 组 {group_key} 赢家字段 {field_name}="
                f"{record.get(field_name)!r} 与不可变来源行 "
                f"{source_value!r} 不一致"
            )

    winner_block = {
        "identity": _identity_dict(winner_key),
        "selection_rule": "minimum of (source_row.price, supplier, product_id)",
        "selection_rule_sql": "ORDER BY price ASC, supplier ASC, product_id ASC LIMIT 1",
        "catalog_row": {
            "supplier": record["supplier"],
            "product_id": int(record["product_id"]),
            "url": record["url"],
            "price": int(record["price"]),
            "product_revision": int(record["product_revision"]),
        },
        "complete_source_row": winner_row,
        # 短名别名：最低报价成员的完整来源行。
        "source_row": winner_row,
        "source": winner_row,
        "source_row_matches_catalog_row": True,
    }

    evidence = {
        "record_type": RECORD_TYPE,
        "package_schema": PACKAGE_SCHEMA,
        "batch_number": batch_n,
        "batch": batch_n,
        "batch_created_at": facts.created_at,
        "group_key": _identity_dict(group_key),
        "key": _identity_dict(group_key),
        "group_supplier": group_key[0],
        "group_product_id": group_key[1],
        "members": member_entries,
        "supplier_versions": version_blocks,
        "winner": winner_block,
        "claims": claims_out,
        # 顶层直接给出“足以连接整组”的当时有效声明编号，便于快速检索。
        "claim_ids": [c["claim_id"] for c in claims_out],
        "connecting_claim_ids": tree_ids,
        "connection_evidence": connection_evidence,
        # 批次提交时规范化并持久化的完整目录记录，作为采购库侧的事实锚点。
        "catalog_record_at_batch": record,
        "checks": {
            "member_count": len(member_keys),
            "members_sorted_unique": member_keys == sorted(set(member_keys)),
            "group_key_is_min_identity": True,
            "claim_component_equals_members": True,
            "spanning_tree_connects_all_members": len(member_keys) == 1 or tree_connected,
            "winner_recomputed_matches": True,
            "winner_source_row_complete": True,
        },
    }
    return evidence


def _find_root_local(
    v: tuple[str, int],
    edges: Sequence[tuple[str, tuple[str, int], tuple[str, int]]],
) -> tuple[str, int]:
    comp = component_from_edges(v, [{"a": a, "b": b} for _, a, b in edges])
    return _min_identity(list(comp))


# --------------------------------------------------------------------------- #
# 离线独立复核（只凭导出物，不打开任何数据库）
# --------------------------------------------------------------------------- #

def verify_evidence_record(record: Any) -> list[str]:
    """对单条证据做纯离线一致性复核；返回问题字符串列表（空列表=通过）。"""
    problems: list[str] = []
    if not isinstance(record, dict):
        return ["证据行不是 JSON 对象"]
    if record.get("record_type") != RECORD_TYPE:
        problems.append(f"record_type 不是 {RECORD_TYPE!r}")

    batch_n = record.get("batch_number")
    if not isinstance(batch_n, int):
        problems.append("缺少整数 batch_number")

    gk_obj = record.get("group_key")
    if not isinstance(gk_obj, dict):
        problems.append("缺少 group_key 对象")
        return problems
    try:
        group_key = _identity_key(gk_obj)
    except (KeyError, TypeError, ValueError):
        problems.append("group_key 身份无法解析")
        return problems

    if record.get("group_supplier") != group_key[0] or record.get("group_product_id") != group_key[1]:
        problems.append("group_supplier/group_product_id 与 group_key 不一致")

    members = record.get("members")
    if not isinstance(members, list) or not members:
        problems.append("members 必须是非空数组")
        return problems

    member_keys: list[tuple[str, int]] = []
    for i, m in enumerate(members):
        if not isinstance(m, dict):
            problems.append(f"members[{i}] 不是对象")
            continue
        try:
            key = _identity_key(m["identity"] if "identity" in m else m)
        except (KeyError, TypeError, ValueError):
            problems.append(f"members[{i}] 身份无法解析")
            continue
        member_keys.append(key)
        if m.get("adopted_version_id") != (
            record.get("supplier_versions", {}).get(key[0], {}) or {}
        ).get("version_id"):
            problems.append(f"成员 {key} 的 adopted_version_id 与 supplier_versions 不一致")

    if len(member_keys) != len(members):
        return problems
    if len(set(member_keys)) != len(member_keys):
        problems.append("members 存在重复身份")
    if member_keys != sorted(member_keys):
        problems.append("members 未按 (supplier, product_id 数值) 排序")
    if _min_identity(member_keys) != group_key:
        problems.append("group_key 不是全体成员中的最小身份")

    versions = record.get("supplier_versions")
    if not isinstance(versions, dict):
        problems.append("缺少 supplier_versions")
        versions = {}
    for s in {k[0] for k in member_keys}:
        vb = versions.get(s)
        if not isinstance(vb, dict) or not vb.get("version_id"):
            problems.append(f"supplier_versions 缺少供应商 {s!r} 的版本锁定")

    # ---- 每个成员：在售完整行 或 可验算的明确缺席 -------------------------
    on_sale: list[dict[str, Any]] = []
    for m in members:
        key = _identity_key(m)
        status = m.get("listing_status")
        row = m.get("source_row")
        if status == "on_sale":
            if not isinstance(row, dict):
                problems.append(f"成员 {key} 标为 on_sale 但缺少 source_row")
                continue
            required = ("version_id", "product_id", "url", "price", "product_revision")
            missing = [f for f in required if f not in row]
            if missing:
                problems.append(f"成员 {key} source_row 缺字段 {missing}")
                continue
            if int(row["product_id"]) != key[1]:
                problems.append(f"成员 {key} source_row.product_id 不匹配")
            if row["version_id"] != m.get("adopted_version_id"):
                problems.append(f"成员 {key} source_row.version_id 不是锁定观察版")
            if not isinstance(row["price"], int) or isinstance(row["price"], bool):
                problems.append(f"成员 {key} source_row.price 不是整数")
            on_sale.append({"key": key, "row": row})
        elif status == "absent":
            ae = m.get("absence_evidence")
            if not isinstance(ae, dict):
                problems.append(f"成员 {key} 标为 absent 但缺少 absence_evidence")
                continue
            vb = versions.get(key[0], {})
            dlo = ae.get("domain", [None, None])[0]
            dhi = ae.get("domain", [None, None])[1]
            if not (isinstance(dlo, int) and isinstance(dhi, int)):
                problems.append(f"成员 {key} 缺席证明缺少 domain")
                continue
            if not (dlo <= key[1] < dhi):
                problems.append(f"成员 {key} 不在缺席证明域 [{dlo},{dhi}) 内")
            if (vb.get("domain_lo"), vb.get("domain_hi")) != (dlo, dhi):
                problems.append(f"成员 {key} 缺席域与 supplier_versions 不一致")
            if ae.get("product_within_domain") is not True:
                problems.append(f"成员 {key} 缺席证明未声明 product_within_domain=true")
            cov = vb.get("coverage")
            covered = pid_covered_by_segments(key[1], cov)
            if covered is False:
                problems.append(f"成员 {key} 不在覆盖证明的任何合并区间内，缺席不成立")
        else:
            problems.append(f"成员 {key} 的 listing_status 必须是 on_sale/absent")

    if not on_sale:
        problems.append("整组没有任何在售来源行，无法产生最低报价赢家")

    # ---- 声明与连接证据 ---------------------------------------------------
    claims = record.get("claims")
    if not isinstance(claims, list):
        problems.append("缺少 claims 数组")
        claims = []
    edges: list[dict[str, Any]] = []
    claim_ids: set[str] = set()
    for c in claims:
        if not isinstance(c, dict):
            problems.append("claims 中存在非对象元素")
            continue
        cid = c.get("claim_id")
        if not cid:
            problems.append("存在缺少 claim_id 的声明")
            continue
        if cid in claim_ids:
            problems.append(f"声明 {cid} 在组内重复列出")
        claim_ids.add(cid)
        if c.get("status_at_batch") != "active":
            problems.append(f"声明 {cid} 不是批次时点有效状态")
        try:
            a, b = _identity_key(c["a"]), _identity_key(c["b"])
        except (KeyError, TypeError, ValueError):
            problems.append(f"声明 {cid} 端点无法解析")
            continue
        if a not in member_keys or b not in member_keys:
            problems.append(f"声明 {cid} 的端点 {a}/{b} 超出组成员")
        edges.append({"claim_id": cid, "a": a, "b": b})

    reached = component_from_edges(group_key, edges)
    if reached != set(member_keys):
        problems.append(
            "由 claims 重建的连通分量与 members 不一致："
            f"缺少={sorted(set(member_keys) - reached)} 额外={sorted(reached - set(member_keys))}"
        )

    ce = record.get("connection_evidence")
    if not isinstance(ce, dict):
        problems.append("缺少 connection_evidence")
    else:
        if ce.get("claim_ids") is not None and list(ce["claim_ids"]) != sorted(claim_ids):
            problems.append("connection_evidence.claim_ids 与 claims 不一致（应排序且同集）")
        tree_ids = ce.get("connecting_claim_ids")
        tree_edges = ce.get("spanning_tree_edges")
        if not isinstance(tree_ids, list) or not isinstance(tree_edges, list):
            problems.append("connection_evidence 缺少生成树声明/边")
        else:
            tree_edge_tuples: list[tuple[str, tuple[str, int], tuple[str, int]]] = []
            for te in tree_edges:
                try:
                    tree_edge_tuples.append(
                        (te["claim_id"], _identity_key(te["a"]), _identity_key(te["b"]))
                    )
                except (KeyError, TypeError, ValueError):
                    problems.append("spanning_tree_edges 存在无法解析的边")
            if [t[0] for t in tree_edge_tuples] != tree_ids:
                problems.append("spanning_tree_edges 与 connecting_claim_ids 不一致")
            roots = {
                _find_root_local(v, tree_edge_tuples) for v in member_keys
            }
            if len(roots) != 1:
                problems.append("给出的 spanning tree 不能连通全部成员")

    # ---- 赢家：独立重算并核对完整来源行与批次记录 -------------------------
    winner = record.get("winner")
    if not isinstance(winner, dict):
        problems.append("缺少 winner 对象")
    elif on_sale:
        best = min(on_sale, key=lambda i: (i["row"]["price"], i["key"][0], i["key"][1]))
        best_key, best_row = best["key"], best["row"]
        try:
            winner_key = _identity_key(winner["identity"])
        except (KeyError, TypeError, ValueError):
            winner_key = None
            problems.append("winner.identity 无法解析")
        if winner_key is not None and winner_key != best_key:
            problems.append(
                f"独立重算赢家 {best_key} 与导出赢家 {winner_key} 不一致"
            )
        csr = winner.get("complete_source_row")
        if not isinstance(csr, dict):
            problems.append("winner 缺少 complete_source_row")
        elif csr != best_row:
            problems.append("winner.complete_source_row 与成员内嵌来源行不一致")
        expected_winner_values = {
            "supplier": best_key[0],
            "product_id": best_row["product_id"],
            "url": best_row["url"],
            "price": best_row["price"],
            "product_revision": best_row["product_revision"],
        }
        cr = winner.get("catalog_row")
        cat_rec = record.get("catalog_record_at_batch")
        if isinstance(cat_rec, dict):
            for f, expected in expected_winner_values.items():
                if cat_rec.get(f) != expected:
                    problems.append(
                        f"catalog_record_at_batch.{f} 与来源行不一致"
                    )
            try:
                if _identity_key(cat_rec.get("winning_identity", {})) != best_key:
                    problems.append("catalog_record_at_batch.winning_identity 与重算赢家不一致")
            except (KeyError, TypeError, ValueError):
                problems.append("catalog_record_at_batch.winning_identity 无法解析")
        if isinstance(cr, dict):
            for f, expected in expected_winner_values.items():
                if cr.get(f) != expected:
                    problems.append(f"winner.catalog_row.{f} 与来源行不一致")

    return problems


# --------------------------------------------------------------------------- #
# 包路径布局（目录包 或 x.jsonl + 旁车清单/标记）
# --------------------------------------------------------------------------- #

@dataclass(frozen=True, slots=True)
class PackageLayout:
    target: Path
    mode: str  # "dir" | "file"
    evidence: Path
    manifest: Path
    done: Path

    def marker_paths(self) -> list[Path]:
        paths = [self.done]
        if self.mode == "dir":
            paths.extend(self.target / name for name in SUCCESS_MARKER_ALIASES)
        else:
            stem = self.evidence.name[: -len(".jsonl")]
            paths.extend(
                self.evidence.with_name(stem + suffix)
                for suffix in DONE_SUFFIX_ALIASES
            )
        return paths

    def existing_marker(self) -> Path | None:
        for marker in self.marker_paths():
            if marker.is_file():
                return marker
        return None

    def is_complete(self) -> bool:
        return (
            self.evidence.is_file()
            and self.manifest.is_file()
            and self.existing_marker() is not None
        )

    def require_done(self) -> None:
        if self.existing_marker() is None:
            raise PackageIncomplete(
                f"审计来源包尚未完成（缺少 DONE 标记 {self.done}）；"
                "暂存/中断结果不能作为成品读取"
            )


def resolve_layout(target: str | os.PathLike[str]) -> PackageLayout:
    p = Path(target).expanduser()
    # 允许直接把 .../DONE 或某个旁车文件当作目标。
    if p.name == DONE_FILE_NAME and p.is_file():
        p = p.parent
    elif p.name == MANIFEST_FILE_NAME and p.is_file():
        p = p.parent
    name = p.name
    if name.endswith(DONE_SUFFIX):
        stem = name[: -len(DONE_SUFFIX)]
        p = p.with_name(stem + ".jsonl")
    elif name.endswith(MANIFEST_SUFFIX):
        stem = name[: -len(MANIFEST_SUFFIX)]
        p = p.with_name(stem + ".jsonl")
    if p.suffix == ".jsonl":
        return PackageLayout(
            target=p,
            mode="file",
            evidence=p,
            manifest=p.with_name(p.name[: -len(".jsonl")] + MANIFEST_SUFFIX),
            done=p.with_name(p.name[: -len(".jsonl")] + DONE_SUFFIX),
        )
    return PackageLayout(
        target=p,
        mode="dir",
        evidence=p / EVIDENCE_FILE_NAME,
        manifest=p / MANIFEST_FILE_NAME,
        done=p / DONE_FILE_NAME,
    )


def package_status(target: str | os.PathLike[str]) -> dict[str, Any]:
    """不抛异常地报告包状态：complete / incomplete / absent。"""
    layout = resolve_layout(target)
    if layout.is_complete():
        return {"status": "complete", "target": os.fspath(layout.target),
                "evidence": os.fspath(layout.evidence),
                "marker": os.fspath(layout.existing_marker())}  # type: ignore[arg-type]
    if layout.evidence.exists() or layout.manifest.exists() or any(
        m.exists() for m in layout.marker_paths()
    ):
        return {"status": "incomplete", "target": os.fspath(layout.target),
                "evidence": os.fspath(layout.evidence)}
    return {"status": "absent", "target": os.fspath(layout.target)}


# --------------------------------------------------------------------------- #
# 主导出
# --------------------------------------------------------------------------- #

class AuditSourcePackage:
    """导出并（重新）打开一个批次的审计历史来源包。

    典型用法::

        result = AuditSourcePackage.export("procurement.db", 1, "./audit-b1")
        report = AuditSourcePackage.verify("./audit-b1")
        for ev in AuditSourcePackage.iter_records("./audit-b1"):
            ...
    """

    def __init__(self, target: str | os.PathLike[str]):
        self.layout = resolve_layout(target)

    # ---- 导出 -------------------------------------------------------------
    @classmethod
    def export(
        cls,
        catalog_db: str | os.PathLike[str],
        batch_number: int | None = None,
        out: str | os.PathLike[str] | None = None,
        *,
        force: bool = False,
    ) -> dict[str, Any]:
        return export_package(catalog_db, batch_number, out, force=force)

    # ---- 读取/复核 --------------------------------------------------------
    @classmethod
    def verify(cls, target: str | os.PathLike[str]) -> dict[str, Any]:
        return verify_package(target)

    @classmethod
    def iter_records(cls, target: str | os.PathLike[str]) -> Iterator[dict[str, Any]]:
        return iter_package_records(target)

    @classmethod
    def read(cls, target: str | os.PathLike[str]) -> list[dict[str, Any]]:
        return read_package(target)

    def verify_self(self) -> dict[str, Any]:
        return verify_package(self.layout.target)

    def __iter__(self) -> Iterator[dict[str, Any]]:
        return iter_package_records(self.layout.target)


# 描述性别名。
AuditPackage = AuditSourcePackage
HistoricalSourcePackage = AuditSourcePackage
SourcePackage = AuditSourcePackage
AuditPackageBuilder = AuditSourcePackage
AuditTrailPackage = AuditSourcePackage
ProvenancePackage = AuditSourcePackage


class AuditExporter:
    """“先指定采购库、再多次导出不同批次”习惯的导出器。

    用法::

        exporter = AuditExporter("procurement.db")
        exporter.export(1, "./audit-b1")
    """

    def __init__(self, catalog_db: str | os.PathLike[str]):
        self.catalog_db = os.fspath(catalog_db)

    def export(
        self,
        batch_number: int | None = None,
        out: str | os.PathLike[str] | None = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        return export_package(self.catalog_db, batch_number, out, **kwargs)

    def export_batch(self, *a: Any, **kw: Any) -> dict[str, Any]:
        return self.export(*a, **kw)

    @staticmethod
    def verify(target: str | os.PathLike[str]) -> dict[str, Any]:
        return verify_package(target)

    @staticmethod
    def iter_records(target: str | os.PathLike[str]) -> Iterator[dict[str, Any]]:
        return iter_package_records(target)


def _default_out(catalog_db: str | os.PathLike[str], batch_number: int) -> Path:
    db = Path(catalog_db).expanduser().resolve()
    return db.parent / "audit-packages" / f"batch-{batch_number}"


def export_package(
    catalog_db: str | os.PathLike[str] | int | None = None,
    batch_number: int | str | None = None,
    out: str | os.PathLike[str] | None = None,
    *extra_args: Any,
    force: bool = False,
    **kwargs: Any,
) -> dict[str, Any]:
    """导出第 ``batch_number`` 个成功批次的审计来源包。

    ``out`` 为目录路径时生成目录包；以 ``.jsonl`` 结尾时生成单文件证据和
    同名 ``*.manifest.json`` / ``*.done`` 旁车标记。``batch_number=None``
    表示最新成功批次。任何严格校验失败都抛 :class:`AuditError` 子类，且不会
    在目标位置留下带 ``DONE`` 的成品。

    为兼容不同调用习惯，也接受 ``output/output_path/output_dir/dest/
    package_dir/path`` 作为输出位置关键字、``batch`` 作为批次号关键字，以及
    ``(batch_number, catalog_db, out)`` 的位置参数顺序。
    """
    # 兼容 (batch_number, catalog_db, out) 的位置顺序。
    if isinstance(catalog_db, int) and (
        batch_number is None or isinstance(batch_number, (str, os.PathLike))
    ):
        catalog_db, batch_number = batch_number, catalog_db  # type: ignore[assignment]
        if extra_args and out is None:
            out = extra_args[0]

    if batch_number is None and "batch" in kwargs:
        batch_number = kwargs.pop("batch")
    if out is None:
        for key in ("output", "output_path", "output_dir", "dest",
                    "package_dir", "path", "target"):
            value = kwargs.pop(key, None)
            if value is not None:
                out = value
                break
    if not force:
        force = bool(kwargs.pop("overwrite", False)
                     or kwargs.pop("force_overwrite", False))

    if catalog_db is None:
        raise AuditError("export_package 缺少 catalog_db 采购库路径")

    catalog_path = Path(catalog_db).expanduser()
    conn = _open_catalog_readonly(catalog_path)
    staging_dir: Path | None = None
    try:
        batch_number = resolve_batch_number(conn, batch_number)

        facts = reconstruct_batch_facts(conn, batch_number)
        group_count = replay_batch_snapshot(conn, batch_number)
        active_claim_count = build_active_claim_tables(conn, facts)

        layout = resolve_layout(out if out is not None else _default_out(catalog_path, batch_number))

        # 目标已存在成品时的保护。
        pre_status = package_status(layout.target)
        if pre_status["status"] == "complete" and not force:
            raise PackageError(
                f"审计包已存在且完整: {layout.target}；事实已锁定，"
                "如需重新生成请加 force=True/--force"
            )

        # 暂存目录必须是目标的同级隐藏目录（同一文件系统，最后可原子改名），
        # 绝不能位于最终目录内部。
        if layout.mode == "dir":
            staging_dir = layout.target.parent / (
                f".{layout.target.name}.building-{uuid.uuid4().hex[:8]}"
            )
        else:
            staging_dir = layout.evidence.parent / (
                f".{layout.evidence.name}.building-{uuid.uuid4().hex[:8]}"
            )
        staging_dir.parent.mkdir(parents=True, exist_ok=True)
        staging_dir.mkdir(parents=True, exist_ok=False)
        staging_evidence = staging_dir / EVIDENCE_FILE_NAME

        live_stats = {
            "groups": 0, "members": 0, "on_sale_members": 0,
            "absent_members": 0, "claims_listed": 0,
        }
        h = hashlib.sha256()
        evidence_bytes = 0

        with SourceProvider(catalog_path, facts) as provider, \
                staging_evidence.open("wb") as fh:
            cursor = conn.execute(
                "SELECT record_json FROM temp.cc_audit_replay "
                "ORDER BY group_supplier ASC, group_product_id ASC"
            )
            while True:
                rows = cursor.fetchmany(_READ_CHUNK)
                if not rows:
                    break
                for r in rows:
                    record = json.loads(r["record_json"])
                    evidence = build_group_evidence(conn, facts, provider, record)
                    line = _canonical_line(evidence)
                    fh.write(line)
                    h.update(line)
                    evidence_bytes += len(line)
                    live_stats["groups"] += 1
                    live_stats["members"] += len(evidence["members"])
                    live_stats["on_sale_members"] += sum(
                        1 for m in evidence["members"] if m["listing_status"] == "on_sale"
                    )
                    live_stats["absent_members"] += sum(
                        1 for m in evidence["members"] if m["listing_status"] == "absent"
                    )
                    live_stats["claims_listed"] += len(evidence["claims"])
            _fsync_fileobj(fh)

        if live_stats["groups"] != group_count:
            raise AuditConsistencyError(
                f"重放快照组数 {group_count} 与实际导出组数 {live_stats['groups']} 不一致"
            )

        evidence_sha = h.hexdigest()

        # ---- 落盘后全量独立复核（不相信导出时的内存结论）------------------
        verification = _verify_evidence_file(
            staging_evidence,
            expected_batch=batch_number,
            expected_groups=group_count,
            expected_sha256=evidence_sha,
        )
        if not verification["ok"]:
            raise PackageVerificationError(
                "导出证据未通过全量离线复核: "
                + "; ".join(verification["errors"][:10]),
                verification,
            )
        if verification["stats"] != live_stats:
            raise PackageVerificationError(
                f"复核统计 {verification['stats']} 与生成统计 {live_stats} 不一致"
            )

        manifest = {
            "package_type": "catpipe_audit_source_package",
            "package_schema": PACKAGE_SCHEMA,
            "record_type": RECORD_TYPE,
            "batch_number": batch_number,
            "batch_created_at": facts.created_at,
            "generated_at": utc_now(),
            "procurement_db": os.fspath(catalog_path.resolve()),
            "batch_input_sha256": facts.input_sha256,
            "batch_input": facts.input_obj,
            "batch_summary": facts.summary_obj,
            "evidence_file": EVIDENCE_FILE_NAME if layout.mode == "dir" else layout.evidence.name,
            "evidence_sha256": evidence_sha,
            "evidence_bytes": evidence_bytes,
            "group_count": group_count,
            "active_claim_count_at_batch": active_claim_count,
            "supplier_pins": [
                {
                    "supplier": p.supplier,
                    "version_id": p.version_id,
                    "selected_batch": p.selected_batch,
                    "selection_source": p.selection_source,
                    "db_path_recorded": p.db_path_recorded,
                    "db_path_table": p.db_path_table,
                }
                for p in sorted(facts.pins.values(), key=lambda x: x.supplier)
            ],
            "unresolved_claims": facts.unresolved_claims,
            "counts": verification["stats"],
            "verification": {
                "performed_before_completion_marker": True,
                "all_checks_passed": True,
                "records_checked": group_count,
                "problems": [],
            },
        }

        # ---- 清单先落盘 ---------------------------------------------------
        if layout.mode == "dir":
            staging_manifest = staging_dir / MANIFEST_FILE_NAME
            manifest_sha = _atomic_write_json(staging_manifest, manifest)
            staging_done_payload = {
                "marker": "catpipe-audit-source-package-done",
                "package_schema": PACKAGE_SCHEMA,
                "batch_number": batch_number,
                "completed_at": utc_now(),
                "manifest_file": MANIFEST_FILE_NAME,
                "manifest_sha256": manifest_sha,
                "evidence_file": EVIDENCE_FILE_NAME,
                "evidence_sha256": evidence_sha,
                "group_count": group_count,
            }
            staging_done = staging_dir / DONE_FILE_NAME
            done_sha = _atomic_write_json(staging_done, staging_done_payload)
            # 同义完成标记（_SUCCESS 约定），内容一致、同样在最后写入。
            for alias in SUCCESS_MARKER_ALIASES:
                _atomic_write_json(staging_dir / alias, staging_done_payload)
            # _atomic_write_json 已 fsync 父目录；再显式 fsync 暂存目录。
            _fsync_dir(staging_dir)

            final_dir = layout.target
            final_dir.parent.mkdir(parents=True, exist_ok=True)
            backup: Path | None = None
            if final_dir.exists():
                if force:
                    backup = final_dir.parent / f".{final_dir.name}.old-{uuid.uuid4().hex[:8]}"
                    os.replace(final_dir, backup)
                else:
                    try:
                        # 允许清理早期失败留下的空目录；非空（可能是未完成结果）
                        # 必须显式 --force，避免误删任何东西。
                        final_dir.rmdir()
                    except OSError as exc:
                        raise PackageError(
                            f"目标已存在且非空（或为未完成包）: {final_dir}；"
                            "确认后用 --force 重建"
                        ) from exc
            os.replace(staging_dir, final_dir)
            _fsync_dir(final_dir.parent)
            staging_dir = None
            if backup is not None:
                shutil.rmtree(backup, ignore_errors=True)
            done_path = layout.done
        else:
            # 文件模式：先把证据原子改名到最终 .jsonl，再写旁车清单与 DONE。
            os.replace(staging_evidence, layout.evidence)
            _fsync_dir(layout.evidence.parent)
            manifest_sha = _atomic_write_json(layout.manifest, manifest)
            done_payload = {
                "marker": "catpipe-audit-source-package-done",
                "package_schema": PACKAGE_SCHEMA,
                "batch_number": batch_number,
                "completed_at": utc_now(),
                "manifest_file": layout.manifest.name,
                "manifest_sha256": manifest_sha,
                "evidence_file": layout.evidence.name,
                "evidence_sha256": evidence_sha,
                "group_count": group_count,
            }
            done_sha = _atomic_write_json(layout.done, done_payload)
            stem = layout.evidence.name[: -len(".jsonl")]
            for suffix in DONE_SUFFIX_ALIASES:
                _atomic_write_json(
                    layout.evidence.with_name(stem + suffix), done_payload
                )
            shutil.rmtree(staging_dir, ignore_errors=True)
            staging_dir = None
            done_path = layout.done

        return {
            "status": "complete",
            "package": os.fspath(layout.target),
            "mode": layout.mode,
            "batch_number": batch_number,
            "group_count": group_count,
            "evidence": os.fspath(layout.evidence),
            "manifest": os.fspath(layout.manifest),
            "done": os.fspath(done_path),
            "evidence_sha256": evidence_sha,
            "manifest_sha256": manifest_sha,
            "done_sha256": done_sha,
            "counts": verification["stats"],
        }
    finally:
        conn.close()
        if staging_dir is not None and staging_dir.exists():
            shutil.rmtree(staging_dir, ignore_errors=True)


# 函数性别名。
def export_audit_package(*a: Any, **kw: Any) -> dict[str, Any]:
    return export_package(*a, **kw)


def export_source_package(*a: Any, **kw: Any) -> dict[str, Any]:
    return export_package(*a, **kw)


def build_audit_package(*a: Any, **kw: Any) -> dict[str, Any]:
    return export_package(*a, **kw)


def build_source_package(*a: Any, **kw: Any) -> dict[str, Any]:
    return export_package(*a, **kw)


def create_audit_package(*a: Any, **kw: Any) -> dict[str, Any]:
    return export_package(*a, **kw)


def export_audit_trail(*a: Any, **kw: Any) -> dict[str, Any]:
    return export_package(*a, **kw)


def export_history_package(*a: Any, **kw: Any) -> dict[str, Any]:
    return export_package(*a, **kw)


def export_provenance_package(*a: Any, **kw: Any) -> dict[str, Any]:
    return export_package(*a, **kw)


def build_provenance_package(*a: Any, **kw: Any) -> dict[str, Any]:
    return export_package(*a, **kw)


def export_evidence_package(*a: Any, **kw: Any) -> dict[str, Any]:
    return export_package(*a, **kw)


def write_audit_package(*a: Any, **kw: Any) -> dict[str, Any]:
    return export_package(*a, **kw)


# --------------------------------------------------------------------------- #
# 复核与读取已完成包（全新进程可用）
# --------------------------------------------------------------------------- #

def _verify_evidence_file(
    path: Path,
    *,
    expected_batch: int | None = None,
    expected_groups: int | None = None,
    expected_sha256: str | None = None,
) -> dict[str, Any]:
    errors: list[str] = []
    stats = {
        "groups": 0, "members": 0, "on_sale_members": 0,
        "absent_members": 0, "claims_listed": 0,
    }
    h = hashlib.sha256()
    size = 0
    with path.open("rb") as fh:
        for lineno, raw in enumerate(fh, start=1):
            h.update(raw)
            size += len(raw)
            try:
                rec = json.loads(raw.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                errors.append(f"第 {lineno} 行不是合法 JSON: {exc}")
                if len(errors) >= 50:
                    break
                continue
            if expected_batch is not None and isinstance(rec, dict) \
                    and rec.get("batch_number") != expected_batch:
                errors.append(
                    f"第 {lineno} 行 batch_number={rec.get('batch_number')} "
                    f"!= {expected_batch}"
                )
            problems = verify_evidence_record(rec)
            if problems:
                gid = rec.get("group_key") if isinstance(rec, dict) else None
                errors.append(f"第 {lineno} 行组 {gid} 复核失败: " + "; ".join(problems[:8]))
                if len(errors) >= 50:
                    break
                continue
            stats["groups"] += 1
            stats["members"] += len(rec["members"])
            stats["on_sale_members"] += sum(
                1 for m in rec["members"] if m["listing_status"] == "on_sale"
            )
            stats["absent_members"] += sum(
                1 for m in rec["members"] if m["listing_status"] == "absent"
            )
            stats["claims_listed"] += len(rec["claims"])

    digest = h.hexdigest()
    if expected_sha256 is not None and digest != expected_sha256:
        errors.append(f"evidence SHA-256 {digest} 与清单/预期 {expected_sha256} 不一致")
    if expected_groups is not None and stats["groups"] != expected_groups and not errors:
        errors.append(
            f"复核通过行数 {stats['groups']} 与预期组数 {expected_groups} 不一致"
        )
    return {
        "ok": not errors,
        "errors": errors,
        "stats": stats,
        "evidence_sha256": digest,
        "evidence_bytes": size,
    }


def verify_package(target: str | os.PathLike[str]) -> dict[str, Any]:
    """完整离线复核一个**已完成**的包（DONE → 清单哈希 → 证据哈希 → 逐行）。"""
    layout = resolve_layout(target)
    layout.require_done()
    if not layout.manifest.is_file():
        raise PackageError(f"包缺少内容清单: {layout.manifest}")
    if not layout.evidence.is_file():
        raise PackageError(f"包缺少证据文件: {layout.evidence}")

    marker_path = layout.existing_marker() or layout.done
    done_raw = marker_path.read_bytes()
    try:
        done_obj = json.loads(done_raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise PackageVerificationError(f"DONE 标记无法解析: {exc}") from exc

    manifest_raw = layout.manifest.read_bytes()
    manifest_sha = _sha256_bytes(manifest_raw)
    if done_obj.get("manifest_sha256") != manifest_sha:
        raise PackageVerificationError(
            "DONE 中的 manifest_sha256 与清单文件不一致（清单可能被改写）"
        )
    try:
        manifest = json.loads(manifest_raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise PackageVerificationError(f"manifest.json 无法解析: {exc}") from exc

    expected_evidence_name = manifest.get("evidence_file") or layout.evidence.name
    evidence_path = layout.evidence
    if layout.mode == "dir":
        evidence_path = layout.target / expected_evidence_name
        if evidence_path != layout.evidence and not evidence_path.is_file():
            evidence_path = layout.evidence

    evidence_sha, evidence_size = _sha256_file(evidence_path)
    errors: list[str] = []
    if manifest.get("evidence_sha256") != evidence_sha:
        errors.append("manifest.evidence_sha256 与证据文件不一致")
    if done_obj.get("evidence_sha256") not in (None, evidence_sha):
        errors.append("DONE.evidence_sha256 与证据文件不一致")
    if manifest.get("evidence_bytes") != evidence_size:
        errors.append(
            f"manifest.evidence_bytes={manifest.get('evidence_bytes')} "
            f"与实际 {evidence_size} 不一致"
        )

    result = _verify_evidence_file(
        evidence_path,
        expected_batch=manifest.get("batch_number"),
        expected_groups=manifest.get("group_count"),
        expected_sha256=manifest.get("evidence_sha256"),
    )
    errors.extend(result["errors"])

    report = {
        "ok": not errors,
        "status": "complete" if not errors else "failed",
        "package": os.fspath(layout.target),
        "mode": layout.mode,
        "batch_number": manifest.get("batch_number"),
        "group_count": manifest.get("group_count"),
        "stats": result["stats"],
        "evidence_sha256": evidence_sha,
        "evidence_bytes": evidence_size,
        "manifest_sha256": manifest_sha,
        "errors": errors,
    }
    if errors:
        raise PackageVerificationError(
            "审计包复核未通过: " + "; ".join(errors[:10]), report
        )
    return report


def iter_package_records(
    target: str | os.PathLike[str], *, require_complete: bool = True
) -> Iterator[dict[str, Any]]:
    """流式读取已完成包的证据记录；默认拒绝没有 DONE 的未完成结果。"""
    layout = resolve_layout(target)
    if require_complete:
        layout.require_done()
    if not layout.evidence.is_file():
        raise PackageError(f"找不到证据 JSONL: {layout.evidence}")
    with layout.evidence.open("r", encoding="utf-8") as fh:
        for lineno, line in enumerate(fh, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except json.JSONDecodeError as exc:
                raise PackageError(
                    f"证据文件第 {lineno} 行不是合法 JSON: {exc}"
                ) from exc


def iter_audit_records(
    target: str | os.PathLike[str], **kw: Any
) -> Iterator[dict[str, Any]]:
    return iter_package_records(target, **kw)


def read_audit_records(target: str | os.PathLike[str]) -> list[dict[str, Any]]:
    return read_package(target)


def read_package(target: str | os.PathLike[str]) -> list[dict[str, Any]]:
    """小目录便捷方法；大目录请用 :func:`iter_package_records`。"""
    return list(iter_package_records(target))


def verify_audit_package(target: str | os.PathLike[str]) -> dict[str, Any]:
    return verify_package(target)


# --------------------------------------------------------------------------- #
# 命令行
# --------------------------------------------------------------------------- #

def _emit(value: Any) -> None:
    print(json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True))


def _cli_export(args: Any) -> int:
    try:
        result = export_package(
            args.db, args.batch, args.out, force=args.force
        )
    except PackageVerificationError as exc:
        print(f"导出失败：全量复核未通过，未生成完成包。{exc}", file=sys.stderr)
        if exc.report:
            print(json.dumps(exc.report, ensure_ascii=False, indent=2)[:4000],
                  file=sys.stderr)
        return 3
    except AuditError as exc:
        print(f"导出失败，未产出完成包：{exc}", file=sys.stderr)
        return 2
    _emit(result)
    return 0


def _cli_verify(args: Any) -> int:
    try:
        report = verify_package(args.package)
    except PackageIncomplete as exc:
        print(f"包未完成，不能当作成品：{exc}", file=sys.stderr)
        return 4
    except PackageVerificationError as exc:
        _emit(exc.report or {"ok": False, "error": str(exc)})
        return 3
    except AuditError as exc:
        print(f"复核失败：{exc}", file=sys.stderr)
        return 2
    _emit(report)
    return 0


def _cli_status(args: Any) -> int:
    _emit(package_status(args.package))
    return 0


def _cli_records(args: Any) -> int:
    try:
        for rec in iter_package_records(args.package):
            if args.pretty:
                print(json.dumps(rec, ensure_ascii=False, indent=2, sort_keys=True))
            else:
                print(json.dumps(rec, ensure_ascii=False, sort_keys=True))
    except PackageIncomplete as exc:
        print(f"拒绝读取未完成结果：{exc}", file=sys.stderr)
        return 4
    return 0


def build_parser() -> Any:
    import argparse

    parser = argparse.ArgumentParser(
        prog="catpipe-audit",
        description="面向审计的批次历史来源包导出与离线复核",
    )
    sub = parser.add_subparsers(dest="audit_cmd", required=True)

    e = sub.add_parser("export", help="为成功批次导出 JSONL 证据来源包")
    e.add_argument("--db", required=True, help="采购端跨供应商 SQLite 库")
    e.add_argument("--batch", type=int, help="成功批次号；默认最新成功批次")
    e.add_argument(
        "--out",
        help="输出目录包路径；或以 .jsonl 结尾使用单文件+旁车清单/DONE。"
             "默认 <采购库目录>/audit-packages/batch-<N>",
    )
    e.add_argument("--force", action="store_true", help="覆盖已存在的包")
    e.set_defaults(func=_cli_export)

    v = sub.add_parser("verify", help="只凭导出物全量离线复核已完成的包")
    v.add_argument("--package", required=True, help="包目录、.jsonl 文件或 DONE 路径")
    v.set_defaults(func=_cli_verify)

    s = sub.add_parser("status", help="报告包状态 complete/incomplete/absent")
    s.add_argument("--package", required=True)
    s.set_defaults(func=_cli_status)

    r = sub.add_parser("records", help="流式打印包内证据记录（JSONL）")
    r.add_argument("--package", required=True)
    r.add_argument("--pretty", action="store_true", help="每条记录缩进美化（非 JSONL）")
    r.set_defaults(func=_cli_records)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    if argv is None:
        argv = sys.argv[1:]
    argv = list(argv)
    # 兼容不显式写 export 的自然形式：``audit --db x --batch 1 --out y``。
    known = {"export", "verify", "status", "records"}
    if argv and argv[0].startswith("-") and argv[0] not in ("-h", "--help"):
        argv = ["export", *argv]
    elif not argv:
        parser.print_help(sys.stderr)
        return 2
    elif argv[0] not in known and not argv[0].startswith("-"):
        # 未知子命令交给 argparse 报标准错误。
        pass
    args = parser.parse_args(argv)
    return int(args.func(args) or 0)


if __name__ == "__main__":
    raise SystemExit(main())
