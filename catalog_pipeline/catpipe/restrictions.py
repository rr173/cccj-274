"""按版本发布的供应商商品限制清单（召回/禁采通告）。

采购批次可以指定某份限制清单的一个**不可变版本**和一个**核算时点**
（``as_of`` 日期）。清单条目以 ``(供应商名, 稳定商品 ID)`` 标识商品，携带
记录编号与半开/闭区间形式的生效日期；只有在核算时点当天有效的条目才会
排除对应商品的报价。

本模块只负责：

* 从文件或内联 JSON 加载清单文档；
* 严格校验清单版本与条目（任何无效都抛 :class:`RestrictionError`，由采购批次
  统一回滚，不留下半批结果）；
* 归一化为字节稳定的规范文档（用于 SHA-256 指纹与重复导入一致性校验）；
* 提供“条目在某核算时点是否有效”的纯函数，供采购流水线与离线审计共用。

持久化与批次语义在 :mod:`catpipe.procurement`，审计证据在 :mod:`catpipe.audit`。
"""

from __future__ import annotations

import json
import os
import sys
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Sequence


class RestrictionError(ValueError):
    """限制清单相关输入无效的基础异常。

    :class:`catpipe.procurement.InvalidBatch` 把它作为父类，因此采购批次
    捕获 ``InvalidBatch`` 时同时覆盖限制清单校验失败，统一回滚整批。
    """


# 兼容采购侧的异常命名习惯。
InvalidRestriction = RestrictionError

__all__ = [
    "RestrictionSelection",
    "RestrictionContext",
    "RestrictionError",
    "InvalidRestriction",
    "load_restriction_selection",
    "restriction_selection_header",
    "validate_restriction_document",
    "canonical_document",
    "canonical_document_bytes",
    "document_sha256",
    "parse_calendar_date",
    "parse_product_id",
    "is_effective",
    "intervals_overlap",
    "norm_key",
    "get_field",
    "RESTRICTION_BATCH_KEYS",
]

# 批次 JSON 中可用于携带限制清单选择的字段名（归一化后匹配）。
RESTRICTION_BATCH_KEYS = (
    "restriction_list",
    "restrictionlist",
    "restrictions",
    "restriction",
    "product_restrictions",
    "productrestrictions",
    "blocklist",
    "block_list",
    "denylist",
    "deny_list",
    "recall_list",
    "recallist",
    "purchase_ban_list",
)

_LIST_ID_FIELDS = (
    "list_id", "list", "restriction_list_id", "document_id", "name",
    "registry_id", "ban_list_id",
)
_VERSION_FIELDS = (
    "version", "version_id", "list_version", "revision", "rev", "edition",
)
_ENTRY_FIELDS = (
    "entries", "items", "records", "restrictions", "notices", "announcements",
)
_RECORD_ID_FIELDS = (
    "record_id", "record_number", "notice_id", "announcement_id",
    "restriction_id", "ban_id", "recall_id", "case_id", "id",
)
_FROM_FIELDS = (
    "effective_from", "from", "start", "start_date", "effective_start",
    "valid_from", "in_force_from",
)
_TO_FIELDS = (
    "effective_to", "to", "end", "end_date", "effective_end",
    "valid_to", "in_force_to", "until",
)
_AS_OF_FIELDS = (
    "as_of", "accounting_date", "evaluation_date", "effective_date",
    "point_in_time", "at", "on_date", "decision_date", "as_of_date",
)
_DOCUMENT_FIELDS = ("document", "content", "payload", "list_document", "data")
_PATH_FIELDS = (
    "path", "file", "file_path", "json_path", "source_path", "document_path",
    "src", "filename",
)
_OPEN_ENDED = (None, "", "none", "null", "open", "open_ended")


def norm_key(key: Any) -> str:
    return "".join(ch for ch in str(key).lower() if ch not in "_- ")


def get_field(obj: dict[str, Any], names: Sequence[str]) -> Any:
    """与采购侧字段解析同规则的宽松取值（忽略下划线/连字符/大小写）。"""
    if not isinstance(obj, dict):
        return None
    normalized = {norm_key(k): v for k, v in obj.items()}
    for name in names:
        key = norm_key(name)
        if key in normalized and normalized[key] is not None:
            return normalized[key]
    return None


def parse_product_id(value: Any) -> int:
    """把字符串/整数等归一化为非负数值稳定商品 ID。"""
    if isinstance(value, bool):
        raise RestrictionError("product_id 不能是布尔值")
    if isinstance(value, int):
        pid = value
    elif isinstance(value, str) and value.strip():
        try:
            pid = int(value.strip())
        except ValueError as exc:
            raise RestrictionError(
                f"无法解析数值 product_id: {value!r}"
            ) from exc
    elif isinstance(value, float) and value.is_integer():
        pid = int(value)
    else:
        raise RestrictionError(f"无法解析数值 product_id: {value!r}")
    if pid < 0:
        raise RestrictionError("product_id 不能为负数")
    return pid


@dataclass(frozen=True, slots=True)
class RestrictionSelection:
    """批次对限制清单版本与核算时点的一次显式选择。"""

    list_id: str
    version: str
    as_of: date
    document: dict[str, Any]
    source_path: str | None = None
    clear: bool = False


@dataclass(frozen=True, slots=True)
class RestrictionContext:
    """批次实际采用的限制上下文（显式选择或沿用上一批）。"""

    list_id: str
    version: str
    as_of: date
    document_sha256: str

    def as_dict(self) -> dict[str, Any]:
        return {
            "list_id": self.list_id,
            "version": self.version,
            "as_of": self.as_of.isoformat(),
            "document_sha256": self.document_sha256,
        }


# --------------------------------------------------------------------------- #
# 日期与区间
# --------------------------------------------------------------------------- #

def parse_calendar_date(value: Any, *, what: str) -> date:
    """严格解析 ``YYYY-MM-DD`` 日历日期；拒绝时间分量、非格里高利值。"""
    if isinstance(value, date) and not isinstance(value, bool):
        return value
    if isinstance(value, bool) or not isinstance(value, str):
        raise RestrictionError(f"{what} 必须是 YYYY-MM-DD 字符串: {value!r}")
    text = value.strip()
    if len(text) != 10 or text[4] != "-" or text[7] != "-":
        raise RestrictionError(f"{what} 必须是 YYYY-MM-DD 字符串: {value!r}")
    try:
        return date.fromisoformat(text)
    except ValueError as exc:
        raise RestrictionError(f"{what} 不是有效日历日期: {value!r} ({exc})") from exc


def _parse_end_date(value: Any, *, what: str) -> date | None:
    """生效上限；None/空/字面 null 表示开放区间（长期有效）。"""
    if isinstance(value, str):
        stripped = value.strip()
        if stripped.lower() in _OPEN_ENDED:
            return None
    elif value is None:
        return None
    return parse_calendar_date(value, what=what)


def intervals_overlap(
    a_from: date, a_to: date | None, b_from: date, b_to: date | None
) -> bool:
    """两个闭区间是否重叠；``None`` 上限视为无限远。相邻区间不算重叠。"""
    a_end = a_to or date.max
    b_end = b_to or date.max
    return a_from <= b_end and b_from <= a_end


def is_effective(entry: dict[str, Any], as_of: date) -> bool:
    """条目在核算时点当天是否有效（闭区间；上限为空表示长期有效）。"""
    eff_from = entry["effective_from"]
    eff_to = entry["effective_to"]
    if isinstance(eff_from, str):
        eff_from = date.fromisoformat(eff_from)
    if isinstance(eff_to, str):
        eff_to = date.fromisoformat(eff_to)
    if as_of < eff_from:
        return False
    return eff_to is None or as_of <= eff_to


# --------------------------------------------------------------------------- #
# 文档校验与归一化
# --------------------------------------------------------------------------- #

def _norm_entry(raw: Any, index: int) -> dict[str, Any]:
    where = f"限制清单条目[{index}]"
    if not isinstance(raw, dict):
        raise RestrictionError(f"{where} 必须是 JSON 对象: {raw!r}")

    record_id = get_field(raw, _RECORD_ID_FIELDS)
    if record_id is None or str(record_id).strip() == "":
        raise RestrictionError(f"{where} 缺少非空记录编号 record_id")
    record_id = str(record_id).strip()

    supplier = get_field(raw, (
        "supplier", "supplier_name", "vendor", "vendor_name", "seller", "name",
    ))
    if supplier is None or str(supplier).strip() == "":
        raise RestrictionError(f"{where}（记录 {record_id!r}）缺少非空 supplier")
    supplier = str(supplier).strip()

    pid_value = get_field(raw, (
        "product_id", "stable_product_id", "stable_id", "id", "pid", "productid",
    ))
    if pid_value is None:
        raise RestrictionError(f"{where}（记录 {record_id!r}）缺少 product_id")
    product_id = parse_product_id(pid_value)

    eff_from = parse_calendar_date(
        get_field(raw, _FROM_FIELDS), what=f"{where}（记录 {record_id!r}）effective_from"
    )
    eff_to = _parse_end_date(
        get_field(raw, _TO_FIELDS), what=f"{where}（记录 {record_id!r}）effective_to"
    )
    if eff_to is not None and eff_to < eff_from:
        raise RestrictionError(
            f"{where}（记录 {record_id!r}）生效区间倒置: "
            f"{eff_from.isoformat()} > {eff_to.isoformat()}"
        )

    return {
        "record_id": record_id,
        "supplier": supplier,
        "product_id": product_id,
        "effective_from": eff_from.isoformat(),
        "effective_to": eff_to.isoformat() if eff_to is not None else None,
    }


def validate_restriction_document(
    document: Any,
    *,
    expected_list_id: str | None = None,
    expected_version: str | None = None,
) -> dict[str, Any]:
    """校验清单文档并返回归一化规范文档；任何无效都抛 :class:`RestrictionError`。"""
    if not isinstance(document, dict):
        raise RestrictionError("限制清单文档必须是 JSON 对象")

    list_id = get_field(document, _LIST_ID_FIELDS)
    if list_id is None or str(list_id).strip() == "":
        raise RestrictionError("限制清单文档缺少非空 list_id")
    list_id = str(list_id).strip()

    version = get_field(document, _VERSION_FIELDS)
    if version is None or str(version).strip() == "":
        raise RestrictionError(f"限制清单 {list_id!r} 的文档缺少非空 version")
    version = str(version).strip()

    if expected_list_id is not None and list_id != str(expected_list_id).strip():
        raise RestrictionError(
            f"清单文档 list_id={list_id!r} 与批次指定的 "
            f"{str(expected_list_id).strip()!r} 不一致"
        )
    if expected_version is not None and version != str(expected_version).strip():
        raise RestrictionError(
            f"清单文档 version={version!r} 与批次指定的 "
            f"{str(expected_version).strip()!r} 不一致"
        )

    raw_entries = get_field(document, _ENTRY_FIELDS)
    if raw_entries is None:
        raise RestrictionError(f"限制清单 {list_id!r}/{version!r} 缺少 entries 数组")
    if not isinstance(raw_entries, (list, tuple)):
        raise RestrictionError(
            f"限制清单 {list_id!r}/{version!r} 的 entries 必须是数组"
        )

    entries = [_norm_entry(raw, i) for i, raw in enumerate(raw_entries)]

    seen_records: set[str] = set()
    for entry in entries:
        if entry["record_id"] in seen_records:
            raise RestrictionError(
                f"限制清单 {list_id!r}/{version!r} 记录编号重复: "
                f"{entry['record_id']!r}"
            )
        seen_records.add(entry["record_id"])

    # 同一商品在同一清单版本内被多个条目限定时，区间不得重叠：否则核算时点
    # 无法给出唯一、确定的“有效限制依据”。相邻接续区间是允许的。
    by_identity: dict[tuple[str, int], list[dict[str, Any]]] = {}
    for entry in entries:
        by_identity.setdefault(
            (entry["supplier"], entry["product_id"]), []
        ).append(entry)
    for identity, group in by_identity.items():
        if len(group) < 2:
            continue
        ordered = sorted(group, key=lambda e: (e["effective_from"], e["record_id"]))
        for earlier, later in zip(ordered, ordered[1:]):
            a_from = date.fromisoformat(earlier["effective_from"])
            a_to = (
                date.fromisoformat(earlier["effective_to"])
                if earlier["effective_to"] is not None else None
            )
            b_from = date.fromisoformat(later["effective_from"])
            b_to = (
                date.fromisoformat(later["effective_to"])
                if later["effective_to"] is not None else None
            )
            if intervals_overlap(a_from, a_to, b_from, b_to):
                raise RestrictionError(
                    f"限制清单 {list_id!r}/{version!r} 中商品 {identity} 的条目 "
                    f"{earlier['record_id']!r} 与 {later['record_id']!r} 生效区间重叠，"
                    "无法确定唯一有效限制依据"
                )

    return canonical_document(list_id, version, entries)


def canonical_document(
    list_id: str, version: str, entries: Sequence[dict[str, Any]]
) -> dict[str, Any]:
    """构造字节稳定的规范文档（键排序、条目按记录编号排序）。"""
    normalized_entries = sorted(
        (
            {
                "record_id": str(e["record_id"]),
                "supplier": str(e["supplier"]),
                "product_id": int(e["product_id"]),
                "effective_from": str(e["effective_from"]),
                "effective_to": (
                    None if e["effective_to"] is None else str(e["effective_to"])
                ),
            }
            for e in entries
        ),
        key=lambda e: e["record_id"],
    )
    return {
        "list_id": str(list_id),
        "version": str(version),
        "entries": normalized_entries,
    }


def canonical_document_bytes(document: dict[str, Any]) -> bytes:
    return (
        json.dumps(document, ensure_ascii=False, sort_keys=True,
                   separators=(",", ":"))
        + "\n"
    ).encode("utf-8")


def document_sha256(document: dict[str, Any]) -> str:
    import hashlib

    return hashlib.sha256(canonical_document_bytes(document)).hexdigest()


# --------------------------------------------------------------------------- #
# 批次选择解析
# --------------------------------------------------------------------------- #

def _extract_restriction_field(batch: dict[str, Any]) -> tuple[bool, Any]:
    """返回 ``(字段是否出现, 原始值)``，以区分“显式 null”与“未提供”。"""
    for key in RESTRICTION_BATCH_KEYS:
        if key in batch:
            return True, batch[key]
    # 兼容下划线/连字符差异（get_field 的归一化规则）。
    normalized = {
        "".join(ch for ch in str(k).lower() if ch not in "_- "): v
        for k, v in batch.items()
    }
    for key in RESTRICTION_BATCH_KEYS:
        norm = "".join(ch for ch in key.lower() if ch not in "_- ")
        if norm in normalized:
            return True, normalized[norm]
    return False, None


def _load_document_payload(value: Any, *, source_path: str | None) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        path = Path(value).expanduser()
        try:
            with path.open("r", encoding="utf-8") as fh:
                payload = json.load(fh)
        except (OSError, json.JSONDecodeError) as exc:
            raise RestrictionError(f"无法读取限制清单文件 {path}: {exc}") from exc
        if not isinstance(payload, dict):
            raise RestrictionError(f"限制清单文件 {path} 内容必须是 JSON 对象")
        return payload
    raise RestrictionError(f"无法解析限制清单文档: {value!r}")


def restriction_selection_header(
    batch: dict[str, Any],
) -> tuple[str, str | None, str | None, str | None]:
    """只解析批次输入中的限制选择头，不打开任何清单文件。

    供审计重放交叉校验使用，返回 ``(kind, list_id, version, as_of)``：

    * ``("absent", None, None, None)``：批次未提及限制清单；
    * ``("clear", None, None, None)``：批次显式给出 null/false；
    * ``("select", list_id, version, as_of_iso)``：显式选择。
    """
    present, raw = _extract_restriction_field(batch)
    if not present:
        return "absent", None, None, None
    if raw is None or raw is False or (
        isinstance(raw, str) and raw.strip().lower() in ("none", "null", "off", "")
    ):
        return "clear", None, None, None
    if not isinstance(raw, dict):
        raise RestrictionError(f"限制清单选择必须是对象或 null: {raw!r}")
    list_id = get_field(raw, _LIST_ID_FIELDS)
    if list_id is None or str(list_id).strip() == "":
        raise RestrictionError("限制清单选择缺少非空 list_id")
    version = get_field(raw, _VERSION_FIELDS)
    if version is None or str(version).strip() == "":
        raise RestrictionError(f"限制清单 {list_id!r} 的批次选择缺少非空 version")
    as_of_value = get_field(raw, _AS_OF_FIELDS)
    if as_of_value is None:
        raise RestrictionError(
            f"限制清单 {list_id!r}/{version!r} 的批次选择缺少核算时点 as_of"
        )
    as_of = parse_calendar_date(as_of_value, what="限制清单核算时点 as_of")
    return "select", str(list_id).strip(), str(version).strip(), as_of.isoformat()


def load_restriction_selection(batch: dict[str, Any]) -> RestrictionSelection | None:
    """从批次 JSON 解析限制清单选择。

    * 返回 :data:`None`：批次未提及限制清单（沿用上一批上下文，或从无限制）；
    * ``clear=True``：批次显式给出 null/false，本批不应用任何限制；
    * 否则返回带规范文档与核算时点的选择。
    """
    present, raw = _extract_restriction_field(batch)
    if not present:
        return None
    if raw is None or raw is False or (
        isinstance(raw, str) and raw.strip().lower() in ("none", "null", "off", "")
    ):
        return RestrictionSelection(
            list_id="", version="", as_of=date.min, document={}, clear=True
        )
    if not isinstance(raw, dict):
        raise RestrictionError(f"限制清单选择必须是对象或 null: {raw!r}")

    list_id = get_field(raw, _LIST_ID_FIELDS)
    if list_id is None or str(list_id).strip() == "":
        raise RestrictionError("限制清单选择缺少非空 list_id")
    list_id = str(list_id).strip()

    version = get_field(raw, _VERSION_FIELDS)
    if version is None or str(version).strip() == "":
        raise RestrictionError(f"限制清单 {list_id!r} 的批次选择缺少非空 version")
    version = str(version).strip()

    as_of_value = get_field(raw, _AS_OF_FIELDS)
    if as_of_value is None:
        raise RestrictionError(
            f"限制清单 {list_id!r}/{version!r} 的批次选择缺少核算时点 as_of"
        )
    as_of = parse_calendar_date(as_of_value, what="限制清单核算时点 as_of")

    source_path: str | None = None
    path_value = get_field(raw, _PATH_FIELDS)
    inline = get_field(raw, _DOCUMENT_FIELDS)
    if path_value is not None:
        source_path = str(path_value)
        document = _load_document_payload(source_path, source_path=source_path)
        source_path = os.fspath(Path(source_path).expanduser().resolve())
    elif inline is not None:
        document = _load_document_payload(inline, source_path=None)
    else:
        raise RestrictionError(
            f"限制清单 {list_id!r}/{version!r} 的批次选择必须提供文件 path "
            "或内联 document"
        )

    canonical = validate_restriction_document(
        document, expected_list_id=list_id, expected_version=version
    )
    return RestrictionSelection(
        list_id=list_id,
        version=version,
        as_of=as_of,
        document=canonical,
        source_path=source_path,
    )
