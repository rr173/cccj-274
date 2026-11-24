"""线路协议、摘要工具与公共异常。

线格式为 JSONL（每行一个 JSON 对象），可逐条流式落盘，
响应与商品规模都可以远超内存，采集端不会把整页读进内存。

页声明（半开 ID 范围 [lo, hi)）::

    {"type": "page", "version_id": "v1", "token": "...",
     "page_id": "p1", "revision": 3,
     "range": {"lo": 100, "hi": 200},
     "domain": {"lo": 0, "hi": 1000}}

商品行（页内跟随在页声明之后）::

    {"type": "product", "product_id": 101,
     "url": "https://x/101", "price": 990, "product_revision": 2}

结束行::

    {"type": "end", "count": 7, "checksum": "..."}

校验和只用于传输层完整性；业务层的"同号同内容"判定使用
:func:`page_content_hash`，对页内商品按 product_id 排序后取摘要，
因此同内容商品行乱序重送仍判定为无操作重送。
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Any, BinaryIO, Iterator

# ---- 传输层 ----------------------------------------------------------------

LINE_ENCODING = "utf-8"


def dump_jsonl_line(obj: dict[str, Any]) -> bytes:
    """把一个对象编码为一行 JSONL（含换行）。"""
    return (json.dumps(obj, ensure_ascii=False, sort_keys=True) + "\n").encode(LINE_ENCODING)


def iter_jsonl(stream: BinaryIO) -> Iterator[dict[str, Any]]:
    """逐行解析 JSONL；空行跳过，坏行抛 :class:`ProtocolError`。"""
    for lineno, raw in enumerate(stream, start=1):
        if not raw.strip():
            continue
        try:
            obj = json.loads(raw.decode(LINE_ENCODING))
        except (ValueError, UnicodeDecodeError) as exc:
            raise ProtocolError(f"第 {lineno} 行不是合法 JSON: {exc}") from exc
        if not isinstance(obj, dict):
            raise ProtocolError(f"第 {lineno} 行不是 JSON 对象")
        yield obj


def stream_checksum() -> Any:
    """传输层流式 SHA-256 计算器。"""
    return hashlib.sha256()


# ---- 业务摘要 --------------------------------------------------------------


def _canonical_product(product: dict[str, Any]) -> str:
    """单商品的规范化文本（稳定字段：ID/URL/价格/商品修订）。"""
    return json.dumps(
        {
            "product_id": int(product["product_id"]),
            "url": str(product["url"]),
            "price": int(product["price"]),
            "product_revision": int(product["product_revision"]),
        },
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def page_content_hash_from_sorted(
    page_id: str,
    revision: int,
    range_lo: int,
    range_hi: int,
    sorted_rows: Any,
) -> str:
    """流式内容摘要：调用方保证 ``sorted_rows`` 已按 product_id 升序排列。

    与 :func:`page_content_hash` 摘要结果完全一致，但不缓存任何行，
    供存储层用 ``ORDER BY product_id`` 的游标逐行喂入，页大小可远超内存。
    每行可为 dict 或 ``(product_id, url, price, product_revision)`` 元组。
    """
    h = hashlib.sha256()
    h.update(
        json.dumps(
            {
                "page_id": page_id,
                "revision": int(revision),
                "lo": int(range_lo),
                "hi": int(range_hi),
            },
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    )
    for item in sorted_rows:
        if isinstance(item, dict):
            pid, url, price, prev = (
                int(item["product_id"]), str(item["url"]),
                int(item["price"]), int(item["product_revision"]),
            )
        else:
            pid, url, price, prev = item
        h.update(
            json.dumps(
                {
                    "product_id": pid,
                    "url": url,
                    "price": price,
                    "product_revision": prev,
                },
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
        )
        h.update(b"\n")
    return h.hexdigest()


def page_content_hash(
    page_id: str,
    revision: int,
    range_lo: int,
    range_hi: int,
    products: list[dict[str, Any]] | Iterator[tuple[int, str, int, int]],
) -> str:
    """页内容摘要。

    入参可以是商品字典列表，也可以是 ``(product_id, url, price,
    product_revision)`` 元组流（存储层重算时使用，无需重建原文）。

    摘要覆盖：页 ID、修订号、半开范围、按 product_id 排序后的全部商品。
    商品行输入顺序不影响结果。
    """
    rows: list[tuple[int, str, int, int]] = []
    if products:
        for item in products:
            if isinstance(item, dict):
                rows.append(
                    (
                        int(item["product_id"]),
                        str(item["url"]),
                        int(item["price"]),
                        int(item["product_revision"]),
                    )
                )
            else:
                rows.append(item)  # type: ignore[arg-type]
    rows.sort(key=lambda r: r[0])
    return page_content_hash_from_sorted(
        page_id, revision, range_lo, range_hi, rows,
    )


# ---- 数据对象 --------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class PageHeader:
    version_id: str
    token: str
    page_id: str
    revision: int
    range_lo: int
    range_hi: int
    domain_lo: int
    domain_hi: int

    @classmethod
    def from_obj(cls, obj: dict[str, Any]) -> "PageHeader":
        try:
            rng = obj["range"]
            dom = obj["domain"]
            return cls(
                version_id=str(obj["version_id"]),
                token=str(obj["token"]),
                page_id=str(obj["page_id"]),
                revision=int(obj["revision"]),
                range_lo=int(rng["lo"]),
                range_hi=int(rng["hi"]),
                domain_lo=int(dom["lo"]),
                domain_hi=int(dom["hi"]),
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise ProtocolError(f"页声明字段缺失或类型错误: {exc}") from exc


@dataclass(frozen=True, slots=True)
class ProductRow:
    product_id: int
    url: str
    price: int
    product_revision: int

    @classmethod
    def from_obj(cls, obj: dict[str, Any]) -> "ProductRow":
        try:
            return cls(
                product_id=int(obj["product_id"]),
                url=str(obj["url"]),
                price=int(obj["price"]),
                product_revision=int(obj["product_revision"]),
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise ProtocolError(f"商品行字段缺失或类型错误: {exc}") from exc


# ---- 异常 ------------------------------------------------------------------


class CatpipeError(Exception):
    """所有 catpipe 业务异常的基类。"""


class ProtocolError(CatpipeError):
    """线格式/传输层错误。"""


class RejectedError(CatpipeError):
    """整次接收被拒绝；候选区保持原状（已在事务内回滚）。

    reason_code 供日志与隔离区归类。
    """

    def __init__(self, reason_code: str, message: str):
        super().__init__(f"[{reason_code}] {message}")
        self.reason_code = reason_code
        self.message = message


class VersionInvalid(RejectedError):
    """观察版或令牌失效：必须放弃整轮，不得用旧轮补洞。"""

    def __init__(self, message: str = "观察版或令牌已失效"):
        super().__init__("version_invalid", message)


class NotPublishable(CatpipeError):
    """当前候选尚不能发布（覆盖缺口或存在矛盾）。"""


class NotFound(CatpipeError):
    """请求的版本/资源不存在。"""
