"""采集器：从接口模拟器（或兼容供应商接口）流式拉取页并驱动接收引擎。

保证
====
* ``urlopen(stream=True)`` + 逐行读取 + 引擎逐行落盘/入暂存，
  响应与商品规模可远超内存。
* 接口允许页乱序与重送：按 manifest 顺序逐页拉取，重送到引擎
  自然走"同号同内容无操作"；乱序由引擎的集合校验兜底。
* 收到 401（观察版/令牌失效）立即 :meth:`Repository.abandon_version`
  放弃整轮，绝不用旧轮数据补洞，然后以新令牌重新开轮。
* 每轮拉取后尝试发布；发布失败（缺口）时旧已发布版仍完整可读，
  下一轮继续补页。
"""

from __future__ import annotations

import json
import time
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import urlencode

from .ingest import IngestionEngine, Receipt
from .protocol import NotPublishable, RejectedError, VersionInvalid
from .publisher import coverage_status, publish
from .repository import Repository


@dataclass
class CollectReport:
    rounds: int = 0
    accepted: int = 0
    replaced: int = 0
    noop: int = 0
    rejected: list[dict[str, Any]] = field(default_factory=list)
    abandoned: list[dict[str, Any]] = field(default_factory=list)
    published: list[dict[str, Any]] = field(default_factory=list)
    publish_blocked: list[dict[str, Any]] = field(default_factory=list)
    network_errors: list[str] = field(default_factory=list)


class Collector:
    def __init__(self, base_url: str, repo: Repository, *,
                 token: str | None = None, timeout: float = 10.0,
                 max_retries: int = 2, verbose: bool = True):
        self.base_url = base_url.rstrip("/")
        self.repo = repo
        self.engine = IngestionEngine(repo)
        self.token = token
        self.timeout = timeout
        self.max_retries = max_retries
        self.verbose = verbose

    def log(self, msg: str) -> None:
        if self.verbose:
            print(f"[collector] {msg}")

    # ---- 低层 HTTP --------------------------------------------------------

    def _get(self, path: str, params: dict[str, str] | None = None) -> Any:
        url = f"{self.base_url}{path}"
        if params:
            url = f"{url}?{urlencode(params)}"
        req = urllib.request.Request(url, headers={"Accept": "application/json"})
        with urllib.request.urlopen(req, timeout=self.timeout) as resp:
            return json.loads(resp.read().decode("utf-8"))

    def _open_page_stream(self, page_id: str, token: str):
        url = f"{self.base_url}/pages/{page_id}?{urlencode({'token': token})}"
        req = urllib.request.Request(url, headers={"Accept": "application/x-ndjson"})
        return urllib.request.urlopen(req, timeout=self.timeout)

    def fetch_manifest(self, token: str) -> dict[str, Any]:
        try:
            return self._get("/manifest", {"token": token})
        except urllib.error.HTTPError as exc:
            if exc.code == 401:
                raise VersionInvalid("manifest 拉取被拒：观察版或令牌失效") from exc
            raise

    def fetch_page(self, page_id: str, token: str) -> Receipt:
        """流式拉取单页并交给接收引擎；网络错误重试，语义拒绝不重试。"""
        last_exc: Exception | None = None
        for attempt in range(self.max_retries + 1):
            try:
                with self._open_page_stream(page_id, token) as stream:
                    return self.engine.ingest_stream(stream)
            except urllib.error.HTTPError as exc:
                if exc.code == 401:
                    raise VersionInvalid(f"页 {page_id} 拉取被拒：令牌失效") from exc
                last_exc = exc
            except (urllib.error.URLError, TimeoutError, OSError) as exc:
                last_exc = exc
            if attempt < self.max_retries:
                time.sleep(0.1 * (attempt + 1))
        raise RejectedError("network_error", f"页 {page_id} 拉取失败: {last_exc}")

    # ---- 单轮/循环 ---------------------------------------------------------

    def run_once(self, report: CollectReport | None = None) -> CollectReport:
        """拉取当前观察版 manifest 的全部页一次，并尝试发布。"""
        report = report or CollectReport()
        if not self.token:
            raise VersionInvalid("尚未持有有效令牌")
        try:
            manifest = self.fetch_manifest(self.token)
        except VersionInvalid:
            raise

        vid = manifest["version_id"]
        report.rounds += 1
        self.log(f"观察版 {vid}，manifest 含 {len(manifest['pages'])} 页")

        for spec in manifest["pages"]:
            pid = spec["page_id"]
            known = self.repo.get_page(vid, pid)
            if known is not None and known["revision"] >= spec["revision"]:
                self.log(f"  跳过 {pid}（本地修订 {known['revision']} >= 远端 {spec['revision']}）")
                continue
            try:
                receipt = self.fetch_page(pid, self.token)
                if receipt.outcome == "accepted":
                    report.accepted += 1
                elif receipt.outcome == "replaced":
                    report.replaced += 1
                else:
                    report.noop += 1
                self.log(f"  {pid} rev{receipt.revision}: {receipt.outcome} ({receipt.detail})")
            except VersionInvalid:
                raise
            except RejectedError as exc:
                report.rejected.append({"page_id": pid, "reason": exc.reason_code,
                                        "detail": exc.message})
                self.log(f"  {pid} 拒绝接收: {exc.message}；原始响应已隔离，候选不变")

        # 尝试发布；缺口是正常的"移窗未补齐"状态，不算错误。
        try:
            summary = publish(self.repo, vid)
            report.published.append({
                "version_id": vid,
                "product_count": summary["product_count"],
                "diff_counts": summary["diff_counts"],
            })
            self.log(f"  发布成功 {vid}: {summary['diff_counts']}")
        except NotPublishable as exc:
            status = coverage_status(self.repo, vid)
            report.publish_blocked.append({
                "version_id": vid,
                "reason": str(exc),
                "gaps": status["gaps"],
            })
            self.log(f"  暂不可发布 {vid}: {exc}")
        return report

    def poll(self, *, max_rounds: int = 10, interval: float = 0.2,
             stop_after_publish: bool = True) -> CollectReport:
        """轮询直到发布成功或达到轮数上限；失效时自动以新令牌重启轮次。"""
        report = CollectReport()
        for _ in range(max_rounds):
            try:
                self.run_once(report)
            except VersionInvalid as exc:
                # 失效：放弃该轮全部候选，重新发现观察版与令牌，绝不用旧轮补洞。
                vid = self._best_effort_current_version()
                if vid is not None:
                    self.repo.abandon_version(vid, str(exc))
                    report.abandoned.append({"version_id": vid, "reason": str(exc)})
                else:
                    # 没有在途候选（上一轮已发布）：记录一次无候选的失效重启事件。
                    report.abandoned.append({"version_id": None, "reason": str(exc)})
                self.log(f"观察版失效，已放弃候选轮次: {exc}")
                new_token = self._rediscover_token()
                if new_token is None:
                    self.log("无新令牌可用，轮询结束")
                    break
                self.token = new_token
                continue
            if stop_after_publish and report.published:
                break
            time.sleep(interval)
        return report

    def _best_effort_current_version(self) -> str | None:
        row = self.repo.conn.execute(
            "SELECT version_id FROM versions WHERE state='candidate' "
            "ORDER BY created_at DESC LIMIT 1"
        ).fetchone()
        return row["version_id"] if row else None

    def _rediscover_token(self) -> str | None:
        """演示用：模拟器失效后，通过引导接口取得新观察版令牌。

        真实部署中令牌通常来自带外配置/服务发现；这里约定模拟器
        在 /bootstrap 暴露当前有效令牌。
        """
        try:
            data = self._get("/bootstrap")
            self.log(f"发现新观察版 {data['version_id']} / 令牌 {data['token']}")
            return str(data["token"])
        except Exception as exc:  # noqa: BLE001 - 发现失败即结束轮询
            self.log(f"令牌重新发现失败: {exc}")
            return None
