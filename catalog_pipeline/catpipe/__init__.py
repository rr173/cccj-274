"""catpipe: 分页目录采集、整版校验与快照发布流水线。

模块划分
--------
- protocol:  线格式、摘要、常量、异常类型
- repository: SQLite 存储层（候选暂存、证据、已发布版、覆盖证明、接收日志）
- ingest:    单页接收的全部校验/替换/拒绝语义
- publisher: 覆盖证明、整版发布、新旧版差异（新增/变更/未变/下架）
- simulator: 本地接口模拟器（观察版/令牌/页乱序/重送/失效/故障注入）
- collector: 采集器（流式拉取 JSONL，超内存安全）
- reader:    下游只读 HTTP 接口（只能读完整观察版，不暴露未发布候选）
- cli:       命令行入口与场景演示
"""

__all__ = [
    "protocol",
    "repository",
    "ingest",
    "publisher",
    "simulator",
    "collector",
    "reader",
    "cli",
]
