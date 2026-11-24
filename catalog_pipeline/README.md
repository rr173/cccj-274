# catpipe — 分页目录采集与整版发布流水线

供应商按页面交付目录，修订页可能更换覆盖窗口。本项目用 Python（**仅标准库**）
实现分页采集、严格校验、整版原子发布，以及一个本地接口模拟器和下游只读服务。

## 规格对应关系

| 规格要求 | 实现位置 |
|---|---|
| 每轮有观察版 ID 与有限稳定商品 ID 域；页声明半开 `[lo,hi)` 范围、页 ID/修订号 | `protocol.PageHeader`、`simulator.render_page_lines` |
| 页列出该范围当版全部在售商品（稳定 ID、URL、价格、商品修订）；缺席仅在完整范围内表示下架 | 不存缺席行，由 `pages.range_*` 与 `evidence` 存在性推导（`ingest._check_contradictions`） |
| 页可重叠；同版同商品出现/缺席或载荷不得矛盾；URL 变动不另造商品 | 三条集合 SQL 矛盾检查（payload/presence ×2），身份永远是 `product_id` |
| 范围内 ID 重复、越界、证据矛盾 → 拒绝整次接收，不改候选 | 暂存表主键 + 事务回滚 + 隔离区（`ingest.IngestionEngine`） |
| 高修订整份替换旧页值与覆盖；同号同内容重送无操作；同号异内容/低修订拒绝 | `ingest.IngestionEngine`（accepted/replaced/noop/rejected 四态，单事务） |
| 观察版或令牌失效 → 放弃整轮，不拿旧轮补洞 | `VersionInvalid` + `Repository.abandon_version` + `Collector.poll` |
| 仅当有效页覆盖整个 ID 域且无矛盾才发布；与旧版列新增/变更/未变/下架 | `publisher.publish`（区间合并覆盖证明 + 四类 diff） |
| 发布失败旧版仍可读 | 单事务原子发布，`published_*` 多版本保留 |
| 覆盖证明及原响应定位随版保留 | `published_versions.coverage_proof_json`、`pages.raw_locator`、`raw/` 落盘 |
| 响应和商品可远超内存；修订不重扫其他页原文；空窗口不逐 ID 展开 | 逐行 JSONL 流式落盘/入暂存；矛盾检查纯 SQL；覆盖用区间合并 |
| 接口允许页乱序与重送 | 内容摘要按 product_id 排序，乱序同内容判 noop；校验基于集合而非顺序 |
| 展示移窗缺口、重叠撤回、URL 迁移、失效重启 | `python -m` 演示六场景（见下） |
| 下游只能读完整一个观察版，不能混轮 | 只读连接 + 仅暴露 `published_*`，候选表物理不可见 |

## 目录结构

```
catalog_pipeline/
├── catpipe/
│   ├── protocol.py    # JSONL 线格式、内容摘要、异常
│   ├── repository.py  # SQLite 存储：候选/证据/发布快照/覆盖证明/隔离区/日志
│   ├── ingest.py      # 单页接收引擎（全部校验/替换/拒绝语义）
│   ├── publisher.py   # 覆盖证明、原子发布、四类差异
│   ├── simulator.py   # 本地接口模拟器（HTTP/JSONL + 故障注入 + 管理接口）
│   ├── collector.py   # 流式采集器（轮询、失效弃轮、尝试发布）
│   ├── reader.py      # 下游只读 HTTP 服务
│   └── cli.py         # 命令行入口与端到端演示
└── tests/
    ├── test_ingest_publish.py  # 22 条语义单元测试
    └── test_end_to_end.py      # 5 条 HTTP 端到端测试
```

## 快速开始

需要 Python 3.10+，无第三方依赖。建议用隔离模式运行（避免 cwd 中
被植入的 `json.py` 等被导入）：

```bash
# 1) 一条龙演示（自动起停模拟器与只读服务，覆盖全部关键场景）
python3 -I catpipe/cli.py demo --workdir ./run

# 2) 分进程运行
python3 -I catpipe/cli.py sim  --port 8080
python3 -I catpipe/cli.py collect --url http://127.0.0.1:8080 --db ./run/cat.db
python3 -I catpipe/cli.py read --db ./run/cat.db --port 8090
python3 -I catpipe/cli.py status --db ./run/cat.db

# 3) 测试
python3 -I tests/test_ingest_publish.py
python3 -I tests/test_end_to_end.py
```

## 线格式（JSONL，每行一个对象）

```jsonc
{"type":"page","version_id":"obs-v1","token":"...","page_id":"pA","revision":2,
 "range":{"lo":0,"hi":300},"domain":{"lo":0,"hi":900}}
{"type":"product","product_id":42,"url":"https://x/42","price":1420,"product_revision":1}
{"type":"end","count":299,"checksum":"<sha256，覆盖 end 之前的行>"}
```

## 模拟器管理接口

| 接口 | 作用 |
|---|---|
| `POST /admin/replace-page` | 推送某页高修订（移窗/改价/下架/URL 迁移），修订号必须前进 |
| `POST /admin/upsert-page` | 新建一页 |
| `POST /admin/invalidate` | 作废当前观察版+令牌，可 `rollover` 到新观察版 |
| `POST /admin/fault` | 一次性故障：`duplicate_product`/`out_of_range`/`bad_checksum`/`truncated`/`http500` |
| `GET /bootstrap` | 失效重启后的带外引导，返回当前有效观察版与新令牌 |

## 只读服务端点

`GET /versions`、`/latest`、`/versions/{vid}`、
`/versions/{vid}/coverage-proof`、`/versions/{vid}/manifest`、
`/versions/{vid}/products?after_id=&limit=`（keyset 分页）、
`/versions/{vid}/product/{pid}`、`/versions/{vid}/diff`。

## 关键设计说明

1. **两套世界分离**：`versions/pages/evidence/page_staging` 是在途候选，
   `published_versions/published_products/published_diff` 是不可变发布快照。
   读者以 `mode=ro` 打开库且只查后者，候选半成品物理上不可见。
2. **缺席不展开**：不存任何缺席商品行。下架 = 商品落在某页完整半开范围内、
   但 `evidence` 无对应行；空窗口 `lo==hi` 只贡献空区间。覆盖证明是
   O(P log P) 的区间合并，缺口只存端点，不逐 ID 展开。
3. **修订不重扫原文**：高修订替换先在 `evidence`/`pages` 上跑集合 SQL 矛盾
   检查，通过后删旧证据插新证据；旧页原始 JSONL 不再被解析。
4. **拒绝即隔离**：任何拒绝（结构/校验和/矛盾/过期修订）都在事务内回滚，
   候选不变，原始响应移入 `quarantine/` 并在 `deliveries` 留审计记录。
5. **流式安全**：采集逐行读 HTTP、逐行落 `raw/`、逐行进暂存表，整个接收
   处于单事务内（拒绝即回滚，暂存自动清空）。实测 100 万商品页：
   峰值 RSS 约 50MB（与页规模脱钩）、SQLite 139MB，内容摘要走排序游标不缓存行。
