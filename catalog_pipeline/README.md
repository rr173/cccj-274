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
| **跨供应商同款目录、最低报价、按批次同步** | `catpipe.procurement.CrossCatalog`（见下文专章） |
| **面向审计的批次历史来源包、JSONL 证据、离线复核、完成标记** | `catpipe.audit` / `python3 -I catpipe/audit.py`（见“审计历史来源包”专章） |

## 跨供应商同款目录

采购端能力在 `catpipe/procurement.py`，只读取各供应商现有 catpipe 库中的
`published_versions` / `published_products`，**不改采集协议，也不读候选区**。
跨供应商状态保存在独立 SQLite 库中：

- `cc_supplier_versions` / `cc_supplier_quotes`：上次成功选版后复制下来的在售报价；
- `cc_claims` / `cc_claim_endpoints`：人工同款声明及有效无向边；
- `cc_group_members`：传递连通后的同款组成员（包括暂无报价的声明组）；
- `cc_catalog`：当前跨供应商目录快照；
- `cc_batches` / `cc_batch_changes`：成功批次及相对上一成功批次的键级变更。

### 批次 JSON

```jsonc
{
  "suppliers": [
    {"name": "alpha", "db_path": "/data/alpha.db", "version_id": "obs-alpha-v1"},
    {"name": "beta",  "db_path": "/data/beta.db",  "version_id": "obs-beta-v3"}
  ],
  "claims": [
    {"op": "add", "claim_id": "claim-001",
     "a": {"supplier": "alpha", "product_id": 10},
     "b": {"supplier": "beta",  "product_id": 7}},
    {"op": "replace", "claim_id": "claim-002",
     "a": {"supplier": "alpha", "product_id": 2},
     "b": {"supplier": "gamma", "product_id": 5}},
    {"op": "withdraw", "claim_id": "claim-003"}
  ]
}
```

规则：

- 身份固定为 `(供应商名, 数值稳定商品 ID)`；ID 按数值排序，`10` 不会排在 `2` 前。
- `add` 必须使用新编号；`replace` / `withdraw` 必须使用当前有效编号。
- 一批内同一供应商最多选版一次，同一声明编号最多出现一次。
- 声明两端必须是不同商品；供应商必须已经选版或在本批选版。商品 ID 可以暂时无报价。
- 有效声明按图传递连通；多个编号支持同一条边。缺货/换版只影响报价，不删除声明。
- 未在批次中指定 `version_id` 的供应商永不重新打开源库，始终使用上次成功复制的内容。
- 选版时在源库的一个只读事务中读取指定 `published_versions.version_id` 的
  `published_products`；版本不存在、库结构不对或声明编辑无效时，整批回滚。

### 目录记录与变更

每组键是全体成员中按 `供应商名` 字典序、再按数值 `product_id` 排序的最小身份。
组内无任何当前报价时不输出目录记录。最低报价按 `price ASC, supplier ASC,
product_id ASC` 选择，因此同价时取身份排序最小者。

记录示例：

```json
{
  "group_key": {"supplier": "alpha", "product_id": 10},
  "group_supplier": "alpha",
  "group_product_id": 10,
  "members": [
    {"supplier": "alpha", "product_id": 10},
    {"supplier": "beta", "product_id": 7},
    {"supplier": "gamma", "product_id": 3}
  ],
  "winning_identity": {"supplier": "beta", "product_id": 7},
  "supplier": "beta",
  "product_id": 7,
  "url": "https://beta.example/item/7",
  "price": 700,
  "product_revision": 3
}
```

每个成功批次都有连续批次号。变更只包含：

- `op="put"` / `change_type="added"`：新目录键及完整记录；
- `op="put"` / `change_type="changed"`：记录规范 JSON 发生变化的键及完整新记录；
- `op="delete"` / `change_type="removed"`：消失的目录键。

批次号、当前快照和变更在同一个 `BEGIN IMMEDIATE` 事务中提交；源版本读取失败、
声明无效或进程中断都会回滚，重开后只能看到完整成功批次。

### 增量与超内存设计

- 源报价用 `fetchmany(1000)` 流式复制到 `cc_batch_staging`，不 `fetchall` 源库。
- SQL 集合比对得到 `inserted/changed/deleted` 报价；未换版供应商的源库不会被打开。
- 报价变化只通过 `cc_group_members` 定位旧组；声明变化只收集该批声明的新旧端点。
- 图编辑后从这些端点用递归 CTE 重算受影响连通组；未受影响组的成员和快照不动。
- 目录、成员关系和变更都在 SQLite 磁盘表中；`iter_catalog()` / `iter_changes()`
  以及 CLI `--format jsonl` 均按 1000 行块流式读取。
- 历史快照可用批次变更在临时表中重放。

### 本地入口与示例

```bash
# 生成三个供应商样例库和四份批次 JSON，并跑完三批 + 一次失败批次
python3 -I examples/cross_catalog_example.py --workdir ./cross-demo

# 应用批次到一个新库（示例主库已经由脚本应用过 batch1~3，重复新增同编号会被正确拒绝）
python3 -I catpipe/cli.py cross apply \
  --db ./cross-demo/cli-procurement.db \
  --batch ./cross-demo/batch1.json

# 查看当前目录 / 第 2 批变更 / 批次列表
python3 -I catpipe/procurement.py catalog --db ./cross-demo/procurement_catalog.db --format jsonl
python3 -I catpipe/procurement.py changes --db ./cross-demo/procurement_catalog.db --batch 2
# 流式导出给下游（JSON Lines，每行一个 put/delete 事件）
python3 -I catpipe/procurement.py changes --db ./cross-demo/procurement_catalog.db \
  --batch 2 --format jsonl --out ./cross-demo/changes-2.jsonl
python3 -I catpipe/procurement.py batches --db ./cross-demo/procurement_catalog.db

# 下游只读 HTTP：/catalog、/changes、/batches/<n>/changes，均支持 keyset 分页
python3 -I catpipe/procurement.py serve --db ./cross-demo/procurement_catalog.db --port 8091
```

Python API：

```python
from catpipe.procurement import CrossCatalog

with CrossCatalog("./procurement_catalog.db") as catalog:
    receipt = catalog.apply_batch(batch_dict)
    records = catalog.catalog()          # 小结果便捷方法
    for record in catalog.iter_catalog():  # 大目录流式读取
        ...
    changes = catalog.get_changes(2)
```

`catpipe.cross_catalog`、`catpipe.cross_supplier`、`catpipe.cross_vendor`
均为同一实现的命名别名。

## 审计历史来源包（批次时点证据导出）

采购质疑“为什么归成一组、为什么选这个报价”时，`catpipe.audit` 为一个
**成功批次号**导出可流式消费、可离线独立复核的 JSONL 证据来源包。它只以
只读方式打开现有采购库和各供应商的**不可变发布版**
（`published_versions` / `published_products`），不接触采集协议与候选区，
也不向采购库写任何东西。

```bash
# 可复现示例：含“明确缺席”成员、平价选择、事实锁定与严格失败演练
python3 -I examples/audit_package_example.py --workdir ./audit-demo

# 下面的手动导出复用“跨供应商目录”示例生成的健康采购库（先跑一次）：
python3 -I examples/cross_catalog_example.py --workdir ./cross-demo

# 为第 1 批导出目录包：evidence.jsonl + manifest.json + DONE
python3 -I catpipe/audit.py export \
  --db ./cross-demo/procurement_catalog.db --batch 1 --out ./audit-demo/pkg-b1

# 只凭导出物全量离线复核（DONE→清单哈希→证据哈希→逐行重算）
python3 -I catpipe/audit.py verify --package ./audit-demo/pkg-b1
python3 -I catpipe/audit.py status --package ./audit-demo/pkg-b1
python3 -I catpipe/audit.py records --package ./audit-demo/pkg-b1   # 流式打印 JSONL

# 等价入口
python3 -I catpipe/cli.py audit export --db ... --batch 1
python3 -I catpipe/procurement.py audit-export --db ... --batch 1 --out ...
python3 -I catpipe/procurement.py audit-verify --package ./audit-demo/pkg-b1
```

`--out x.jsonl` 时改用单文件模式：证据写到 `x.jsonl`，清单与完成标记为
同名 `x.manifest.json`、`x.done` 旁车文件。省略 `--out` 时默认写到
`<采购库目录>/audit-packages/batch-<N>/`，省略 `--batch` 时取最新成功批次。

Python API：

```python
from catpipe.audit import export_package, verify_package, iter_package_records

result = export_package("./procurement_catalog.db", 1, "./audit-b1")
report = verify_package("./audit-b1")          # 全新进程/重启后同样可复核
for evidence in iter_package_records("./audit-b1"):   # 流式，逐组一条
    ...

# 也可以：CrossCatalog("./p.db").export_audit_package(1, "./audit-b1")
```

`catpipe.audit_package`、`catpipe.provenance`、`catpipe.source_package`
以及 `export_audit_package` / `export_source_package` /
`AuditSourcePackage` / `AuditExporter` 均为同一实现的命名别名。

### 一条证据记录包含什么

每个该批次时点目录快照中的组对应 `evidence.jsonl` 一行（规范紧凑 JSON、
键排序、逐行换行，重复导出字节一致）：

- `group_key` / `catalog_record_at_batch`：该时点的组键与批次提交时持久化
  的完整目录记录（事实锚点）；
- `members[]`：**全体成员**，每个成员带
  - `adopted_version_id`：该成员所属供应商在第 N 批时点**实际采用的观察版**
    （重放只增不改的 `cc_batches.input_json` 选版历史得到，并用
    `cc_supplier_versions` 的 `selected_batch` 交叉校验）；
  - `source_row`：该观察版 `published_products` 中的**完整来源行**
    （version/product/url/price/product_revision），或
  - `listing_status="absent"` + `absence_evidence`：版本行存在、ID 位于
    版本域内、被覆盖证明的合并区间覆盖、但该版无商品行的**明确缺席证明**；
- `supplier_versions{}`：每个相关供应商的版本锁定、解析到的源库路径、
  发布时间、ID 域、覆盖区间结论及覆盖证明/清单的 SHA-256；
- `winner`：按 `(price ASC, supplier ASC, product_id ASC)` 从内嵌来源行
  **独立重算**的最低报价成员，含其**完整来源行**和与批次 `catalog_row`
  的逐项一致性；
- `claims[]` 与 `connection_evidence`：第 N 批时点**有效**、两端都在组内
  的全部同款声明编号与端点（含同一边的多个编号），以及确定性生成树
  `connecting_claim_ids` / `spanning_tree_edges`，可直接验算“这些声明足以
  连通整组”。

下游不需要任何数据库：`verify_evidence_record(record)`（纯标准库）演示了
如何只凭一行重算成员排序、最小组键、声明无向图连通分量、生成树连通性、
赢家选择以及与完整来源行/批次记录的一致性；`verify_package` 在此之外再
校验 DONE→manifest→evidence 的 SHA-256 链与行数。

### 事实锁定与“不降级”

- 选版与声明状态通过重放 `cc_batches.input_json` 中第 `1..N` 批的不可变
  成功输入重建；之后撤回/替换声明、供应商发布新版本或换版，都改变不了
  第 N 批包内的事实。包内联了完整来源行，因此**源库以后删行/删版也不影响
  已完成包的离线复核**（示例脚本会现场演练这一点）。
- 以下任一情况，导出严格失败（`AuditError`），只清理隐藏暂存目录，目标
  位置不会出现 `DONE` 成品：
  - 批次号不是成功批次；
  - 某成员供应商在 N 时点的选版无法重建，或其目标 `published_versions`
    行在所有候选源库路径中都不存在；
  - 记录赢家的完整来源行缺失，或来源行载荷/重算赢家与批次记录不一致；
  - 重建出的当时有效声明不能让连通分量恰好等于批次组成（缺少有效连接
    证明）；
  - 商品在目标版既无来源行、又无法给出域内完整覆盖的明确缺席证明。

### 超内存、原子完成与重启可读

- `<=N` 的批次变更在 TEMP 磁盘表中流式重放出 N 时点快照（`temp_store=FILE`、
  1000 行块），证据逐行生成、逐行写盘并增量哈希；Python 进程任何时刻只持有
  一个组。
- 包先写在与目标同级、同文件系统的隐藏目录
  `.<name>.building-<uuid>/`；证据写完后先对落盘 JSONL 做**全量逐行独立
  复核**，通过后才原子写 `manifest.json`（文件 SHA-256/字节数/组数/统计/
  批次输入），**最后**原子写 `DONE`（内含 manifest 与 evidence 的
  SHA-256），再把暂存目录原子改名为最终包目录。
- 没有 `DONE` 的目录，`package_status` 报 `incomplete`，
  `verify_package` / `iter_package_records` 默认直接拒绝；崩溃留下的
  `.building-*` 隐藏目录永远不会被当成成品。已完成的包只靠目录内三个文件，
  在全新进程（或重启后）仍可完整读取与复核。

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
│   ├── procurement.py # 跨供应商同款目录、最低报价、批次变更与 CLI
│   ├── audit.py       # 审计历史来源包：批次时点证据 JSONL、清单、DONE 与离线复核
│   ├── audit_demo.py  # 可复现审计包示例（锁定/严格失败/半成品演练）
│   ├── audit_package.py / provenance.py / source_package.py # audit 的命名别名
│   ├── cross_reader.py # 跨供应商目录下游只读 HTTP/分页服务
│   ├── cross_demo.py  # 可复现跨供应商示例
│   └── cli.py         # 命令行入口与端到端演示
├── examples/
│   ├── cross_catalog_example.py # 跨供应商目录复现示例入口
│   └── audit_package_example.py # 审计历史来源包复现示例入口
└── tests/
    ├── test_ingest_publish.py  # 22 条语义单元测试
    ├── test_end_to_end.py      # 5 条 HTTP 端到端测试
    ├── test_cross_catalog.py   # 跨供应商分组/批次/原子性测试
    └── test_audit_package.py   # 审计证据包/锁定/严格失败/崩溃安全测试
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
python3 -I tests/test_cross_catalog.py
python3 -I tests/test_audit_package.py

# 4) 跨供应商目录复现示例
python3 -I examples/cross_catalog_example.py --workdir ./cross-demo

# 5) 审计历史来源包复现示例（导出/锁定/严格失败/离线复核）
python3 -I examples/audit_package_example.py --workdir ./audit-demo
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
