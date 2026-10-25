# 同步业务表写入

适用版本：2.0.0 候选；本文使用当前标识与契约；历史 1.1.0 的 AWS 验收不代表候选通过，候选验证范围见 [PR #23](https://github.com/colvenna/colvenna-isle/pull/23)。同步写由机器合同校验、原生事务和独立私网代理实现；接口见 [API 契约](api-contract.md#synchronous-table-writes)，使用方式见[用户手册](../user-guide.md)。公开写后查询（`after_write`）见第 6 节。多 DuckDB 分片与独立写者只是尚未设计的演进方向，不在 1.1.0 范围内（见[发行说明](../release-notes.md#计划中尚未实现)）。

## 1. 写入目标与成功语义

Colvenna Isle 提供同步业务表写入和异步批量接入。同步请求在指定原生 DuckDB 表的事务持久提交后返回；异步接入保留 S3 归档和 SQS FIFO 缓冲。

| 入口 | 处理对象 | 成功回执 |
| --- | --- | --- |
| `POST /v1/spaces/{space_id}/ingestions` | 追加、更新、删除事件，先进入 ODS | `202 ACCEPTED`：原文已归档且 SQS 确认发送 |
| `POST /v1/spaces/{space_id}/writes` | 注册且允许直接写入的业务表 | `200 COMMITTED`：目标数据、写入账本、变更记录和数据集水位已在同库事务中持久提交 |

等待 ODS 接入提交与同步改写目标业务表具有不同语义。同步表写入不等待 DWD/DWS/ADS 流水线完成；响应不宣称新的分析 release 已发布。1.1.0 不提供任意写 SQL、DDL、跨请求交互事务或跨表/跨库事务 API。

## 2. 注册、权限与原生约束

- 注册合同包含 `write_owner: direct | ingestion | pipeline`。只有明确为 `direct` 的数据集可使用同步写入；未声明 owner 的定义不能获得直接写权限。写者按注册定义创建原生表并安装约束。
- 每个目标只有一个写入 owner。异步接入与流水线发布不得改写 `direct` 表；同步 API 不得修改接入管理表、流水线目标、`ctl`、内部状态或未发布 generation。DWD 等业务 schema 可以包含明确登记为 `direct` 的人工维护表，但所在 schema 不独自授予写权限。
- 使用 Bearer API Key 和 `data:write` scope，另核对数据空间、目标数据集及 owner。`ingest:write` 不自动获得同步表写权限。查询仍要求 `query:read`；写入回执仅对原 principal 或获授权空间管理员可见。轮换 key 保持 principal 身份。
- 原生索引、PRIMARY KEY、UNIQUE、FOREIGN KEY、NOT NULL 和 CHECK 按实际 DuckDB 能力执行。注册键 metadata 不代替原生约束；写者核对物理表、schema 版本及约束。DuckDB 本身的限制仍有效，不承诺额外的延迟约束检查或 OLTP 性能。
- 一个已知的 DuckDB 1.5.6 限制：`UPDATE ... RETURNING`，以及给被索引覆盖的列（显式索引、PRIMARY KEY、UNIQUE）或 LIST 列（`VARCHAR[]`）赋值的 UPDATE，会被执行为先删除再插入，而外键检查发生在删除那一步。写者因此只给值实际变化的列赋值，不使用 RETURNING，提交后再读回存储的行。这样，被其他表外键引用的行仍可 `update` / `upsert`：未变化的列（包括索引列和 LIST 列）不会被赋值，完全相同的 upsert 也计为一行受影响。但如果确实要改变被引用行的某个索引列或 LIST 列，DuckDB 会拒绝，请求以 `422 NATIVE_CONSTRAINT` 整批拒绝，不产生任何效果（见[发行说明](../release-notes.md#限制)）。

## 3. 单请求事务

一个请求只指定一个数据集、一个 schema 版本和一种操作，批次全部成功或全部回滚：

| operation | 请求数据 | 行为 |
| --- | --- | --- |
| `insert` | `records`：完整目标行 | 插入；重复主键/唯一键按约束拒绝 |
| `upsert` | `records`：完整目标行 | 按注册且由原生 PRIMARY KEY/UNIQUE 支持的 `key_columns` 插入或替换非键列 |
| `update` | `updates`：每项含完整 `key` 与非空 `set` | 精确按注册键更新指定非键列；不修改键或服务端保留列 |
| `delete` | `keys`：完整注册键 | 精确删除匹配行 |

`upsert`、`update`、`delete` 要求唯一、非空的注册键；一个请求内同一键只能出现一次。`insert` 没有注册键时仍可使用请求级幂等，其他原生约束继续执行。未知字段、非法类型、缺失必填列、空批次和过期 schema 拒绝；Decimal/BIGINT/时间类型沿用[类型合同](../../src/colvenna_isle/contracts/v2/contracts.schema.json)。

更新或删除未找到键时该项影响零行；`affected_rows` 统计插入/upsert 行、匹配的 update 行（包括值未变的行）或实际删除行。成功的零行请求仍记录唯一写入结果及提交序号。约束或任何一项校验失败时不允许部分成功。

业务变更、`ctl.writes`、数据集变更记录及处理水位使用同一数据库事务。变更记录保留注册键、INSERT/UPDATE/DELETE、提交序号以及需要的 before/after 分区值，供下游绑定增量区间或全量快照。流水线读取 `direct` 表时仍固定输入；不能直接改写已绑定的 Parquet 输入。同步写不伪装成 ingestion，不复用 ingestion ID 或其事件业务版本协议。

## 4. 入口与统一写者调度

```text
API Gateway → Lambda Authorizer
  ├─ 接入 Lambda → S3 原文 + SQS FIFO → 唯一写者
  └─ 同步写 Lambda（VPC）→ 活动写者私网 API → 唯一写者
                                               ↓
                               原生 DuckDB 事务与 S3 持久存储
```

同步写 Lambda 与接入 Lambda 分别部署、分别授权；同步函数只处理有界请求、解析活动路由并等待结果，不打开读写 DuckDB attachment。它沿用[写者所有权协议](aws-product-design.md#7-aws-组件选择)，私网请求及响应绑定空间 generation、schema、principal、精确活动 Task/进程/镜像/源码/地址与 permit epoch。写者重新核对授权上下文及许可，提交前再次检查。旧地址、伪造身份、控制面不可用及无法确认所有权时不批准新提交。

同步调用使用批准的 Lambda proxy 路由；写者的 8082 只允许同步 Lambda 的独立安全组访问；写者控制接口与查询服务的受控读取使用 8081；写者没有公开入站地址，也没有 ALB。查询 Service 走 VPC Link/内部 ALB。

每库的同步表写、FIFO 接入、流水线导入和发布进入同一调度器。FIFO 只保证其接入消息顺序；两类入口没有共同的客户端到达顺序承诺，实际数据库提交序号定义已完成事务的顺序。同步请求不插队抢占进行中的事务，不增加第二写者。配额约束请求体、行数、排队、并发等待和事务资源；公平调度保证异步接入与分块导入能继续推进。

1.1.0 的同步请求上限为 1 MiB、1,000 项；`timeout_ms` 默认为 10,000，范围为 1–20,000，包含入口验证、排队和等待提交的预算。部署还需为响应保留余量，受 HTTP API 的 30 秒集成上限约束。这些是接口边界，不是提交延迟或吞吐承诺。未进入执行的过载请求返回 `429`、`status=NOT_SUBMITTED` 和重试提示。

## 5. 持久性、幂等与超时

成功必须在事务 COMMIT 和存储持久化完成后返回。DuckDB 恢复所需 WAL 必须进入 S3-backed 存储；不能以 Lambda 转发成功、内存写入、异步状态投影或请求取消作为提交证据。固定上游的 FileSync 会阻塞调用 Rust sync，后者等待 SlateDB write/flush，这是成功回执在 COMMIT 后返回的依据。持久性要求是：成功回执后立即终止写者，替换实例仍恢复精确数据和账本。

`Idempotency-Key` 必填。写入 ID 的 namespace 为当前空间 generation、目标数据集及幂等键，并绑定原 principal。写者在业务执行前持久登记原身份及目标/schema/operation/内容的规范化摘要；登记不表示业务已提交，等待预算不参与业务摘要。这样提交前崩溃仍能核对原内容，不能让另一份 payload 接替原键。同键同内容重试核对原结果，仅在恢复已确认未提交且无原执行继续运行时，才可继续原请求；同键不同内容返回 `409`，其他 principal 不得认领原请求。并发重复在写者中合并或串行核对，不能并发提交两次。

写入结果随业务事务持久记录。确定回滚的拒绝可以另记 `REJECTED` 回执，重试返回原拒绝；只有拒绝已持久记录时才能称为可重放的确定拒绝。保留期覆盖约定的客户端重试窗口，未到期的身份不得回收。完整回执到期后，在该空间 generation 生命周期内保留键/摘要/owner 的拒绝重放标记；过期的原键拒绝重新执行，不将其当作新的写入。长时间 UNKNOWN 不能自动过期重放。

| 返回或观察 | 含义与后续动作 |
| --- | --- |
| `200` / `COMMITTED` | 目标事务已持久提交，返回原 write_id、提交序号及实际 affected_rows |
| 明确约束/校验拒绝 | 该请求未产生业务变更，按错误分类返回 `409`/`422`；已记录的拒绝可核对 |
| `429` / `NOT_SUBMITTED` | 写者执行前明确拒绝；同键可在退避后重试 |
| `504` / `UNKNOWN` | 等待预算耗尽，仍可能已提交或正在完成 |
| `503` / `UNKNOWN`、连接断开或响应丢失 | 无法确定结果；先查询原回执或以原幂等键核对重试 |

HTTP 超时不承诺数据库回滚，也不自动转成 `202` 异步接入。服务端有原身份时返回 `write_id` 和 `status_url`；传输故障导致无回执时，客户端保留原请求和幂等键，通过同键重试核对。正在执行的原操作不能被另一请求重新调度。

`GET /writes/{write_id}` 向活动写者的权威账本核对；DynamoDB 可保存监控投影，但不独自决定 COMMITTED/REJECTED。账本不可用、投影缺失、permit 不明、接管中或存在未完成原执行时返回 UNKNOWN；单次“没有记录”不证明没有提交。恢复后先核对原身份和同库账本，再决定是否可执行未提交的原请求。恢复未完成时拒绝新执行。

重试窗口为 30 天，回执与身份至少保留至窗口结束；1.1.0 保守保留整个 generation 的记录，不自动回收 UNKNOWN。写者在排队前以条件创建保存 `spaces/<space>/write-intents/<generation>/<write_id>.json` 原请求，绑定首个 principal、规范化业务摘要、schema 和源提交。S3 原请求不是异步派发队列或提交凭证；`ctl.writes` 始终是权威结果。代理没有 S3 写入权限。

当前候选的原生格式为 6。历史 1.1.0 为格式 5。物理表实际创建结构化 `constraints.primary_key / unique / foreign_keys / checks` 和 `indexes`，并封存列、原生约束及索引的目录摘要。外键引用只能指向已注册的直接写目标；不得用注册键代替原生唯一约束。格式变化时停止旧实例并使用新的 generation，不迁移旧格式（见[安装部署手册](../installation.md#10-破坏性升级)）。

## 6. 写后读取与分析发布

公开查询以 `consistency: after_write` 和 `write_id` 读取同步写之后的状态；机器查询合同检查这两个条件与 after_ingestion、显式 release 条件互斥。同步写提供权威回执和原生提交前置条件。查询服务重新检查 `query:read`、空间/generation、目标权限及回执可见性，确认 COMMITTED 后，由活动写者在其进程内的同一原生数据库实例开启新的受控读事务，复制有界快照供查询执行。快照上限见 [API 契约](api-contract.md#catalog-and-sql-queries)与[发行说明](../release-notes.md#限制)。

返回至少包含该次提交的数据库状态和 `read_watermark`；后续已提交的更新/删除可以影响结果。这不是将表固定在 write_id 对应历史版本的时间旅行。REJECTED 不能满足提交前置条件；UNKNOWN/尚未就绪时等待或明确返回未就绪，不在普通读者上返回旧状态冒充成功。

`after_write`、`after_ingestion` 和显式 `release_id` 不混用；一个请求仅选择一种一致性条件。直接写表没有流水线 release 时，默认 `published` 查询明确拒绝为“无发布版本”，调用者使用 `after_write` 读取当前原生状态并标注实际水位，不虚构 release_id；与分析结果关联时，返回各输入实际 release/水位，也不声称下游覆盖该写入。

实测常驻 DuckDB 只读 attachment 会返回陈旧数据，manifest polling 不是可见性时限。写后读必须沿用受控写者路径；重查询受配额限制，普通分析继续使用独立查询资源。详见[外部读者刷新](aws-product-design.md#2-存储扩展的能力与边界)及[API 合同](api-contract.md)。

## 7. 需要在实际 AWS 中确认的行为

以下行为只能在实际 AWS 环境中确认，文档、图稿和本地原生检查不能代替：

- 四种操作、零行更新/删除、全批回滚及原生主键/唯一性/外键/非空/CHECK 约束，精确数据与独立预期一致。
- 同键重复/并发重复、同键异内容、principal 隔离、schema/generation 改变及旧实例拒绝。
- 提交前故障、持久提交后响应丢失、等待超时、写者切换与恢复；成功数据不丢失、不重复，UNKNOWN 核对后才能归类。
- 同步写与 FIFO 接入/长计算/导入并行的公平性、背压和端到端延迟/资源/成本；1.0.0 规格下的实测值见[性能与容量基线](../performance.md#同步写)。
- 受控原生读取前置条件；公开 `after_write` 在普通读者陈旧、后续更新、写者切换及账本不可用时不返回旧状态冒充成功。

## 8. 依据

- [对象存储扩展 FileSync，固定提交](https://github.com/dentiny/duckdb-object-storage/blob/f096ca13244cb586e04ce04417cadfdd5d18cc0c/src/slatedb_file_system.cpp#L266)
- [对象存储扩展 sync，固定提交](https://github.com/dentiny/duckdb-object-storage/blob/f096ca13244cb586e04ce04417cadfdd5d18cc0c/rust/src/file_handle.rs#L253)
- [DuckDB 约束与限制](https://duckdb.org/docs/stable/sql/constraints)
- [DuckDB 并发](https://duckdb.org/docs/stable/connect/concurrency)
- [HTTP API 配额](https://docs.aws.amazon.com/apigateway/latest/developerguide/http-api-quotas.html)

DuckDB is a trademark of the DuckDB Foundation. Colvenna Isle is independent of the DuckDB Foundation and is not endorsed by it.
