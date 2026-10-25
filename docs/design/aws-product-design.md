# Colvenna Isle：AWS 数据库与分析产品设计

当前未发行的 `2.0.0.dev0` 候选采用 Colvenna 接口名、契约 revision 2 与原生格式 6，功能架构沿用下文的 1.1.0。标识与数据迁移见[迁移设计](identifier-migration.md)。栈、ECR、备份库、归属标签与运行时前缀统一采用 `colvenna-isle`；升级边界见[发行说明](../release-notes.md#200-unreleased)与[安装手册](../installation.md#10-破坏性升级)。不将历史 AWS 验收计入新候选。

适用版本：1.1.0。本文描述 1.1.0 的产品架构；支持矩阵、规格与限制见[发行说明](../release-notes.md)。文中标为演进方向的内容（拆库、多 DuckDB 分片、Lambda Managed Instances / Durable Functions）尚未实现，不在 1.1.0 范围内。

各版本不提供向后兼容、旧数据自动迁移或零停机升级：升级先关闭环境，合同或存储格式变化时按空间重建，见[破坏性升级](../installation.md#10-破坏性升级)。单次运行的固定输入、幂等和发布正确性不受此影响。

## 1. 已确定的架构

对外提供一个写入入口：

```text
API Gateway HTTP API
  → Lambda Authorizer（Authorization: Bearer <API_KEY>）
  → 接入 Lambda
  → SQS FIFO
  → ECS Fargate 唯一写者
  → S3 上的 DuckDB 对象数据库
```

每个数据空间先使用一个逻辑 DuckDB 数据库，ODS、DWD、DWS、ADS 是其中的四个 schema。查询使用独立 ECS 进程；SQL/Python 计算可用 Lambda 或 ECS Task；所有数据库变更归唯一写者所有。

直接业务表同步写使用独立 VPC Lambda，经当前许可写者的 8082 私网端口等待原生事务回执；不复用接入排队函数。只有显式 `write_owner: direct` 和 `data:write` 授权可修改目标。数据、原写入账本、变更记录和水位同事务持久提交后返回 `200`；超时保留原身份并核对 `UNKNOWN`。详见[同步写设计](synchronous-writes.md)。

DynamoDB 使用一张 `colvenna-control` 控制面表，保存 API Key、空间路由和运行状态。接入 Lambda 将批次直接发送 SQS；接收、排队和 DuckDB 提交不经过 DynamoDB 事务或 Streams。认证仍由 Lambda Authorizer 查询控制表，认证服务不可用时拒绝访问。

独立计算采用“固定输入版本 → Lambda / ECS Task 计算 → S3 暂存 → 写者分块导入 → 发布版本”。持续影响接入的分析写入负载，才触发将 `ODS+DWD` 与 `DWS+ADS` 拆成两个数据库的演进。

“一个写入入口”是产品 API 的定义；“每库一个写者”是存储运行约束。将来按租户、数据空间或负载拆库时，公开 API 可以保持不变。

交互图：[architecture.html](architecture.html)。图中分别标明独立同步 Lambda、ECS 写者 Service、查询私网链路，以及 Lambda Invoke / ECS RunTask 执行器。“控制面 Lambda”表示独立的 Authorizer、管理和调度函数；查询链路标注 VPC Link / 内部 ALB / ECS。接入服务先保存 S3 原文，写者从队列引用中读取原文。“S3 原生数据库”保存 SlateDB 管理的数据库对象；“S3 计算文件”保存固定 Parquet 输入、代码版本及暂存结果。环境构建服务和 ECR 详见[运行环境设计](runtime-environments.md)。

## 2. 存储扩展的能力与边界

下表依据 1.1.0 固定的对象存储扩展提交 `f096ca13244cb586e04ce04417cadfdd5d18cc0c`（见 [storage-lock.json](../../runtime/storage-lock.json)），上游说明查阅于 2026-10-15。“实测”指在该固定运行栈上测得的读写边界，其余为上游说明。

| 能力 | 当前文档说明 | 对产品的影响 |
| --- | --- | --- |
| 原生数据库上对象存储 | 自定义 FileSystem 经 SlateDB/OpenDAL 保存逻辑数据库文件及 WAL | 保留原生表、索引、约束；S3 中实际是 SlateDB 对象 |
| 并发模型 | 每库一个读写进程，任意数量只读进程 | 写者常驻，查询进程独立扩容 |
| 外部读者刷新 | SlateDB 约每 10 秒跟随 manifest；实测常驻 DuckDB attachment 在 25 秒观察窗内仍返回旧行/旧 schema，DETACH/ATTACH 与新进程分别测量 | API 回执、数据库提交、查询可见分开表示；重开 attachment 并核对实际 release/水位，polling 不构成查询刷新时限 |
| 同进程读取 | 存在读写 attachment 时，读打开使用同一读写 SlateDB 实例 | 明确要求读到刚提交数据的请求路由到写者实例 |
| 第二个写者 | 第二个 ATTACH 不立即拒绝；实测旧者后续 INSERT 提交被 fencing 拒绝，但 SELECT / 无改动 CHECKPOINT 仍可返回成功 | 需要外部独占部署；不能把 fencing 或单个 SQL 成功回执当作所有权证明 |
| 配置 | bucket/root/cache 等在首次访问时固定，且为进程级配置 | 按数据空间分配运行实例和配置 |
| 本地资源 | spill 留在本地；默认另有 512 MiB 数据缓存、128 MiB 元数据缓存 | DuckDB memory_limit 以外还要预算缓存、Rust runtime、spill |
| 分布式执行 | 作者展示了 Duckherder 部分聚合；统一快照固定及强一致模式在上游列为后续计划 | 每条查询由一个查询进程执行，长计算使用应用管理的固定文件版本 |

发行镜像固定 DuckDB 版本、扩展二进制及其来源：对象存储扩展从固定源码构建，httpfs 与 aws 使用 hash 固定的官方扩展，全部记录在 [storage-lock.json](../../runtime/storage-lock.json)；运行时不安装或更新扩展。

## 3. 产品提供什么

面向通过 API 持续导入业务事件、订单、日志和指标的分析型数据空间：

- 数据接入：批量 append/upsert/delete 事件，schema 校验，幂等键，状态回执。
- 数据开发：内置 SQL/Python 流水线、依赖环境构建、版本发布、调度、增量水位、重算、取消和失败重试。
- 数据分析：SQL 查询、指标接口、报表、结果导出，显示数据版本和更新时间。
- 数据运维：接入积压、层间延迟、查询耗时、资源配额、失败批次和发布历史。

产品接口：

| 接口 | 行为 |
| --- | --- |
| `POST /v1/spaces/{space_id}/ingestions` | 接收批次，返回 `202` 与 `ingestion_id` |
| `GET /v1/spaces/{space_id}/ingestions/{ingestion_id}` | 返回接收、提交、各层处理状态 |
| `POST /v1/spaces/{space_id}/queries` | 提交分析查询，返回 `query_id` |
| `GET /v1/spaces/{space_id}/queries/{query_id}` | 返回运行状态、数据版本及结果地址 |
| `POST /v1/spaces/{space_id}/pipelines/{pipeline_id}/runs` | 针对指定输入版本执行转换 |
| `GET /v1/spaces/{space_id}/datasets/{dataset_id}` | 返回当前发布版本、水位和更新时间 |

长 SQL 和大结果使用异步接口。HTTP 连接的寿命不决定数据库任务的寿命。

### 认证：Bearer API Key

所有产品 API 使用：

```http
Authorization: Bearer <API_KEY>
```

token 的内容直接是服务端签发的高熵 API Key。API Gateway HTTP API 使用 Lambda Authorizer 校验不透明 token；业务接入 Lambda 接收 authorizer 的身份和权限上下文。

API Key 注册信息包括 `key_id`、key hash、`space_id`/授权空间集合、scopes、到期时间、吊销状态和创建主体。原始 key 只在创建时展示；服务端保存 SHA-256 或带服务端密钥的 HMAC-SHA-256 摘要，不能将完整 key 写入日志。审计使用 key_id/fingerprint。

数据 scopes 为 `ingest:write`、`query:read`、`pipeline:run`；管理 scopes 为 `monitor:read`、`keys:manage`、`pipeline:manage`、`runtime:manage`。Authorizer 校验 key 存在、未过期且未吊销；业务服务再次核对实际路由的数据空间及所需 scope。SQS 的 space_id/MessageGroupId 从已授权的业务上下文产生，并与请求路径匹配。

最初关闭 authorizer 缓存以获得明确的吊销行为；需要缓存时，将“吊销最长生效时间”作为公开的产品约定，并确保缓存授权不会跨路由或数据空间扩大权限。DynamoDB TTL 只用于记录回收，不能替代请求时的到期校验。

API Gateway 与接入服务明确处理无 token、无效 key、权限不足和限流；外部错误响应不暴露 key 的内部 hash 或注册细节。

## 4. 数据模型：先一库四层

```text
duckdb_objfs://spaces/<space_id>/warehouse.db
├── ods    原始业务记录与接入信息
├── dwd    清洗、去重、维度关联后的业务明细
├── dws    可复用的主题聚合
├── ads    面向指标 API、报表和应用的数据集
└── ctl    幂等账本、水位、作业、导入分块及发布版本
```

| 层 | 建议内容 | 更新方式 |
| --- | --- | --- |
| ODS | `event_id`、业务版本、事件时间、接入时间、操作类型、原始字段 | 批量追加；原始请求另存 S3 |
| DWD | 类型统一、业务去重、关联维度后的事实和维度 | 增量处理，修正受影响的业务键或分区 |
| DWS | 日期、客户、产品等主题聚合 | 增量聚合；迟到、更新、删除事件重算受影响分区 |
| ADS | 指标视图、应用查询表、发布版本 | 小结果用视图；昂贵结果物化并版本化发布 |
| CTL | ingestion/job/chunk ledger、pipeline 水位、release 指针 | 与对应数据变更在同库事务中提交 |

ODS/DWD/DWS/ADS 是数据组织方式，不强制要求四个数据库或四个服务。

### S3 中的格式

两篇微信文章介绍的 `duckdb_object_storage` 保存的是原生 DuckDB 数据库。DuckDB 的数据库文件、WAL 和恢复文件经过虚拟文件系统映射到 SlateDB，S3 中的物理对象是 SlateDB 的 SST、WAL 和 manifest；`warehouse.db` 是示例逻辑数据库名，不是直接上传的一份 `.duckdb` 文件。写者按存储根的 SHA-256 生成独立逻辑文件 URL，避免同一进程多个空间的文件句柄冲突。

ODS/DWD/DWS/ADS 表、索引、约束和数据库目录都保留原生 DuckDB 语义。普通分析通过 `ATTACH 'duckdb_objfs://…'` 查询这些表，不能用 `read_parquet` 或其他引擎直接扫描 SlateDB 对象。

Parquet 只用于本产品额外设计的长任务固定输入、计算结果暂存和查询导出。它不替代原生数据库，也不是这两篇文章所描述的数据库存储格式。

同库使短转换链及对应水位能够使用一次事务提交。DuckDB 官方说明，一次事务只能写一个 attached database，不提供跨 attached database 的原子提交。四层各放一个库，需要自行处理层间版本、重试及部分成功。

单写者是一个进程，而不是只允许一条连接。DuckDB 同进程支持多个连接和线程，但重查询、事务冲突和后台整理仍然共享资源。写操作交给受控调度器；查询池和长任务池独立于写者。

## 5. 接入为什么加队列

SQS 用来吸收 API 峰值，并在写者重启、计算导入或短暂不可用时保留工作。它不增加单库的持续写入能力；若长期到达速率高于处理速率，仍需限流、扩大批次、提升写者规格或拆库。

采用 FIFO 的约定：

- `MessageGroupId = space_id`，把同一库的消息交给其唯一写者。
- `MessageDeduplicationId = ingestion_id`，减少有限窗口内的重复投递。
- 消息只携带原始 S3 对象引用、hash、schema 版本和接入 ID。
- 数据库 `ctl.ingestions` 仍然实现持久幂等，不能依赖 FIFO 消除所有重复。
- FIFO 顺序是接入顺序；业务事件的先后另由 `source_version`/事件时间定义。

### 默认接入协议

原始批次使用不可变 S3 对象保存。同一空间、数据集和幂等键确定稳定的 `ingestion_id` 与原始对象键；不把 payload hash 放进该对象键，以便发现同一幂等键对应不同内容的冲突。

1. 接入 Lambda 校验已授权的身份上下文、数据空间、schema 和幂等键，计算规范化 payload hash。
2. 使用条件创建写入原始 S3 对象，同时保存 payload hash。对象已存在时比较 hash；相同内容沿用原始 ingestion_id，不同内容返回 `409 Conflict`，不能覆盖原文。
3. 保存成功后直接调用 SQS `SendMessage`，消息引用该原始对象。只有 SQS 确认发送成功才返回 `202` 与 ingestion_id。
4. 发送失败返回可重试错误；发送结果不明时沿用同一 ID 重试，由 FIFO 的有限去重窗口和 DuckDB 持久账本消重。
5. 写者消费后，在 DuckDB 同一事务中提交 ODS 数据及 `ctl.ingestions`，提交成功后删除 SQS 消息。

S3 与 SQS 没有跨服务原子事务。未成功排队的原始对象可能暂时保留；原始归档采用公开的保留期限，清理前还要核对重试窗口、接入状态及重放需求。客户端未得到明确结果时必须查询状态或沿用同一幂等键重试。

写者提交成功后、SQS 删除消息前崩溃，重投递必须查询原始 ingestion ledger 并返回已有结果。接入 Lambda 不写 DynamoDB；控制面根据写者消费观测和 DuckDB 权威账本异步更新接入、COMMITTED/PUBLISHED 状态，按稳定 ID、状态版本重试与补偿。控制表更新失败不回滚接收或提交，也不延迟 `202`。

调用者收到的 `202` 是 SQS 发送成功的接收回执。写者尚未消费或投影尚未更新时，状态 API 可能返回 `UNKNOWN`/待核对；不能将“无投影”解释成“未接收”，也不能仅凭原始 S3 对象存在就宣称已接收。

背压、原生失败诊断、DLQ 与受控重放的接口见 [API 契约](api-contract.md#ingestion-governance)：入队前检查积压、年龄、实际写者处理能力和空间阈值，过载拒绝不删除已接收工作。重放先核对原账本与固定原文，保留原身份和权限，并持久记录审计；DLQ 删除先保存原生核对回执。原文至少保留 45 天和最近重放后的十四天，具体清理仍需引用与原账本核对。

### 回执语义

| 状态 | 产品含义 |
| --- | --- |
| ACCEPTED | 原始 S3 对象已保存，SQS 已确认发送成功，待入库 |
| COMMITTED | 对应 DuckDB 事务已提交 |
| PUBLISHED | 对应分析数据集的新版本已发布 |
| FAILED | 验证失败或任务失败，可查询明确原因及重试策略 |

分析 API 返回它实际使用的 `release_id` 和水位。PUBLISHED 不表示所有已有只读进程瞬间完成刷新。常驻 attachment 可能保持旧 catalog/buffer；读者未见指定版本时必须重新打开并再核对、等待重试或路由到写者，不能仅 sleep 一个 polling 周期，也不能用旧版本冒充指定版本。

## 6. DWD → DWS 很慢时怎么办

长计算不放在接入消费循环里。任务按资源和时间预算选择 Lambda 或独立 ECS Task，计算阶段保持旧 DWS/ADS 版本可读，API 继续接入。普通 Lambda 最多 15 分钟；Managed Instances 的合格异步入口可到 90 分钟（1.1.0 尚未提供该 profile），超大或持续计算选择 ECS。具体限制、超时行为和 Durable Functions 边界见[流水线设计](pipeline-design.md)。

```text
确定 DWD 输入版本
  → 生成不可变分区文件清单
  → Lambda / ECS 计算任务读取固定输入
  → S3 暂存新 DWS 结果及分块清单
  → 唯一写者分批导入一个未发布 generation
  → 核对分块、水位及目标版本
  → 小事务切换发布指针
```

### 固定输入

为 DWD 发布保留不可变 Parquet 分区版本和 manifest。manifest 列出明确对象键、版本、hash、源水位及转换代码版本；任务运行期间不替换这些对象，也不删除其依赖。

这些文件是应用层长任务协议，增加了存储和发布成本。不能直接将 SlateDB 内部 SST 解释为 Parquet，也不能用对当前数据库目录的普通扫描冒充固定输入。

如果 DWD 转换已在计算任务中完成，可直接使用其已发布 Parquet 输出作为后续 DWS 输入；如果 DWD 只存在数据库内，则需要通过写者实例中的稳定读事务导出明确版本并完成清单发布，再启动下游任务。导出本身要预算资源；不能假设对持续变化的只读 attachment 做一次长导出就获得了未经验证的固定快照。

不将当前扩展约 10 秒的 reader polling 解释为所有 worker 读取同一版本。1.1.0 不依赖尚未确认可用的 Duckherder snapshot pinning。

### 分块导入和短发布

计算结束并不意味着大结果可以瞬间入库。百万行导入仍可能长时间消耗写者资源，因此：

1. 给结果分配独立 `job_id`/`generation_id`，先保持未发布。
2. 根据行数、字节数及测得的事务时间划分 chunk。
3. 每个 chunk 的数据与导入账本在同一个事务中提交，重试按 `job_id + chunk_id` 消重。
4. 写者在接入批次和导入 chunk 之间公平调度，优先控制接入积压，避免某个长作业独占消费循环。
5. 所有 chunk 完成后，验证 hash、行数、输入水位及预期的当前 release，再用小事务切换发布指针。
6. 消费者通过受控视图或指标 API 读取发布版本；未完成的 generation 不开放给产品查询。

发布协议防止半成品暴露，也防止较旧的重算任务覆盖已发布的新水位。冲突作业需要重算或明确合并，不能仅按“最后完成者胜出”发布。

可以给每次导入设置短事务目标，但它是需按实测调优的预算，不是 DuckDB/S3 提供的硬时限。S3 提交、checkpoint、compaction、内存压力与 spill 都可能延长处理时间。

未发布版本按任务状态保留或清理；已发布旧版本在所有使用者及保留规则释放后再回收。作业取消停止计算和后续导入，不能删除正在被读者使用的发布数据。

### 什么时候拆成两个库

如果分块后的分析结果写入仍持续造成接入 SLA 超标，改为：

```text
同一个公开 API
├── core.db       ODS + DWD      一个接入写者
└── serving.db    DWS + ADS      一个分析结果写者
```

两库通过不可变 DWD 文件清单、水位及发布协议衔接；不假设跨库事务。查询可在有权限的同一 DuckDB 会话中 ATTACH 两库，但跨库查询不自动获得全局同一时点快照。

这个方案分离写入资源和生命周期。若必须全系统只有一个物理写者，即使拆库也不能凭空增加写入处理能力。

### 内置流水线、Python 与依赖

定义集成到 Colvenna Isle，Console / API 管理同一份版本化 DAG；Git 中的 YAML、SQL 和 Python 可发布为不可变版本。周期 EventBridge Scheduler、上游发布通知和手动运行进入同一个调度器。调度以权威提交水位为准，运行期间继续接入的数据合并为下一轮待处理区间。

每个任务声明语言、执行器、输入别名、目标 schema、输出替换模式、环境版本和资源 profile。短 SQL 可在写者执行；Python 一律在 Lambda 或 ECS 独立运行。Python 读取 context.json 指定的固定 Parquet，输出 Parquet 与清单；适配器重新核对类型、hash、行数和业务键，再交给唯一写者导入发布。

处理方式按任务独立设置 full、incremental 或 partition_recompute。增量清洗读取变更区间及上一版键状态，全量聚合读取已发布的完整上游快照，分区聚合重算受影响分区；水位仅随成功发布推进。计算增量不代表物理写入只有变更行，generation 构建和输入导出成本仍需预算。详见[增量与全量](processing-modes.md)。

pandas 等包在受控构建阶段解析并锁定全部传递依赖，构建固定 digest 的运行镜像。修改依赖生成新环境版本，当前合同中的单次运行保留其绑定；平台破坏性升级可失效旧运行并重建。外部 Airflow 可通过 API 发起运行并等待 PUBLISHED，一条流水线只有一个自动调度 owner。

完整规则与示例：[内置流水线](pipeline-design.md)、[Python 输入输出](python-task-contract.md)、[包依赖与运行环境](runtime-environments.md)、[orders 示例](../../examples/orders/README.md)。

## 7. AWS 组件选择

| 部件 | 部署选择 | 理由 |
| --- | --- | --- |
| 公开 API | API Gateway HTTP API + Lambda Authorizer | `Authorization: Bearer <API_KEY>` 校验、限流及 API 路由 |
| 接入 | Lambda | 有界、短时的验证和排队 |
| 接入队列 | SQS FIFO + DLQ | 每空间有序消费、缓冲及失败保留 |
| 唯一写者 | 常驻 ECS Fargate Service | 保留句柄和缓存，持续提交/checkpoint/后台整理 |
| SQL 查询 | 常驻 ECS Fargate 查询 Service | 维持可扩缩的 Task 副本，接收内部 ALB 转发的查询请求 |
| 转换执行 | 调度 Lambda → Lambda Invoke / ECS RunTask | 有界计算用批准的 Lambda 版本，长计算用独立 Fargate Task |
| 周期调度 | EventBridge Scheduler → 调度 Lambda | 分钟级唤醒，核对版本、依赖和水位；发布通知降低等待 |
| 环境构建 | CodeBuild 或同等隔离构建服务 + ECR | 构建锁定依赖和适配器，保存镜像 digest 及部署绑定 |
| 对象数据 | 同区域 S3 | 分离 raw、warehouse、dwd-releases、staging、query-results 前缀 |
| 控制状态 | DynamoDB 单表 `colvenna-control` | Key/路由、流水线及环境 metadata、异步运行状态投影 |
| 私网请求分发 | VPC Link + 内部 ALB | HTTP API 私网集成到 ALB 监听器，再转发给查询 Service 的 Task |
| 运维 | CloudWatch | 队列、接入、作业、查询及存储 I/O 指标 |

### API Gateway 到 DynamoDB 和 ECS 的实际路径

| 请求用途 | 调用路径 | 集成与执行方式 |
| --- | --- | --- |
| Bearer Key 认证 | API Gateway → Lambda Authorizer → DynamoDB | Gateway 调用 authorizer；Lambda 用执行角色及 AWS SDK 读取 key 摘要记录，返回授权及身份上下文 |
| Key 发放、吊销及监控 | API Gateway → 管理 Lambda → DynamoDB / CloudWatch | `/v1/admin/*` 使用 Lambda proxy integration；函数核对空间及 scope 后执行控制面读写 |
| 数据写入 | API Gateway → 接入 Lambda → SQS FIFO → 写者 Service | 接入函数先存 S3 原文再排队；写者 Task 主动拉取 SQS |
| SQL、目录及查询取消 | API Gateway → VPC Link → 内部 ALB → 查询 Service | HTTP API private HTTP proxy integration，目标为 ALB listener ARN；ALB 转发至查询 target group |
| 触发流水线 | API Gateway → 控制/调度 Lambda → Lambda Invoke / ECS RunTask | 返回 run_id；按已发布定义启动批准的函数版本或 Task 模板，计算异步执行 |

Authorizer 是请求路由前的认证步骤，批准后 API Gateway 才调用该路由的 Lambda 或 HTTP 集成。Authorizer、管理、调度和接入是独立函数，分别授予所需 IAM 权限。管理函数访问 DDB；调度函数使用受限的 `lambda:InvokeFunction`、`ecs:RunTask` 和 `iam:PassRole`，客户端不能自行指定任意执行角色。

查询 Service 使用 Fargate `awsvpc` 网络，ALB target group 的类型为 `ip`。ECS Service 将 Task 注册到查询目标组，并负责健康替换、部署及扩缩容。VPC Link、ALB 和 ECS 用私网网络及安全组连接；查询 Task 不需要公共入站地址。

API Gateway 将 authorizer context 映射到受控的内部身份头，并覆盖调用者提交的同名头；ALB 和查询 Service 只接收批准的私网入口流量。查询服务仍核对 principal、数据空间和 scope，不将客户端提供的空间或权限字段当作授权依据。

Service 管理常驻 Task 的副本及生命周期，不要求绑定负载均衡器。本方案只有查询 Service 使用内部 ALB。写者 Service 保持一个活动 Task，主动消费 SQS，无需 ALB；长转换使用一次性的 RunTask Task，无需 ALB。

需要严格读到指定 ingestion 提交的查询，由查询服务通过控制面登记的活动写者私网地址调用内部读取 API，该 API 在写者进程内开启受控读取连接。请求绑定活动写者身份，并重新核对空间、scope 和 ingestion 提交；写者切换或地址过期时等待或重试。该私网调用无需为写者增加 ALB。

### 一张 DynamoDB 控制面表

`colvenna-control` 使用 `PK` / `SK` 按实体组织记录：

| 记录 | 主键组织示例 | 内容及权威来源 |
| --- | --- | --- |
| API Key | `PK=KEYHASH#<digest>`，`SK=META` | key_id、授权空间、scopes、到期、吊销状态；控制表为权威来源 |
| Key 管理索引 | `PK=KEYID#<key_id>`，`SK=META`；空间内另建 `KEY#<key_id>` 记录 | key_id 到摘要记录的服务端映射及可见 metadata；不向管理 API 返回摘要 |
| 空间配置 | `PK=SPACE#<space_id>`，`SK=META` | 数据库前缀、服务路由及配额；控制表为权威来源 |
| 流水线与环境 | `PK=SPACE#<space_id>`，`SK=PIPELINE#…` / `PIPELINE_VERSION#…` / `ENVIRONMENT#…` | 草稿 revision、活动版本、调度 owner、包/镜像/部署绑定；完整版本包在 S3 |
| 接入状态 | `PK=SPACE#<space_id>`，`SK=INGESTION#<ingestion_id>` | 写者消费观测和 DuckDB 提交状态的异步投影，包含版本及 observed_at |
| 运行、作业、查询与心跳 | `PK=SPACE#<space_id>`，`SK=RUN#…` / `JOB#…` / `ATTEMPT#…` / `QUERY#…` / `WRITER#…` | 调度、进度、结果地址、写者身份及采集时间；数据库发布状态以 DuckDB 账本为准 |

API Key 签发和吊销在控制表内原子更新权威记录及管理索引。Authorizer 对摘要主键执行强一致读取，并在请求时检查有效期与吊销状态；不能用最终一致索引承担即时吊销判定。

数据接入不写控制面事务，不使用 outbox 或 DynamoDB Streams 派发。运行状态投影可滞后，按状态版本条件更新以避免旧状态覆盖新状态；失败可重试或重建。`ctl.ingestions`、chunk ledger、水位和 release 指针仍与业务数据在 DuckDB 的同一事务中提交。

接入状态在原生事务中分配观测版本和时间，数据库锁外异步投影；公开读取先校验所有者/获授权监控管理员，再通过精确身份的写者核对并条件重建。投影故障不参与排队/业务事务/ACK；认证、写者许可及配置强读失败仍暂停。1.1.0 的接入状态不填写下游层（DWD/DWS/ADS 为 `NOT_IMPLEMENTED`），不声称 PUBLISHED；下游发布进度见流水线运行状态。

Lambda 不承担数据库所有者角色。其运行寿命、冷启动和并发执行模型不适合常驻数据库句柄、缓存和后台整理；限制 Lambda 并发也不能替代完整的数据库所有权与恢复协议。

写者至少采用 `desiredCount=1`，并配置先停旧、后启新的部署方式，例如 ECS rolling deployment 的 `maximumPercent=100`、`minimumHealthyPercent=0`，允许短暂不可用。迁移、恢复任务也必须遵守同一独占协议。desiredCount 本身不是所有故障情形下的独占证明。

租约可用于控制调度，但 DynamoDB TTL/租约过期不代表旧进程已停止访问 S3。接管前要确认旧写者终止；无法确认时暂停接管，避免无意启动两个写者。当前 SlateDB fencing 是最后防线。

1.1.0 的写者用一个 Service-wide 许可约束负责所有已注册空间的进程，该进程最多打开两个原生实例（见[发行说明](../release-notes.md#限制)）。新 Task 只登记心跳，获准前不 ATTACH。许可包含精确 Task/进程/镜像/源码/地址和 epoch；运维撤销旧许可并取得旧 Task 的 STOPPED 终止回执后，才批准新身份。DDB 事务同时保存许可和不可变变更审计，写者 Role 无权发放许可。

运行规格按实测负载确定，1.1.0 的规格见[发行说明](../release-notes.md#规格)。将 DuckDB 缓冲区、SlateDB 缓存、Rust runtime、结果缓冲、本地 spill 和可选持久缓存计入预算；热缓存可重建，S3 数据是持久来源。1.1.0 不使用 ECS on EC2 的本地盘/EBS 缓存；只有实测确有缓存收益时才值得采用。

## 8. Admin Console 与 Query Editor

控制台是产品的统一操作界面，包含“运行概览”“SQL 编辑器”“API Keys”“流水线/作业”：

| 页面 | 产品功能 |
| --- | --- |
| 运行概览 | 写者活动身份、心跳和最近提交；队列积压/最老消息年龄/DLQ；各层水位；作业和查询状态 |
| Query Editor | 数据空间及 schema/table/column 浏览、SQL 编写、执行/取消、结果表、执行时间、实际 release_id、导出 |
| API Keys | 创建、授权空间、scopes、到期时间、一次性显示完整 key、停用及轮换 |
| 流水线/作业 | 定义与 SQL/Python 编辑、DAG、环境构建、版本发布、触发配置、回填、输入版本、导入进度及发布历史 |

### Query Editor

通过只读查询池执行 SELECT/WITH/EXPLAIN 类 SQL。编辑器提供语法高亮、根据有权限目录补全、Ctrl/Cmd+Enter 执行和查询历史；历史与原始 SQL 的保留策略由产品明确配置。

API 提交返回 query_id，前端轮询或订阅状态；取消针对具体查询，不能通过终止唯一写者实现。结果采用行数限制、分页或 S3 导出，显示实际数据版本。服务端负责 SQL 授权和隔离，不能依靠前端隐藏表名或字符串黑名单保障空间隔离。

Query Editor 不直接连接 S3，也不在浏览器存放 AWS credentials。业务 SQL 通过 Bearer API Key 调用 Query API。管理员管理控制台和普通查询 key 的权限分别核对。

### 监控

指标带采集时间，区分 ECS task 运行、进程心跳、数据库成功提交和实际数据发布。没有心跳或数据源不可用显示 UNKNOWN，不用“容器运行中”替代数据库健康。

队列与计算指标读取 CloudWatch/控制状态投影；各层水位读取写者公布的 metadata。监控页面不能频繁触发大规模数据库扫描。

### API Key 管理

创建时指定名称、空间、权限和期限，只在创建回执中展示完整 token；后续列表只显示 key_id/fingerprint、空间、scopes、状态及最近使用时间。管理 API 不返回 key hash。

停用由服务端立即更新 revoked 状态，并按已确定的 authorizer 缓存约定生效。轮换先创建新 key，让调用者切换后再停用旧 key，保留审计关联。

拥有 `keys:manage` 的主体只能在其管理空间内授权，并遵守服务器端可委派权限上限；不能通过请求额外 scope 获得更高权限。首个管理凭证由受控部署引导流程签发，公开服务不开放无认证“创建管理员”接口。

### 接口补充

| 接口 | 权限及返回内容 |
| --- | --- |
| `GET /v1/spaces/{space_id}/catalog` | `query:read`；仅返回有权限的 schema/table/column/type |
| `POST /v1/spaces/{space_id}/queries/{query_id}/cancel` | 查询所有者或授权管理员；返回取消状态 |
| `GET /v1/admin/spaces/{space_id}/status` | `monitor:read`；写者、队列、各层水位、任务及采集时间 |
| `GET /v1/admin/spaces/{space_id}/api-keys` | `keys:manage`；可见 key metadata，不含完整 token/hash |
| `POST /v1/admin/spaces/{space_id}/api-keys` | `keys:manage`；提交名称/scopes/expires_at，返回 metadata + 一次性 token |
| `POST /v1/admin/spaces/{space_id}/api-keys/{key_id}/revoke` | `keys:manage`；停用 key，写入审计 |

控制台部署采用静态前端 S3 + CloudFront，产品 API 仍走 API Gateway。Query Editor 的 `/v1/*` 通过 CloudFront 同源 HTTPS 代理，转发 Bearer Authorization 和查询参数，不转发 cookie、不缓存 API 或错误响应；Gateway 继续强制认证。前端只在页面内存保留 API Key，不写入 localStorage/sessionStorage。Query Editor 使用查询 Bearer Key；管理功能另需管理 scopes，增加人员登录时保持管理 scopes 与数据 scopes 的边界。使用方式见[用户手册](../user-guide.md)。


## 9. 一致性、隔离和存储维护

默认分析读取允许滞后，返回实际数据版本。需要读取刚提交结果的请求，通过写者实例内连接执行，并将 ingestion_id 对应的提交作为前置条件。这类读取要有资源配额，不能把大量重查询转回写者。

一个 SQL READ_ONLY attachment 并不构成完整的租户安全边界。数据空间、IAM、可执行 SQL、外部文件访问、扩展加载和资源配额必须在产品层限制。按数据空间隔离数据库与运行实例，避免共享进程向不同租户暴露任意 SQL/对象存储权限。

S3 权限依实际扩展操作确定；实测 READ_ONLY 进程也会写对象：固定源码使用 SlateDB ManagedCheckpoint 管理读者元数据，因此不能只授予 GetObject。IAM 按实际元数据路径和数据空间限定；SQL READ_ONLY 不等于物理 S3 无写权限。固定 Parquet 输入的计算任务可以单独限定读取前缀和暂存输出前缀。

原始事件归档和 Parquet 发布版本采用应用定义的保留策略。SlateDB 对象数据库前缀由其 compaction/GC 管理，不能套用按对象年龄删除的普通 S3 Lifecycle，以免删除仍被 manifest 引用的数据。S3 Versioning 不能单独替代一致性备份和恢复验证。

## 10. 需要在实际 AWS 中确认的行为

以下行为是本设计的主要正确性与运维风险，只能在实际 AWS 环境中确认，本地检查不能代替。1.0.0 测得的容量与延迟见[性能与容量基线](../performance.md)，用户验收用例见 [UAT 测试用例](../uat/test-cases.md)。是否拆分 core/serving，按真实峰值、迟到修正、故障恢复和成本的测量结果决定。

- 同一请求重复、发送回执丢失、提交后消费回执丢失，业务数据与汇总均不重复。
- 接入 Lambda 无 DynamoDB 写权限时正常接收；已完成认证后，状态投影故障不影响排队和提交，恢复后可重建状态；认证读取失败则拒绝访问。
- 写者在 commit/checkpoint/发布阶段终止后的恢复，以及部署期间的独占性。
- 常驻读者对追加、更新、删除和 schema 变更的可见性，长查询的一致性边界。
- 长聚合期间持续 API 写入的积压、提交延迟、内存/spill，以及分块导入期间的延迟。
- 暂存任务失败或取消后，旧发布版本仍完整可读；新版本不会部分发布。
- 固定输入版本、迟到修正、老作业不能覆盖新水位。
- 临时 AWS 凭证轮换、S3/IAM/KMS 权限、备份恢复、资源隔离及成本。
- Bearer API Key 的创建、吊销、轮换、到期、scope、跨数据空间拒绝及日志脱敏。
- SQL 编辑器的真实查询/取消/分页/导出、授权目录、未知状态展示及管理权限。
- Lambda Authorizer/管理 API 的 DDB 路径、VPC Link/ALB 到查询 Task 的 HTTP 路径、调度 Lambda 的 RunTask 权限，以及内部身份头不能被客户端伪造。
- 流水线 DAG/版本/权限校验、单一调度 owner、发布通知丢失补漏、当前版本的部署/重建/恢复、Airflow 断线重试。
- Lambda 重复执行及 Managed Instances 超时后继续运行（该 profile 尚未实现），旧 attempt 不能发布；Python 类型/业务键/hash/空分区校验、取消与发布竞争。

## 11. 参考来源

- [微信文章：DuckDB 数据库整个搬进 S3，单文件时代结束了](https://mp.weixin.qq.com/s/4esJByANpedtecag3Hceew)
- [微信文章：Rust 再下一城：DuckDB 数据库直接运行在 S3 上，分布式计算架构迎来新思路](https://mp.weixin.qq.com/s/aXa9F2dr37cSJEO6uKczEQ)
- [对象存储扩展 README，固定提交](https://github.com/dentiny/duckdb-object-storage/blob/f096ca13244cb586e04ce04417cadfdd5d18cc0c/README.md)
- [作者原文：A Real DuckDB Database in S3, With Nothing Else to Run](https://dentiny.github.io/posts/duckdb-on-object-storage/)
- [DuckDB ATTACH：跨库事务语义](https://duckdb.org/docs/current/sql/statements/attach.html#transactional-semantics)
- [DuckDB 并发模型](https://duckdb.org/docs/current/connect/concurrency.html)
- [DuckDB 事务与隔离](https://duckdb.org/docs/current/sql/statements/transactions.html)
- [AWS SQS FIFO 行为](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/FIFO-queues-understanding-logic.html)
- [API Gateway HTTP API：Lambda Authorizer](https://docs.aws.amazon.com/apigateway/latest/developerguide/http-api-lambda-authorizer.html)
- [API Gateway HTTP API：私网集成](https://docs.aws.amazon.com/apigateway/latest/developerguide/http-api-develop-integrations-private.html)
- [ECS Service](https://docs.aws.amazon.com/AmazonECS/latest/developerguide/ecs_services.html)
- [Lambda 配额](https://docs.aws.amazon.com/lambda/latest/dg/gettingstarted-limits.html)
- [Lambda Managed Instances 执行环境](https://docs.aws.amazon.com/lambda/latest/dg/lambda-managed-instances-execution-environment.html)
- [Lambda 容器镜像](https://docs.aws.amazon.com/lambda/latest/dg/images-create.html)
- [EventBridge Scheduler](https://docs.aws.amazon.com/scheduler/latest/UserGuide/schedule-types.html)

架构图的确定性验证、内容 hash 和浏览器检查状态见 [architecture-validation.md](architecture-validation.md) 及关联 receipt；这些检查不验证 AWS 运行行为。

DuckDB is a trademark of the DuckDB Foundation. Colvenna Isle is independent of the DuckDB Foundation and is not endorsed by it.
