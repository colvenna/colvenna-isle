# Colvenna Isle 发行说明与支持矩阵

<a id="200-unreleased"></a>
<a id="120-unreleased"></a>
## 2.0.0（未发行）

当前开发版本为 `2.0.0.dev0`；尚未创建发行 tag 或 Release。产品名为 **Colvenna Isle**，副标题为「S3 上的持久化分析数据库」/「Durable analytics databases on S3」，沿用 Colvenna 系列的 C 形流线标识。品牌来源见[品牌说明](assets/README.md)。

本次是 **大版本破坏性迁移**（[#22](https://github.com/colvenna/colvenna-isle/issues/22)）：Python 导入改为 `colvenna_isle`，配置改为 `COLVENNA_*`，HTTP 头改为 `x-colvenna-*` / `X-Colvenna-*`，系统列改为 `_colvenna_*`，Airflow 使用 `ColvennaPipelineRunOperator`。旧名字不再作为别名接受。契约 revision 升到 2、生成文件在 `contracts/v2/`；公开 API 路由仍为 `/v1/`。原生空间格式升到 6，旧格式拒绝读写与备份。镜像内部路径改为 `/opt/colvenna`，固定上游二进制重新打包并更新存储镜像 digest。

AWS 资源沿用 `colvenna-isle-*`。从已发布 1.x 升级必须先用旧版本导出完整 ODS 事件历史与 direct 业务行，并完成旧版本备份；再关闭、卸载旧运行时，全新安装新版本。ODS 的事件 ID、业务源版本与删除事件通过[离线迁移工具](design/identifier-migration.md#离线-ods-重放工具)验证后重放，系统列由新写者重建；direct 业务行通过同步写 API 导入；流水线与键状态重新发布并全量重建。不沿用旧回执、水位或发布 ID，不直接还原旧原生备份，不能原地仅改列名。完整方案与回退边界见[迁移设计](design/identifier-migration.md)和[安装手册](installation.md#10-破坏性升级)。

本地服务、原生数据迁移、构建、示例和浏览器检查各自记录结果；AWS 安装、功能 UAT 与性能验收在实际执行前为 `NOT_RUN`。下文 1.1.0 / 1.0.0 为原样保留的已发布历史记录，其接口名与 PASS 不适用于当前候选。Colvenna 律师检索与商标申请为 `WAIVED_BY_REQUESTER_NOT_PASS`，请求者决定不作为发布门槛。

## 1.1.0（已发布，历史）

1.1.0 在 1.0.0 的基础上支持在标准 AWS 商业区域中部署，并修复了写后读快照在万行级表上超时的问题。本文只列出已在实际 AWS 中验收的合同与能力。安装见[安装部署手册](installation.md)，使用见[用户手册](user-guide.md)，验收用例见 [UAT 测试用例](uat/test-cases.md)。计划中、尚未实现的能力单独列出。

## 1.1.0 的变更

- **区域由部署配置决定**（[#8](https://github.com/colvenna/colvenna-isle/issues/8)）：安装工具读取显式的 `COVE_REGION` / `AWS_REGION`，设置冲突或属于其他分区时拒绝执行。AWS 资源使用栈所在区域，区域也注入运行时服务和原生 S3 secret。运行时资源命名空间包含区域，用来隔离账号级的 IAM 角色；CloudFront OAC 与策略名包含栈区域，所以同一个环境名可以装在多个区域。执行器合同接受各商业区域的 Lambda / ECS / ECR 地址。价目表按实际区域索引，没有该区域的价目表时成本报告返回 `UNKNOWN`，不借用其他区域的价格，也不按零计价。
- **写后读快照改为批量装载**（[#5](https://github.com/colvenna/colvenna-isle/issues/5)）：`after_write` / `after_ingestion` 快照不再逐行装载，改用带类型的 Arrow 批次。写者和查询镜像都安装同一个 hash 锁定的 pyarrow 24.0.0。写者在取任何行之前，先在原读事务中检查行数和保守的 JSON 字节估算，超出返回 `QUERY_SOURCE_QUOTA`。上限仍为 100,000 行、2 MiB，复制预算 5 秒。精确编码、回执权威、不重复捕获和封存 SQL 隔离保持不变。
- **升级是破坏性的**：运行时命名空间变了，1.0.0 的环境不能原地升级。先用 1.0.0 的工具备份并卸载运行时资源，再用 1.1.0 全新安装，按当前合同重新生成数据，见[安装部署手册](installation.md#10-破坏性升级)。

## 验收

tag `v1.1.0` 的运行时代码（`src/`、`infra/`、`runtime/`、`scripts/`、`install.sh`）与下表的验收源码 `7005fee` 相同，只有版本号不同。从该验收源码到历史 tag 的其他变更只涉及文档和测试；当前改名候选的资源与部署代码另有变更，不能沿用这项源码等同结论。

2026-10-21 的区域验收：

| 区域 / 范围 | 状态 | 实际验收内容 |
| --- | --- | --- |
| `us-east-2` | PASS | 两套隔离环境完成全新安装的全部 12 步；全部 56 个功能 UAT 用例：原生约束、精确查询、SQL / Lambda / ECS 流水线、控制台、权限、诊断、恢复、调度、Airflow、保留与配额；时点备份与隔离还原；跨源码升级、中断部署与同制品重部署；隔离环境卸载 |
| `us-west-2` | PASS（回归） | 全新安装的全部 12 步；全新安装 / Quickstart / 查询 / Key / 浏览器回归；实际区域价格与原生服务；两个区域使用同一环境名时 IAM 与全局 CloudFront 资源互相隔离；卸载 |
| `aws` 分区的其他商业区域 | NOT_RUN | 配置和执行器合同接受这些区域，满足[区域前置条件](installation.md#33-区域前置条件)即可安装，但未标为已验收；缺少区域价目表时成本为 `UNKNOWN` |
| 中国区、GovCloud 与隔离分区 | 不在范围内 | 选择区域时直接拒绝 |

`us-east-2` 的完整功能与备份还原验收使用源码 `e2812427ca915d89633ea09ada0dbadcedb1e286`。`us-west-2` 回归与新源码升级使用 `7de98ae4221d17e9e4f8c81db22890c458ac8068`，它在全局 CloudFront 名称中加入了区域。中断部署、重部署与隔离环境清理使用 `7005feeee4ab596e18b8691d4c9c4e6d786f0d3e`，其应用、基础设施、部署与运行时代码与 `7de98ae` 相同。只读的 AWS 观察确认了：区域化的数据桶与浏览器桶、注入的区域、两个 2 vCPU / 4 GiB 原生服务固定的 ARM64 镜像，以及各区域实际的价目表快照。

隔离还原与备份时点的独立 ODS 导出一致，排除了备份时点之后的 21 次提交，随后恢复写入和流水线发布，权限照常生效，源空间的注册信息不变。首次查询用时 146.859 秒，流水线发布 317.738 秒。200,000 行的公平导入验收在 1,793 秒内发布，期间 174 次接入提交与 154 次导出照常推进，观测到的最大接入提交延迟为 11.3 秒。这些是这几轮验收中的测量值，不是服务保证。卸载保留了各区域的账号级备份库。

写后读快照（#5）于 2026-10-21 在一套全新的 `us-west-2` 环境中做了针对性回归，源码 `69201586e526ca5a2c507919a49cd9ba52587eba`，查询 Task 为 2 vCPU / 4 GiB。通过公开 API 查询 10,210 行、4 列的表，聚合结果与独立计算一致：`after_write` 6.751 秒，`after_ingestion` 3.839 秒，`current` 2.884 秒。超过 2 MiB 的源在 2.766 秒内返回 `QUERY_SOURCE_QUOTA`。这些耗时包含客户端轮询，不是延迟保证。本地原生检查另外覆盖了 100,000 行的捕获 / 装载预算、精确类型和复制前拒绝。

验收中失败的安装与部署回执与成功的验收分开保留：VPC 配额失败和全局 CloudFront 名称冲突都已核对到终态的回滚 / 删除；一次 Airflow CLI 设置失败被证明发生在 Colvenna Isle 派发之前，修正字面参数后原 DAG 身份继续执行，没有重新接入数据。

发行构建从 tag `v1.1.0` 的干净检出构建，在一个区域全新安装后执行 Quickstart、上述两项变更的针对性回归与发行用例 `UAT-DOC-02` / `UAT-DOC-03`，然后卸载。结果见 [GitHub Release v1.1.0](https://github.com/colvenna/colvenna-isle/releases/tag/v1.1.0) 附带的验证报告。

## 发行身份

| 项目 | 值 |
| --- | --- |
| 版本 | `1.1.0`（tag `v1.1.0` 中的 `src/cove_duckdb/_version.py`；当前源码为 `1.2.0.dev0`） |
| Git tag | `v1.1.0` |
| GitHub Release | [v1.1.0](https://github.com/colvenna/colvenna-isle/releases/tag/v1.1.0) |
| 源码 | tag 指向的提交；release `manifest.json` 记录其 commit / tree |
| 构建 | `scripts/build_product.py`，见[安装部署手册](installation.md#5-构建-release) |

release `manifest.json` 记录以下内容：

- 产品版本与源码 commit / tree；
- 每个组件 manifest 的 SHA-256 与镜像 ID；
- 原生存储镜像的仓库名与 digest。

组件包括：

- control ZIP：control / intake / writer / query 与流水线套件；
- writer 和 query 镜像；
- 环境构建器源码；
- Web：Query Editor 与 Admin Console。

GitHub Release 附带这些 manifest。镜像和 ZIP 本体由安装工具发布到目标账号的 ECR 与制品桶。

## 支持矩阵

| 维度 | 支持 | 说明 |
| --- | --- | --- |
| AWS 账号 | 单个账号 | 账号 ID 不写在源码中，由部署环境决定（`AWS::AccountId` / `COVE_ACCOUNT`）。验收在一个账号中完成，其他账号未单独验证 |
| 区域 | 标准商业分区 `aws`，部署时显式指定（`COVE_REGION` / `AWS_REGION`） | `us-east-2` 完成全部功能验收，`us-west-2` 完成回归；其他商业区域未单独验收，见[区域前置条件](installation.md#33-区域前置条件)；中国区、GovCloud 与隔离分区不支持 |
| CPU 架构 | ARM64（Graviton） | writer、query、计算 Task、Lambda 与运行环境构建都只构建 ARM64 |
| 原生存储 | DuckDB 1.5.6、duckdb-object-storage `f096ca1`、httpfs、aws 扩展 | 二进制 hash 固定于 [storage-lock.json](../runtime/storage-lock.json) |
| 镜像内 Python 依赖 | writer 与 query 镜像含 hash 锁定的 pyarrow 24.0.0 | 用于写后读快照的批量装载 |
| Python 运行环境 | CPython 3.12，linux_arm64 wheel | 依赖在发布前解析、按 hash 锁定，只接受 wheel |
| 执行器 | writer（`writer_short`）、标准 Lambda、ECS Fargate Task | Lambda Managed Instances 与 Durable Functions 尚未实现 |
| 浏览器 | Chromium 148（Playwright 1.60.0） | Query Editor 与 Admin Console 在该浏览器中验收；其他浏览器未验证 |
| 部署 | 每个环境一组栈，同一账号可在一个或多个区域装多个环境 | 见[安装部署手册](installation.md#2-部署组成) |

## 规格

| 组件 | 规格 |
| --- | --- |
| 写者 | ECS Fargate，每个环境 1 个 Task，2 vCPU / 4 GiB / 21 GiB 工作盘；DuckDB `memory_limit` 512 MB、2 线程；每个进程最多同时打开 2 个数据库 |
| 查询 | ECS Fargate Service，2 vCPU / 4 GiB / 21 GiB；每条查询在新进程、新原生实例中执行，不跨查询保留缓存 |
| 同步写代理 | VPC Lambda，256 MB，28 秒 |
| 接入 | Lambda，256 MB，60 秒；SQS FIFO |
| API | API Gateway HTTP API，默认路由限流 10 rps，突发 20 |
| 计算 profile | `writer_short` 30 秒；`*_lambda_standard` 900 秒；`*_ecs_medium` 1 vCPU / 4 GiB，最长 86,400 秒 |

## 能力

| 领域 | 能力 | 文档 |
| --- | --- | --- |
| 存储 | S3 上的原生 DuckDB 数据库、事务恢复、唯一写者与接管 | [AWS 产品设计](design/aws-product-design.md) |
| 访问 | Bearer API Key、空间与 scope 授权、委派、轮换、吊销 | [用户手册](user-guide.md)、[API 合同](design/api-contract.md) |
| 接入 | 不可变接入、ODS 幂等账本、接入状态、背压、DLQ 与受控重放 | [用户手册](user-guide.md) |
| 同步写 | 直接写表的 insert / upsert / update / delete、原生约束、原回执与 UNKNOWN 核对 | [同步写](design/synchronous-writes.md) |
| 查询 | 私网查询服务；current / published / 指定 release / after_write / after_ingestion 一致性 | [用户手册](user-guide.md) |
| 流水线 | SQL / Python DAG、固定输入、增量 / 分区重算、调度、取消、重试、Airflow | [流水线设计](design/pipeline-design.md)、[Python 任务合同](design/python-task-contract.md) |
| 控制台 | Query Editor、Admin Console | [用户手册](user-guide.md) |
| 治理 | 审计与诊断、保留与回收、配额、公平调度、成本 | [用户手册](user-guide.md) |
| 运维 | 安装、破坏性升级、开放 / 关闭、卸载；一致性备份与隔离还原；分段计时 | [安装部署手册](installation.md)、[性能报告](performance.md) |

## 限制

- **单一写者**：每个数据库只有一个写者。接入提交、同步写、导入和发布都在这个写者上排队，按公平调度交替执行。
- **活动空间上限**：每个环境只有一个写者进程，它在进程生命周期内最多打开 2 个空间的数据库。第三个空间的写入、接入提交或流水线导入返回 `503 WRITER_DATABASE_LIMIT`，直到写者重启（`admission.py close` 后再 `open`）。需要更多活动空间时，请部署多个环境。
- **外键与索引**：DuckDB 1.5.6 中，被外键引用的行，其索引列（显式索引、PRIMARY KEY、UNIQUE）和 LIST 列（`VARCHAR[]`）的值不能改变。这类更新以 `NATIVE_CONSTRAINT` 整批拒绝，其他列可以正常更新（见[同步写](design/synchronous-writes.md#2-注册权限与原生约束)）。
- **请求上限**：
  - API 请求体 1 MiB；
  - 一次接入最多 10,000 条记录；
  - 一次同步写最多 1,000 项，`timeout_ms` 最大 20,000；
  - SQL 文本最多 64 KiB。
- **查询**：
  - 每次查询只暂存 SQL 引用的数据集，暂存上限 2,000,000 行，按被引用数据集的全部行计算（与 `WHERE` 无关），超出返回 `QUERY_STAGING_LIMIT`；
  - 每个空间同时只执行 1 条查询，最多 8 条排队或执行中；整个环境最多 32 条排队、4 条同时执行；
  - 结果最多 10,000 行，保留 10 分钟；
  - `after_write` / `after_ingestion` 快照上限为全部引用数据集合计 100,000 行、2 MiB，复制预算 5 秒。复制前在同一读事务中检查行数及保守的 JSON 字节估算，超出返回 `QUERY_SOURCE_QUOTA`，不复制任何行。大表的分析查询仍建议用 `current`。
- **查询结果保留配额**：整个环境最多保留 64 个结果（合计 64 MiB，每个空间 32 MiB），每个结果保留 10 分钟后回收。配额已满时，查询执行完会以 `429 QUERY_RESULT_RETENTION_QUOTA` 失败，所以持续查询吞吐约为每 10 分钟 64 个。
- **流水线输入**：每个任务最多 16 个输入；每个输入最多 100,000 行，一次输入导出最多 32 MiB，超出时以 `PARQUET_ROWS_QUOTA` / `PARQUET_BYTES_QUOTA` 失败。
- **备份**：备份在原生锁下导出全部表，期间写入需要等待；不支持跨产品版本还原。
- **升级**：不提供向后兼容、旧数据自动迁移或零停机升级。升级时先关闭，再部署新版本；已发布的流水线版本需要重新发布（`PIPELINE_VERSION_BINDING_CHANGED`）。从 1.0.0 升级到 1.1.0 必须先用 1.0.0 的工具备份并卸载，再全新安装（运行时命名空间变了，见上文）。
- **区域**：每个环境部署在一个区域；不提供跨区域复制、多区域读写或故障切换。
- **写入高峰下的查询**：写入高峰期间或刚结束时，读取同一张表的分析查询会明显变慢，可能超过 30 秒的默认时限（`QUERY_TIMEOUT`）。这时应设置更长的 `timeout_ms`（最大 300,000），或者错开写入高峰。
- **逐行执行**：同步写和长任务导入仍逐行执行语句，1,000 行的同步写约 5 秒，导入约 150 行/秒。
- **无服务目标**：没有约定的 SLA、RPO 或 RTO 目标。实测值只是上述规格下的基线，见[性能与容量基线](performance.md)。

## 测得基线（摘要）

以下是 1.0.0 在上述规格下的实测值（1.1.0 规格相同，未重测；`us-west-2`），详见[性能报告](performance.md)：

- **同步写**：
  - 单行请求约 1.0 秒；
  - 写者串行提交，吞吐饱和在约 2.0–2.4 次提交/秒；
  - 每批 1,000 行时约 240–280 行/秒。
- **异步接入**：
  - 每批 1,000–5,000 条时约 570–1,970 条/秒，事务单元 0.6–1.9 秒；
  - 1 条一批时每秒 1–2 个批次。
- **查询**：
  - 每条查询使用新进程，服务端耗时 p50 约 4.2–5.7 秒，其中原生打开约 3 秒；
  - 结果保留配额实测为 64 个。
- **备份与还原**：
  - 备份约 15 秒（6.3 万行）；
  - 隔离还原约 3.5 分钟后可以查询，约 6 分钟后流水线重新发布。
- **成本**：写者与查询两个常驻 Task 约 0.158 USD/小时（公开单价，不含共享基础设施）。按数据档位的每月估算见[成本估算](cost-estimate.md)。

## 计划中，尚未实现

- Lambda Managed Instances 执行 profile。
- Durable Functions 多步骤任务。
- 按实测压力评估 core / serving 拆库。
- 多 DuckDB 分片与独立写者（只是方向，尚未设计）。

## 1.0.0（历史）

| 项目 | 值 |
| --- | --- |
| 版本 / tag | `1.0.0` / `v1.0.0`（[GitHub Release](https://github.com/colvenna/colvenna-isle/releases/tag/v1.0.0)，2026-10-20） |
| 区域 | 只支持 `us-west-2`：区域写在模板和服务代码中 |
| 写后读快照 | 沙箱逐行装载，约 1 万行、4 列的表超过 30 秒查询时限，返回 `QUERY_TIMEOUT` |

其余支持矩阵、规格与限制与 1.1.0 相同。1.0.0 的环境不能原地升级到 1.1.0，见上文「升级」。

## 许可

项目源码、文档与品牌资产使用 [Apache-2.0](../LICENSE)。第三方组件（DuckDB、扩展、Python 依赖、基础镜像、Archify 等）保留各自的许可，见 [THIRD_PARTY_NOTICES.md](../THIRD_PARTY_NOTICES.md)。构建产物另附所含 Python 发行包自带的许可文件。

DuckDB is a trademark of the DuckDB Foundation. Colvenna Isle is independent of the DuckDB Foundation and is not endorsed by it.
