# 内置流水线：SQL、Python 与外部编排

适用版本：2.0.0（tag `v2.0.0`）；本文使用当前标识与契约，验证范围见 [2.0.0 UAT 报告](../uat/v2.0.0-report.md)。下文沿用的 1.1.0 行为说明适用于 2.0.0，历史 AWS 结果不计入本版本。定义与版本、运行与 DAG、调度、取消与重试、SQL / Python 任务、固定输入、原生发布与 Airflow 编排均已实现，使用方式见[用户手册](../user-guide.md)。任务示例用于验证数据合同。

合同版本 2 的[机器合同](../../src/colvenna_isle/contracts/v2/contracts.schema.json)、[profile 与准入上限](../../src/colvenna_isle/contracts/v2/profiles.json)和[语义校验器](../../src/colvenna_isle/contracts/validation.py)明确准入上限、输入/输出字段和状态规则；JSON Schema 通过仍需语义及真实资源校验。writer 任务省略 runtime_environment/input_mode，使用平台固定环境和原生版本绑定；外部任务显式声明这些字段。

升级是停机的破坏性升级，见[安装部署手册](../installation.md#10-破坏性升级)。本文的固定版本约束适用于单次运行及当前合同，不要求旧产品版本的定义或运行继续工作。

## 1. 产品边界

流水线定义、版本发布、依赖、水位、执行、重试和结果发布集成到 Colvenna Isle。Console 的“流水线”页面和 Pipeline API 操作同一份定义；Git 中的 YAML、SQL 和 Python 文件也可通过 API 发布。

Airflow 是可选的外部编排方。它负责跨系统依赖，调用 Colvenna Isle 的运行 API 并等待发布结果；Colvenna Isle 继续负责固定输入、计算、唯一写者导入及发布。用户不需要部署 Airflow 才能完成 ODS → DWD → DWS → ADS。

一条流水线的自动触发方由 `schedule.owner` 明确选择 `colvenna`、`external` 或 `none`。选择 external 时关闭 Colvenna Isle 的自动运行创建；Colvenna Isle 仍执行该流水线内部 DAG。`none` 用于只允许手动运行。切换触发方使用版本条件更新，避免两边同时自动启动。

## 2. 定义与发布

可运行的示例源文件见 [orders/pipeline.yaml](../../examples/orders/pipeline.yaml)。其依赖为：

```text
ODS orders
  → clean_orders（SQL）→ DWD orders
      → score_orders（Python / pandas）→ DWD order_scores
      → daily_sales（SQL，依赖 clean_orders 和 score_orders）→ DWS daily_sales
          → dashboard（SQL）→ ADS dashboard
```

任务定义字段：

| 字段 | 含义 |
| --- | --- |
| `task_id`、`depends_on` | 稳定任务标识及 DAG 依赖，依赖必须存在且无环 |
| `language` | `sql` 或 `python` |
| `processing` | 每个任务独立选择 full / incremental / partition_recompute，详见[增量与全量](processing-modes.md) |
| `executor` | `writer`、`lambda` 或 `ecs_task` |
| `entrypoint` | 版本包内的 SQL 或 Python 相对路径 |
| `inputs` | 输入别名到授权数据集、当前运行上游输出或批准的目标状态，附带选择方式 |
| `output` | 注册的目标数据集、schema、写入方式、业务键、空结果策略 |
| `runtime_environment` | `{environment_id, version}` 指定同空间已 READY 的不可变环境；writer 使用平台绑定环境 |
| `resource_profile` | 平台批准的内存、CPU、临时存储、函数/任务模板与权限配置 |
| `input_mode` | `local_files` 或 `s3_files`；需与脚本支持方式及资源预算匹配 |
| `timeout_seconds`、`max_attempts` | 执行期限及总尝试次数，上限受空间和执行器配置约束 |

定义中的 `snapshot` 是指定水位或发布版本的完整输入；它不表示任意读取最新数据。`affected_partitions` 读取经过确认的受影响分区完整快照，用于分区重算。多输入任务绑定一份明确的版本向量；跨数据库输入不自动具有全局同一时点快照。

[同步表写合同](synchronous-writes.md)将数据集写入 owner 分为 direct、ingestion 和 pipeline，该字段属于注册机器合同。流水线导入与发布只允许写入 pipeline 目标，不能覆盖直接写表或接入目标。任务可将授权 direct 表作为输入，但须由写者绑定固定原生状态并形成不可变输入或合格的变更区间，不能在计算中持续扫描变化的当前表。同步写成功不推进流水线任务水位或宣称结果已发布。

保存和发布流程：

1. 创建草稿，上传定义、SQL/Python 文件及依赖环境引用。
2. 校验 DAG、输入授权、目标 schema、运行参数、资源预算和执行器兼容性；SQL 是产出关系的查询，写入操作由产品管理。
3. Python 环境必须完成构建并处于 READY；发布锁定实际镜像、包锁文件、代码 hash 和执行配置。
4. 先保存完整的不可变 S3 版本包，核对文件清单与 hash，再条件更新 DDB 的 active_version。
5. 启用调度。同一当前合同下，已有运行使用启动时绑定的版本；可重新激活仍有效的定义版本，不修改其内容。平台破坏性升级可以停止/失效旧运行并重建，不提供跨产品版本兼容或自动迁移。

草稿为 DRAFT，校验失败为 INVALID，发布版本为 PUBLISHED；流水线启用状态为 ENABLED / PAUSED / ARCHIVED。暂停只停止新运行，取消已有运行使用单独接口。归档保留审计与运行引用。

## 3. 执行器选择

| 语言与执行器 | 用途及限制 |
| --- | --- |
| SQL + writer | 已通过数据量与时间预算校验的短转换；在唯一写者内执行受控 SQL 和提交 |
| SQL + lambda | 有界 Parquet 计算，使用批准的 DuckDB Lambda 运行环境，产出暂存文件 |
| SQL + ecs_task | 大型关联、聚合、重算，RunTask 启动独立计算 Task |
| Python + lambda | 有界 Python 算法、规则和 pandas 处理，Invoke 启动批准的固定函数版本 |
| Python + ecs_task | 长计算、大内存或大临时文件任务，使用独立运行环境 |
| Python + writer | 不支持；Python 代码不进入唯一写者进程 |

执行配置由平台绑定 AWS 资源；调用者不能提交任意 function ARN、task definition、镜像或 IAM role。Lambda 调度绑定函数的已发布数字版本，ECS 调度绑定模板 revision 和镜像 digest。用户选择的是批准的 profile。

当前 AWS 文档（查阅日期 2026-10-15）说明，普通 Lambda 单次最多 900 秒；Lambda Managed Instances 的异步调用或符合条件的 event source mapping 最多 5,400 秒，Amazon MQ / DocumentDB 的相关入口不适用该延长时限。`executor: lambda` 的 profile 必须明确 `standard` 或 `managed_instances`，不能仅凭 executor 名称接受 90 分钟配置。1.1.0 只提供 standard profile，Managed Instances profile 尚未实现。

Managed Instances 在客户拥有的 EC2 上运行，并可能在超时响应后继续执行代码，因此不能把 TIMEOUT 当作旧计算已停止的证明。不同 invocation 使用独立工作目录，失效 attempt 永远不能发布。其成本、并发、终止及隔离行为尚未验证。

Lambda Durable Functions 支持最长一年的可恢复工作流，但这不是一段 Python 计算可以连续运行一年的承诺。Colvenna Isle 的主 DAG 由自身管理；1.1.0 尚未提供 durable profile。durable profile 如用于多步骤任务，需记录 durable execution identity，约束 checkpoint/replay 的副作用，最后仍执行相同的产物校验和发布协议。

执行器不会因为一次运行超时就直接换一种执行器重放。要先确认原尝试的输出及发布结果，并以新的 attempt 和固定输入重试；迁移 executor 需要新定义版本或明确的受控运行配置。

## 4. 触发与水位

1. 写者在同一 DuckDB 事务中提交 ODS 和单调递增的接入序号。加工进度使用提交序号或发布版本，不用业务事件时间代替提交水位。
2. EventBridge Scheduler 周期唤醒调度 Lambda，例如 `rate(1 minute)`；调度器核对活动定义、DAG 和已处理水位，决定是否创建运行。
3. 上游发布后可异步唤醒同一调度器，降低下游等待；周期扫描负责发现丢失的通知、待运行任务及状态投影缺口。
4. Console / API 的手动、回填和 external 触发进入同一运行准入协议；不会绕过权限、输入固定或发布检查。

EventBridge Scheduler 是分钟级调度，不承诺亚分钟加工 SLA。停止定时调度后，已有发布通知只推进已创建的运行，不偷偷创建新的自动运行。

调度决定以写者的权威水位及发布账本为准，DDB 中的水位和发布状态是异步投影。投影缺失或有争议时，通过写者内部 metadata API 核对；不能凭“暂无投影”跳过数据或把下游视为就绪。

默认 `max_active_runs=1`。运行过程中继续到达的 ODS 数据合并为 pending 水位区间；完成当前运行后，从已处理水位推进到待处理水位。合并不会丢弃中间变更。迟到、更新和删除事件必须计算受影响的业务键/分区，不能只处理最新一天。

根输入在运行开始时固定到完整 manifest；下游 `inputs.<alias>.task` 只绑定同一次运行中已 PUBLISHED 的上游结果。上游 ECS 退出或 Lambda 返回成功，仅表示计算结束，不足以推进下游。

## 5. 运行、重试与发布

`run_id` 标识流水线运行，`job_id` 标识其中一个任务，`attempt_id` 标识一次计算尝试。手动/external 请求使用 Idempotency-Key；调度触发由定义版本、源水位区间和触发窗口形成稳定准入键。

任务状态为：

```text
BLOCKED → READY → DISPATCHING → RUNNING → STAGED → VALIDATING
  → IMPORTING → PUBLISHED
```

异常分支为 RETRY_WAIT、FAILED、CANCELLING、CANCELLED 和 UNKNOWN。PUBLISHED 是成功终态；FAILED / CANCELLED 保留已经发布的上游结果。状态投影中的 UNKNOWN 需要核对 AWS 执行 identity、输出和 DuckDB 账本，不等于任务不存在。

调度 Lambda 负责短时准入和 AWS 调用，不保持 HTTP 连接等待计算。RunTask 记录 task ARN，Invoke 记录 invocation/执行关联 ID；ECS 状态事件、Lambda 完成通知和周期核对用于更新计算状态。ECS STOPPED 时仍检查容器退出码及输出；Lambda 的异步 Invoke 接收成功仍不等于函数完成。

重复派发不能被描述为 exactly-once 计算。ECS 使用受支持的 dispatch token；Lambda 可能重复执行。执行适配器为每个实际 invocation 分配独立 execution/output 目录，调度器条件选定一个有效候选。旧 attempt 和未选中的 invocation 的产物不得发布。

计算重试绑定同一代码、依赖环境、输入版本和参数，输出使用新 attempt 目录。产品任务默认是可重试的数据变换；对外部系统的写入需独立、显式的权限和幂等方案，DuckDB 发布事务不会回滚外部副作用。

计算完成后的协议见 [Python 输入输出合同](python-task-contract.md)：

1. 校验所有 Parquet、schema、业务键、文件 hash、行数及分区覆盖。
2. 上传不可变 attempt 产物，绑定 S3 object version/hash，条件选定候选，形成可信提交清单。
3. 唯一写者将结果分块导入未发布 generation，chunk 数据与账本在同一事务中提交。
4. 核对 expected_previous_release、输入水位、有效 attempt 和完整分块，再用短事务切换发布指针及处理水位。
5. 只有该发布事务成功，才将任务视为 PUBLISHED，放行下游。

写者短 SQL 可省去外部 Parquet 暂存，但必须保留相同的任务身份、输入版本、schema 校验、水位和原子提交语义。输入别名绑定明确 release 的 generation；ODS 输入按固定提交序号读取保留的不可变事件，再计算对应快照，不能把别名绑定到正在变化的“最新”视图。事务结果不明时先查账本，不能直接执行第二遍。

**发布资格归写者账本控制。** 调度器通过授权的内部命令向写者登记 job、有效 attempt、选定 execution、单调 eligibility_epoch 和 expected_previous_release，写者在 `ctl` 中提交后才接受结果导入。替换 attempt 或取消时，命令在同一权威账本中提升 epoch / 撤销资格；每个导入及发布事务核对这份记录，DDB 状态或租约不能单独批准发布。发布与撤销竞争由写者事务顺序决定，事务确认丢失先核对账本，不能宣称取消成功。内部命令与接入批次公平调度，不让控制操作绕过唯一写者。

取消在发布前阻止新计算、导入和发布，并撤销 attempt 的发布资格。若发布事务先完成，接口返回既有 PUBLISHED，不能将已经发布的数据标成 CANCELLED。发布是每个任务/数据集的边界，整个 DAG 不提供跨任务全局事务；需要最终一致结果的消费者读取 ADS 发布版本。

## 6. 输出更新语义

| 写入方式 | 语义 |
| --- | --- |
| `versioned_replace` | 输出是目标数据集的完整镜像；导入新 generation，再切换版本 |
| `replace_partitions` | 输出是已声明受影响分区的完整镜像；未受影响分区继续引用旧版本 |
| `merge_keys` | 输出每个受影响业务键的最终 UPSERT / DELETE；绑定上一发布版本，构建新 generation 后发布 |

分区替换清单必须列明所有待替换分区，即使某个分区变为空；否则删除或退款可能留下旧数据。发布清单原子更新分区到 generation 的映射，用户通过受控视图读取当前映射，避免半成品可见。

`allow_empty` 明确允许或拒绝替换后的空结果；merge_keys 的零条变更只是 no-op，不触发“清空目标”判定，若删除后目标确实为空则核对该策略。`merge_keys` 使用显式操作列及键状态，不能直接把普通结果当作 upsert。原始接入 API 的 append/upsert/delete 与 pipeline 的发布协议分别校验。增量任务可以只重算受影响分区，但不能把一个批次的部分聚合直接覆盖整个主题表。

## 7. 定义、环境和状态存在哪里

继续使用一张 `colvenna-control` 表：

| `PK=SPACE#<space_id>` 下的 SK | 内容 |
| --- | --- |
| `PIPELINE#<generation>#<pipeline_id>` | metadata/draft revision、启用状态、active_version、触发 owner |
| `PIPELINE_VERSION#<generation>#<pipeline_id>#<version>` | 不可变定义包 URI/hash、环境及执行配置绑定 |
| `UPLOAD#<generation>#<pipeline_id>#<upload_id>` | 限定路径/hash/字节数的短时上传准入 |
| `PIPELINE_PUBLICATION#<generation>#<pipeline_id>#<version>` | 原始发布请求、固定快照和 PUBLISHED 回执 |
| `RUN#<run_id>` / `JOB#<job_id>` / `ATTEMPT#<attempt_id>` | 调度、执行关联、候选产物和观测时间 |
| `IDEMPOTENCY#<request_digest>` | API 请求身份及内容 hash，防止同一 key 复用不同请求 |

代码、SQL、锁文件、输入和产物保存在各自 S3 前缀；执行镜像保存在 ECR。DuckDB `ctl` 保留真实提交、水位、chunk、generation 和 release 账本。控制表不承担主数据接入的 outbox。

环境服务当前使用 `PK=ENVIRONMENT#1#<space>#<generation>, SK=META` 的有界不可变环境目录，与定义条目共用同一张表。

## 8. Console 与 Airflow

“流水线”页面提供定义列表、SQL/Python 编辑器、输入输出目录、依赖预览、参数、运行环境构建记录、发布/启停及按版本回填。定义校验和实际数据样本试运行分开显示；试运行产物不自动进入正式发布版本。

Query Editor 仍用于授权的只读分析。定义修改要求 `pipeline:manage`，运行要求 `pipeline:run`，环境创建要求 `runtime:manage`；这些权限分别检查空间及可委派上限。

Airflow 使用专用 Bearer Key，以幂等键提交指定 pipeline_version 和根输入版本，取得 run_id，轮询运行接口直到 PUBLISHED，再推进外部下游。超时后先按同一幂等键查询/重试；请求重试不会创建第二份业务运行。Airflow timeout 或调度进程退出不会自动取消 Colvenna Isle 作业。

新运行接口、发布、调度 owner、环境 API 见 [API 契约](api-contract.md)。Airflow 负责外层工作流，Colvenna Isle 内部任务的 SQL/Python 版本及发布规则只维护一份。

## 9. 验证范围

定义/权限/DAG 校验、调度补漏、Lambda 重复调用、RunTask 结果不明、代码/环境升级、Python 输入输出校验、取消与发布竞争、写者故障恢复、跨租户隔离和 Airflow 断线重试只能在实际 AWS 环境中确认。

本仓库的[本地示例](../../examples/orders/README.md)不调用 AWS，只验证 SQL/Python 转换及文件合同。Lambda Managed Instances 与 Durable Functions profile 尚未实现。

来源：[Lambda 配额](https://docs.aws.amazon.com/lambda/latest/dg/gettingstarted-limits.html)、[Managed Instances 执行环境](https://docs.aws.amazon.com/lambda/latest/dg/lambda-managed-instances-execution-environment.html)、[Durable Functions](https://docs.aws.amazon.com/lambda/latest/dg/durable-functions.html)、[EventBridge Scheduler](https://docs.aws.amazon.com/scheduler/latest/UserGuide/schedule-types.html)。

DuckDB is a trademark of the DuckDB Foundation. Colvenna Isle is independent of the DuckDB Foundation and is not endorsed by it.
