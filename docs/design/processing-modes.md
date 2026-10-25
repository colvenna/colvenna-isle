# 每个任务独立选择增量、分区重算或全量

适用版本：2.0.0 候选；本文使用当前标识与契约，AWS 验收仅属于历史 1.1.0。处理模式是任务级配置，SQL/Python、Lambda/ECS 都适用。模式不会由平台仅根据 SQL 中出现 GROUP BY 或 agg 自动推断。

[机器合同 v2](../../src/colvenna_isle/contracts/v2/contracts.schema.json) 批准 full + versioned_replace、源版本/键状态 incremental + merge_keys、partition_recompute + replace_partitions。1.1.0 尚未定义增量生成完整镜像或通用聚合状态所需的完整状态输入/输出合同，发布校验拒绝未批准的组合。

## 1. 计算范围与输出更新分开定义

| processing.mode | 输入和计算范围 | 常用 output.write_mode |
| --- | --- | --- |
| full | 固定版本的全部输入，每次重新计算完整结果 | versioned_replace |
| incremental | 未处理水位区间内的变更，必要时加上一版键状态/聚合状态 | merge_keys；有完整状态时也可 versioned_replace |
| partition_recompute | 先从变更找出受影响分区，再读取这些分区的完整固定快照重算 | replace_partitions |

清洗可增量；全局复杂聚合可全量；按天聚合通常可重算受影响日期。SUM/COUNT 等在有正确撤回状态时也能增量计算。更新、删除、精确 distinct、窗口排名等需要不同状态，不能把上一批的 sum 直接与本批相加。

任务定义声明模式、所需状态、首次运行策略和迟到事件策略。平台校验输入 selection 与输出模式兼容性；无法证明正确的受影响范围或状态不可用时，按声明的 full 回退策略执行，或拒绝运行。回退记录在运行历史中，不悄悄改成跳过。

## 2. 同一条 DAG 可以混合

下面是设计配置片段；完整 orders 演示为小数据全量重算，另附[增量清洗 SQL](../../examples/orders/sql/clean_orders_incremental.sql)：

```yaml
tasks:
  - task_id: clean_orders
    language: sql
    executor: writer
    processing:
      mode: incremental
      bootstrap: full
      on_missing_state: fail
    inputs:
      changes: {dataset: ods.orders, selection: changes}
      key_state: {state: target_key_state, selection: affected_keys}
    output:
      dataset: dwd.orders
      write_mode: merge_keys
      key_columns: [order_id]

  - task_id: daily_sales
    language: sql
    executor: ecs_task
    depends_on: [clean_orders]
    processing: {mode: full}
    inputs:
      orders: {task: clean_orders, selection: snapshot}
    output:
      dataset: dws.daily_sales
      write_mode: versioned_replace
```

此片段省略 schema、环境、profile、entrypoint 等必填发布字段。clean_orders 只处理新变更；发布后形成完整 DWD release；daily_sales 读取该 release 的完整快照。上游的 delta 文件不能冒充下游 full 输入。

也可以将 daily_sales 改成 partition_recompute：定义中声明授权分区列及其映射，平台绑定受影响日期和完整分区快照，任务输出这些日期的完整聚合。将订单从周一改到周二，需要重算两个日期；删除订单后变空的周一也必须在替换清单中出现。

## 3. 增量输入与状态

`selection: changes` 绑定区间 `(from_watermark, to_watermark]`，顺序按提交序号，不按业务时间。上游变更清单标明 UPSERT/DELETE、业务键、源版本、提交序号，必要时包含 before/after 分区值。原生数据或发布结果未提供这些信息时，由受控发布差异生成，不能凭输出文件名猜测变更。

`target_key_state` 是写者提供的上一目标 release 的键状态，覆盖本次受影响键，包含已经删除的键及其最高业务版本。它不是让用户脚本直接访问 ctl。普通上一版业务表不含 tombstone，不能独自阻止迟到旧版本复活已删除记录。

增量清洗按 `(source_version, ingestion_sequence)` 选择每个键最新事件，并与绑定的 key_state 比较。新到达的旧 source_version 不覆盖较新的业务状态。首次运行必须 full bootstrap，构建数据和版本/tombstone 状态后，才能开始增量。

bootstrap 使用固定的完整事件历史与空键状态初始化；删除键也进入 tombstone 状态。无法获得完整历史或合格的初始状态时拒绝 bootstrap。代码、参数或状态 schema 改变时，使用完整重算/重新 bootstrap。不迁移旧状态，也不提供向后兼容；破坏性升级后按新 generation 重建状态，见[安装部署手册](../installation.md#10-破坏性升级)。多输入关联还要声明维表变更影响的事实键/分区，无法确定时采用 full 回退或失败。

Python context 在普通合同基础上增加：

```json
{
  "processing": {
    "mode": "incremental",
    "from_watermark": {"ods_sequence": 12000},
    "to_watermark": {"ods_sequence": 12345},
    "state_release_id": "dwd_r12",
    "affected_partitions": []
  }
}
```

命名输入可以是 changes、key_state 或注册的聚合状态，仍然是固定 Parquet 与明确 schema。区间和状态由平台绑定，不接受脚本自行宣称“已经处理到哪里”。

## 4. merge_keys 的发布合同

产物必须包含 `_colvenna_op`（UPSERT / DELETE）及注册业务键，每个键最多一个最终操作。UPSERT 是完整目标行，按目标 schema 校验；DELETE 只要求键和声明的状态 metadata，非键业务列可以为空。wire schema 明确包含操作列及需要的版本 metadata；这些保留列不自动成为业务表列。

源版本清洗示例使用 `_colvenna_version`、`_colvenna_sequence` 更新键状态，包含删除 tombstone。任务定义和平台 adapter 校验 metadata 的类型、范围和与固定输入的关系；不能由脚本越权提升水位。对没有业务版本的变换，状态协议必须另行明确，不能直接套用该示例。

当前定义在 `output.change_metadata` 按顺序声明 `_colvenna_version`、`_colvenna_sequence` 两个非空 BIGINT。context.output.target_schema 保存业务 schema，context.output.schema 按 `_colvenna_op`、业务列、metadata 的顺序形成 wire schema；DELETE 非键业务列可空，UPSERT 仍须满足完整业务行和非空条件。版本/序号非负，实际来源还要由可信 adapter 核对。

`row_count=0` 在 merge_keys 中表示“没有键变化”，保留目标数据，仍可在有效区间核对后推进水位；在 versioned_replace 中则表示“目标全量变为空”。清单必须有正确的 wire schema，不能混用这两种语义。

allow_empty 判断更新后的实际目标/替换分区是否为空，不能据 delta 行数拒绝一个保留非空目标的 no-op。大量删除使目标变为空时，发布前仍检查空目标策略。

为了保持旧 release 可读，1.1.0 先从 expected_previous_release 构建独立未发布 generation，再分块应用 UPSERT/DELETE；键状态与 tombstone 也写入该 generation 的状态账本。完整核对后，用同一发布事务切换目标指针、状态指针和处理水位。发布资格、epoch、取消和分块幂等规则仍遵循[流水线合同](pipeline-design.md)。

这意味着增量计算不等于增量物理写入：复制上一 generation、导出固定输入和物化结果仍有成本，必须测量并计入调度预算。不能承诺每次仅写变更行。1.1.0 不使用 overlay / copy-on-write 等未验证的增量存储机制；采用它们需要单独设计读取、整理和回收语义。

## 5. 聚合如何选择

| 场景 | 建议 | 需要的证据 |
| --- | --- | --- |
| 简单字段清洗、源版本去重 | incremental + merge_keys | 键状态、版本顺序、删除 tombstone |
| 按天 SUM/COUNT，业务记录可更新/删除 | partition_recompute | before/after 日期、完整分区快照、空分区覆盖 |
| 仅追加流的 SUM/COUNT | 可用状态化 incremental | 去重、状态/水位同发布、溢出和精度检查 |
| 全局排名、复杂 distinct、多表历史修正 | full，或经单独验证的状态算法 | 完整输入版本向量、正确性对照和资源预算 |

任务各自维护输入水位向量和状态版本。一个全量聚合运行很久时，上游可继续接入；同一流水线默认合并 pending 区间，结束后跑下一轮。跨流水线并发还需目标冲突检查。没有新输入且代码/参数未变时可跳过；回填、首次运行、代码变更或强制重算不得当作“没有新数据”跳过。

水位只在对应任务发布成功时推进，失败重试使用原区间和状态。状态保留期必须覆盖可重放/回填窗口，过期后按 bootstrap 策略处理。多任务发布不是全局事务；下游 full 始终绑定本次已发布的完整输入 release。

## 6. 正确性对照

[本地示例](../../examples/orders/README.md)检查增量清洗的旧版本迟到、删除、删除后旧版本到达及新版本恢复。增量与分区重算以同水位的全量结果为正确性对照：多批增量/分区重算的结果必须与全量一致，并覆盖跨分区更新、删除后变空的分区、重试、状态丢失或不可复用、代码/规则变更及取消竞争。
