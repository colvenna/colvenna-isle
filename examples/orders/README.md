# Orders：SQL + Python 流水线示例

这是本地文件合同演示，不是 Colvenna Isle 的服务实现。定义中的 Lambda、ECS 和 writer 选择是目标配置；本地检查使用临时 DuckDB 和 Python 子进程执行转换，不调用 AWS。

[pipeline.yaml](pipeline.yaml) 定义四个任务：

| 任务 | 执行配置 | 输入 → 输出 |
| --- | --- | --- |
| clean_orders | SQL / writer_short | ODS 事件 → 最新有效订单 |
| score_orders | Python / Lambda standard | 订单 → pandas 高价值/退款分类 |
| daily_sales | SQL / ECS Task | 订单 + 分类 → 每日营业额 |
| dashboard | SQL / writer_short | 每日营业额 → ADS 看板 |

四步 DAG 使用 `processing.mode: full` 完整重算小数据集，`versioned_replace` 表示全量替换。另外的[增量清洗 SQL](sql/clean_orders_incremental.sql) 读取 changes 与包含 tombstone 的 key_state，输出键级 UPSERT / DELETE。本地检查单独演示其多批状态逻辑。生产增量任务需明确 merge_keys 或完整的受影响分区镜像，不能把单批次聚合覆盖全表。writer_short 的资源预算和云端 profile 必须实际校验。

Python 从 context.json 的 orders 文件清单读取固定 Parquet，用 Decimal 保持金额精度，输出显式类型的 Parquet 与 output-manifest.json。运行环境和包版本随发布版本锁定；脚本不运行 pip install、不写原生 warehouse。

本地 runner 使用 [当前机器合同](../../src/colvenna_isle/contracts/v2/contracts.schema.json) 校验 pipeline、完整 input_manifest/context 和 output_manifest。输入描述含 release、schema、水位、固定对象/hash/行数/字节数；其中 S3 URI/version 是 local fixture 标识，不会访问 AWS。实际本地文件和输出 Parquet 仍独立核对。业务脚本仅依赖 pandas/PyArrow，平台合同校验在 runner/adapter 边界执行。

[score_orders 配置详解](score-orders-guide.md) 逐项说明环境、资源、输入快照、超时和输出更新，包含 Lambda /tmp 文件布局及 output-manifest.json 示例。

## 运行

锁文件适用于 Python 3.12、Linux arm64，来自真实下载的二进制 wheel，包含所有传递依赖和 SHA-256。其他架构需要重新解析并生成自己的锁文件，不能关闭 hash 检查来复用本锁。

示例锁引用 [合同依赖锁](../../runtime/contracts-requirements.lock)，一起安装 JSON Schema/YAML 校验依赖。两份锁的 hash 共同组成验证环境；包版本范围不会在运行时重新解析。

```bash
python3 -m venv /tmp/colvenna-pipeline-demo-venv
/tmp/colvenna-pipeline-demo-venv/bin/python -m pip install --require-hashes -r examples/orders/requirements.lock
/tmp/colvenna-pipeline-demo-venv/bin/python examples/orders/run_local.py
```

临时输入、context、输出和清单由脚本生成并在结束时删除。检查覆盖重复订单取最新版本、删除、退款、Python 分类、两天精确汇总、空结果类型及被篡改的输出 hash。预期 ADS：

| 日期 | revenue | order_count | high_value_count |
| --- | --- | --- | --- |
| 2026-10-14 | 610.00 | 3 | 1 |
| 2026-10-15 | 50.00 | 1 | 0 |

另检查四批增量清洗：新到达的旧业务版本不覆盖当前状态；删除保留 tombstone；删除后的旧版本不能复活记录；更高版本可恢复记录。最后将增量结果与同水位的全量 SQL 结果比较。本地键状态模型不实现云端 generation、事务或发布账本。

本地 PASS 只验证 SQL / pandas 转换与文件合同，不涉及 Lambda、ECS、S3/IAM、运行环境构建、调度、故障恢复或写者发布；这些在已安装环境中由 [quickstart](../quickstart/README.md) 覆盖。

协议和设计：[内置流水线](../../docs/design/pipeline-design.md)、[增量与全量](../../docs/design/processing-modes.md)、[Python 输入输出](../../docs/design/python-task-contract.md)、[依赖环境](../../docs/design/runtime-environments.md)。

DuckDB is a trademark of the DuckDB Foundation. Colvenna Isle is independent of the DuckDB Foundation and is not endorsed by it.
