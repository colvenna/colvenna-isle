# score_orders 配置与输出清单详解

适用版本：2.0.0 候选，合同 revision 为 2；历史 1.1.0 的 AWS 验收不代表候选通过，候选验证范围见 [PR #23](https://github.com/colvenna/colvenna-isle/pull/23)。本文说明 `score_orders` 的配置与执行协议；云端执行（Lambda / ECS 运行环境、资源 profile 与数据库发布）见[用户手册](../../docs/user-guide.md#8-流水线)。

该任务等待 clean_orders 发布结果，在 Lambda 中运行 Python，对完整订单快照分类，再由唯一写者发布 dwd.order_scores。配置源见 [pipeline.yaml](pipeline.yaml)，代码见 [score_orders.py](python/score_orders.py)。

## 配置字段

| 配置 | 含义 |
| --- | --- |
| task_id: score_orders | 任务标识，用于依赖、运行和日志关联 |
| language: python | 执行 Python 脚本 |
| processing.mode: full | 对绑定输入版本中的全部订单重新计算 |
| executor: lambda | 使用批准的 Lambda 函数数字版本执行 |
| entrypoint: python/score_orders.py | 已发布代码包中的相对脚本路径 |
| runtime_environment: {environment_id: pandas-demo, version: v1} | 固定 Python、pandas、PyArrow 等依赖及镜像的环境版本 |
| resource_profile: python_lambda_standard | 批准的 Lambda 内存、临时磁盘、权限和函数配置 |
| input_mode: local_files | adapter 下载并校验输入，将本地路径传给脚本 |
| timeout_seconds: 300 | 单次计算执行预算为 5 分钟，包含输入准备、脚本和产物处理；排队与写者导入另行计时 |
| max_attempts: 3 | 最多三次任务尝试，包括首次执行；平台记录的尝试上限不保证 Lambda 恰好只调用三次 |

runtime_environment 管软件和版本，resource_profile 管运行资源和权限。这两个名称是示例引用，需要先构建 READY 环境并注册批准的 profile，不能凭名字推断实际内存或函数 ARN。默认业务脚本来自固定的 S3 代码包，镜像中的平台 adapter 核对代码 hash 后执行。

## 输入与输出

depends_on: [clean_orders] 控制调度顺序；inputs.orders 定义名为 orders 的输入，绑定同一次运行中 clean_orders 已发布的结果。脚本从 context.json 的 inputs.orders.files 获取文件路径。

selection: snapshot 表示完整、固定的输入快照；processing.mode: full 表示使用它重新计算全部订单。输入在任务运行期间不会变成最新版本。脚本逐批使用 pandas 处理，全量计算不要求把全部数据一次性装进一个 DataFrame。

| 输出配置 | 含义 |
| --- | --- |
| dataset: dwd.order_scores | 注册的目标数据集 |
| write_mode: versioned_replace | 输出代表完整数据集，构建新 generation 后整体发布 |
| key_columns: [order_id] | 全部输出中的 order_id 必须唯一，重复键校验失败 |
| allow_empty: true | 允许发布空结果；全量替换时目标会变为空数据集 |
| schema | order_id 为 BIGINT、high_value 为 BOOLEAN、value_band 为 VARCHAR，三列均非空 |

脚本读取流水线参数 high_value_threshold，默认是字符串 "500.00"，使用 Decimal 比较金额：

| amount | high_value | value_band |
| --- | --- | --- |
| 600.00 | true | HIGH |
| 50.00 | false | REGULAR |
| -2.34 | false | REFUND |

该逻辑可以设计成增量分类，但需要变更输入、删除语义和状态协议。仅修改 mode 字段不能让当前读取完整快照、输出完整替换结果的脚本自动变成增量任务。

## Lambda /tmp 中放什么

local_files 模式下，adapter 为每次实际 execution 创建独立目录。例如：

```text
/tmp/colvenna/run_123/attempt_1/exec_1/
├── context.json
├── input/
│   └── orders/
│       └── part-00000.parquet
└── output/
    └── result/
        ├── part-00000.parquet
        └── output-manifest.json
```

context.json 提供实际 input 文件路径和 output.directory。业务脚本读取输入，写结果 Parquet，关闭文件后计算真实大小/hash，最后将临时清单原子 rename 成 output-manifest.json。实际目录由 adapter 生成，脚本不能硬编码 run、attempt 或 execution。

/tmp 是临时工作盘；执行环境的重用不构成持久性保证。输入、输出和 spill 一起受 profile 的临时磁盘预算限制。逐批处理降低内存需求，但不能消除本地文件占用；容量或执行预算超限应调整资源、分区拆分或选择 ECS，并重新校验定义。

## output-manifest.json 长什么样

它是结果文件的清单。下面展示与当前脚本一致的形状；行数、大小和 hash 为示意值，真实运行必须按实际文件计算：

```json
{
  "contract_version": 2,
  "job_id": "job_123",
  "attempt_id": "attempt_1",
  "execution_id": "exec_1",
  "output_name": "result",
  "format": "parquet",
  "schema": [
    {"name": "order_id", "type": "BIGINT", "nullable": false},
    {"name": "high_value", "type": "BOOLEAN", "nullable": false},
    {"name": "value_band", "type": "VARCHAR", "nullable": false}
  ],
  "row_count": 1000,
  "files": [
    {
      "path": "part-00000.parquet",
      "rows": 1000,
      "bytes": 24576,
      "sha256": "<actual-file-sha256>",
      "partition_values": {}
    }
  ],
  "metrics": {"scored_orders": 1000}
}
```

| 字段 | 用途 |
| --- | --- |
| contract_version | 数据交换协议版本 |
| job_id / attempt_id / execution_id | 关联任务、尝试和实际执行，防止旧执行结果冒用当前身份 |
| output_name | 与 context.output.name 匹配，本例只有 result |
| format / schema | 声明输出格式、列名、类型和非空要求，adapter 要独立验证 |
| row_count | 所有结果文件的总行数，必须等于各文件实际行数之和 |
| files[].path | 相对于 output.directory 的文件路径，不是 S3 URI |
| files[].rows / bytes / sha256 | 每个 Parquet 文件的行数、字节数和完整文件 SHA-256 |
| files[].partition_values | 声明文件所属分区；本例无分区，因此为 {} |
| metrics | 业务观测指标，本例记录分类订单数；不代替数据校验 |

当前脚本只写一个 part 文件。合同支持多个文件，必须分别列明并核对；空输入时该脚本仍写带正确 schema 的空 Parquet 和零行清单。

目标 dataset、写入模式、输入 release 和发布资格由已发布定义、context 和权威任务记录绑定。用户代码生成的清单不能自行更改目标或批准发布。

## 从本地文件到已发布数据

1. adapter 独立核对实际 Parquet 的 schema、非空列、业务键、行数、大小和 hash。
2. 将结果文件上传到本次 execution 的不可变 S3 staging 位置，再上传完成清单，记录精确对象版本及 hash。
3. 平台生成可信提交清单/引用，将选定且有效的产物交给唯一写者。
4. 写者分块导入未发布 generation，核对有效 attempt/execution、eligibility_epoch、输入版本和 expected_previous_release。
5. 发布事务成功后任务成为 PUBLISHED，才放行 daily_sales。

原始脚本清单里的相对 path 可以保留；上传后的提交记录负责映射实际 S3 对象 URI/version/hash。没有完成清单的部分文件不能作为完整结果。Lambda 返回成功表示计算阶段完成，S3 staging 表示产物已暂存，二者都不表示目标数据集已经发布。

协议：[Python 输入输出](../../docs/design/python-task-contract.md)、[运行环境](../../docs/design/runtime-environments.md)、[任务级处理模式](../../docs/design/processing-modes.md)。本地运行方法和验证范围见 [示例 README](README.md)。
