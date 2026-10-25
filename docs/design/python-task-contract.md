# Python 任务的输入、输出与执行合同

合同版本：2。适用版本：2.0.0 候选；本文使用当前标识与契约，AWS 验收仅属于历史 1.1.0。Python 适配器负责固定描述、独立执行和 staging，唯一写者负责原生导入与发布；完整流水线中的 pandas 任务按本合同运行。[pandas 示例](../../examples/orders/python/score_orders.py) 可在本地运行。

固定输入由写者在一个原生读事务内导出：输入按明确水位或 release 绑定为 Parquet 文件、schema、对象版本/hash 和完整清单。持续接入期间，已绑定的输入保持不变，新的运行才看到新水位；写者替换后，原清单仍可读取。执行角色的读写范围见第 6 节。

[机器 JSON Schema](../../src/colvenna_isle/contracts/v2/contracts.schema.json) 与[语义校验器](../../src/colvenna_isle/contracts/validation.py)是当前合同的可执行形状；本地示例使用同一 pipeline/input_manifest/context/output_manifest 校验，但不调用 AWS。

## 1. Python 到底读什么、写什么

**输入是固定版本的数据集 Parquet 文件；输出是目标数据集的 Parquet 文件及 output-manifest.json。** Python 不打开原生 DuckDB/SlateDB 对象数据库进行写入。原生四层表仍在 DuckDB 中，输入 Parquet 是产品为任务导出的固定快照，输出 Parquet 是待校验的计算产物。

一个任务声明一个目标数据集，可以声明多个命名输入。任务通过 `context.json` 获取输入文件清单、列类型、输入版本、参数及指定输出位置。脚本可使用 pandas、PyArrow、DuckDB Python 或其他已锁定的包；数据交换格式不随执行器改变。

## 2. 输入准备

1. 调度器绑定 pipeline_version、代码包 hash、environment_version、镜像 digest、参数和输入版本向量。
2. 上游为原生表时，唯一写者在稳定读取事务中导出指定水位的快照，发布完整 manifest 后才放行任务。不能让 Python 扫描持续变化的 warehouse 前缀。
3. manifest 列明精确 S3 key、object version、SHA-256、行数、schema 和水位；不使用 wildcard 或“读取最新”替代固定输入。
4. 执行适配器准备独立工作目录，核对代码/输入 hash，生成 context。并发 invocation 的目录包含 execution_id，不能共用固定的 `/tmp/context.json` 或输出文件名。

输入访问模式：

| `input_mode` | `files` 内容及读取方式 |
| --- | --- |
| `local_files` | 适配器下载并验证文件，提供绝对 `local_path`；Python 用 pandas/PyArrow 读取本地 Parquet |
| `s3_files` | 提供固定 `s3_uri` 和 object version/hash；脚本或批准的读取库按清单使用受限执行角色流式读取，不能扫描目录选择新版本 |

local_files 的输入、输出、包和 spill 必须落在 profile 的临时存储预算内。超出预算在派发前拒绝，改为受支持的 s3_files、增加资源或按分区拆任务。s3_files 的对象不可变性/版本固定须由适配器和保留策略保证；读取库不能无声地忽略绑定的 object version。

示例脚本使用 local_files。它按 Parquet batch 读取并使用 pandas 处理，不需要把全部输入一次性拼成大 DataFrame。s3_files 脚本必须显式支持该模式；选择 executor 不自动改写用户读取代码。

## 3. context.json

下面展示合同形状。`local_path`、directory 和 identifiers 由执行适配器生成；hash 是已绑定 manifest 的真实 hash，示意字符串不能作为生产值。

```json
{
  "contract_version": 2,
  "space_id": "space_123",
  "pipeline_id": "orders",
  "pipeline_version": "v3",
  "run_id": "run_123",
  "task_id": "score_orders",
  "job_id": "job_123",
  "attempt_id": "attempt_1",
  "execution_id": "exec_1",
  "input_mode": "local_files",
  "parameters": {"high_value_threshold": "500.00"},
  "inputs": {
    "orders": {
      "dataset": "dwd.orders",
      "release_id": "dwd_r12",
      "manifest_sha256": "<64-hex-sha256>",
      "watermark": {"ods_sequence": 12345},
      "selection": {"mode": "snapshot", "partitions": []},
      "schema": [
        {"name": "order_id", "type": "BIGINT", "nullable": false},
        {"name": "order_date", "type": "DATE", "nullable": false},
        {"name": "amount", "type": "DECIMAL(18,2)", "nullable": false}
      ],
      "files": [
        {
          "s3_uri": "s3://example-bucket/spaces/space_123/inputs/dwd_r12/part-00000.parquet",
          "object_version": "<s3-version-id>",
          "sha256": "<64-hex-sha256>",
          "rows": 1000,
          "bytes": 65536,
          "local_path": "/workspace/job_123/attempt_1/exec_1/input/orders/part-00000.parquet"
        }
      ]
    }
  },
  "output": {
    "name": "result",
    "dataset": "dwd.order_scores",
    "directory": "/workspace/job_123/attempt_1/exec_1/output/result",
    "write_mode": "versioned_replace",
    "expected_previous_release": "scores_r4",
    "key_columns": ["order_id"],
    "allow_empty": true,
    "replace_partitions": [],
    "schema": [
      {"name": "order_id", "type": "BIGINT", "nullable": false},
      {"name": "high_value", "type": "BOOLEAN", "nullable": false},
      {"name": "value_band", "type": "VARCHAR", "nullable": false}
    ]
  }
}
```

Lambda 的 directory 位于本次 invocation 独立的 `/tmp/colvenna/<run>/<attempt>/<execution>/…`；ECS 的目录位于其独立 Task 的工作盘。绝对路径会变化，脚本必须读取 context，不能硬编码示例目录。输入路径只用于读取，输出只写声明的目录。

调用形式：

```bash
python score_orders.py --context /path/to/context.json --output-dir /path/to/output/result
```

SQL 任务使用相同的输入版本和输出 schema。SQL adapter 将输入别名绑定为只读关系（示例为 `events`、`orders`、`scores`），执行一个产出关系的 SELECT/WITH 查询，再生成 Parquet 和同款清单；用户不通过该 SQL 向原生 warehouse 发起任意 DML。

## 4. 数据类型与参数

| 合同类型 | Parquet / Python 映射 |
| --- | --- |
| BIGINT | signed int64；业务键禁止浮点转换 |
| BOOLEAN | bool |
| VARCHAR | UTF-8 string；pandas 使用明确 string dtype |
| DATE | date32 / Python date |
| TIMESTAMP | 微秒时间戳，平台按 UTC 解释；有时区值使用明确的 TIMESTAMPTZ/UTC 映射 |
| DECIMAL(p,s) | Parquet Decimal，Python decimal.Decimal，不能经 float 中转金额 |
| JSON | UTF-8 JSON 文本，解析后值必须有限；不能用 pickle 交换 |
| BLOB | Parquet binary；JSON API 用规范 base64 表达 |
| VARCHAR[] | Parquet list<string> |

目标类型按注册 schema 校验，不依赖 pandas 的隐式推断。输出 Parquet 不写 pandas index。`nullable: false` 按实际数据检查；Parquet 的物理 optional 标记不替代逐列空值校验。

parameters 是定义中声明的 JSON 类型值。Decimal 使用规范字符串，如 `"500.00"`；日期使用 ISO 字符串。参数校验发生在准入时。API Key、AWS secret、其他空间路径、任意目标表名或执行角色不属于可传参数。

## 5. 输出文件与清单

脚本关闭所有 Parquet 文件后，最后原子写入 output-manifest.json：

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

path 是输出目录下的相对文件路径。绝对路径、`..`、指向外部文件的 symlink、重复文件和未声明目标均拒绝。stdout/stderr 是日志，不是数据输出；进程退出码 0 也不能代替清单。

空结果仍写带正确 schema 的空 Parquet，或按合同提供 files=[] / row_count=0。替换为实际空目标/分区需要 allow_empty=true；merge_keys 零条变更保留目标，不当作清空。replace_partitions 必须覆盖 context 中全部待替换分区，包括空分区；文件分区值必须与列值及授权范围一致。

replace_partitions 输出清单额外携带 `replace_partitions` 数组，值与 context.output.replace_partitions 完全覆盖同一集合。即使某个分区没有文件/没有行，仍列出该分区；不能只通过 files[].partition_values 推断替换范围。merge_keys 的 context 同时保存 target_schema（业务约束）和 schema（允许 DELETE 空非键字段的 wire 形状）。

增量任务的 context 增加固定水位区间和状态 release。merge_keys 使用含 `_colvenna_op`、业务键及声明 metadata 的 wire schema；UPSERT 校验完整业务行，DELETE 校验键/状态。零行表示无键变化，不表示清空目标。详细输入和更新语义见[增量、分区重算与全量](processing-modes.md)。

默认一份任务产物只对应一个数据集。大结果按已批准的行数/字节预算写多个 part 文件；重算可按分区拆成多个受控任务。所有输出均受工作盘、文件数和总输出字节配额限制，不能假设 Lambda 的 /tmp 或 Task spill 无限大。

## 6. 校验、上传和入库

输出清单由用户代码生成，不能作为可信证明。适配器独立打开 Parquet，重新核对：

- 合同、job/attempt/execution identity 和当前有效候选。
- 文件位置、内容 hash、字节数、实际行数及全体 row_count。
- 列名、类型、Decimal 精度、空值、业务键唯一性和分区覆盖。
- 输出 schema、目标数据集及写入方式与已发布定义一致。

校验通过后，适配器上传当前 execution 的文件，再上传完成清单，记录 object version 和 hash。输出先为 STAGED；校验失败为 FAILED，不推进水位。

执行角色只读批准的数据集快照/代码，可写其流水线的 staging 前缀，不拥有原生 warehouse 写权限。作业隔离还依赖不可变对象版本、候选身份和写者校验；共享 pipeline staging 权限不能被描述成每个 attempt 独立的 IAM 沙箱。自定义代码由获得 pipeline:manage 权限的受信空间成员发布。

唯一写者按可信清单导入未发布 generation；逐块消重后，核对输入水位、`ctl` 中的有效 attempt/execution/eligibility_epoch 和 expected_previous_release，再发布。取消或替换候选通过写者命令更新这份权威资格，不能仅修改 DDB 投影。Python 可在自己的临时目录使用本地 DuckDB，但不能从任务获得产品原生数据库的写者身份。

## 7. Lambda 与 ECS 的适配

Lambda Invoke 的事件只传引用，不传 DataFrame：

```json
{
  "operation": "task_execute",
  "descriptor": {
    "s3_uri": "s3://example-bucket/spaces/space_123/task-contexts/gen_123/run_123/job_123/attempt_1/descriptor.json",
    "object_version": "<s3-version-id>",
    "sha256": "<64-hex-sha256>",
    "bytes": 2048
  }
}
```

`descriptor` 指向控制面保存的可移植任务描述，包含固定 run/job/attempt、定义/输入 manifest 和参数，不含尚未生成的本地路径。adapter 校验确切对象版本、该描述及环境身份后创建新 execution 的独立工作目录，下载固定输入，再生成第 3 节所示带 local_path/directory 的 context.json 给脚本。ECS 使用 `--task-event '<同一 JSON 事件>'` 的批准命令入口。

Lambda adapter 执行脚本、校验产物并上传，最后报告 manifest_uri/object_version/hash。ECS adapter 执行相同步骤，工作盘、CPU、内存和执行期限由 Task profile 提供。

ECS container exit、Lambda 返回和 completion notification 都要经过产物核对。重复 Invoke 可以产生多个 execution；只发布控制面已选定且未失效的候选。Lambda Managed Instances 可能并发使用同一环境并在超时后继续工作，必须隔离工作目录，并禁止旧候选发布。

输入、代码和依赖均固定。重试新建 attempt / execution 目录；禁止覆盖前一次可审计产物。超时、取消或部分上传不会把半成品交给下游。

## 8. 示例与验证

[score_orders.py](../../examples/orders/python/score_orders.py) 使用 PyArrow 逐批读取 Parquet、pandas 分类、Decimal 保留金额精度，并写出显式 schema 的 Parquet 和清单。

[本地演示](../../examples/orders/README.md) 验证重复事件取最新版本、删除、退款、Python 分类、SQL 聚合、空输入及产物校验，不调用 AWS。云端执行时，适配器在 Lambda / ECS 中读取固定版本的 S3 对象，在资源预算内以子进程运行脚本，并上传 staging 产物；原生导入与发布由唯一写者按第 6 节完成。

DuckDB is a trademark of the DuckDB Foundation. Colvenna Isle is independent of the DuckDB Foundation and is not endorsed by it.
