# Python 包依赖与运行环境

适用版本：2.0.0（tag `v2.0.0`）；本文使用当前标识与契约，验证范围见 [2.0.0 UAT 报告](../uat/v2.0.0-report.md)。下文沿用的 1.1.0 行为说明适用于 2.0.0，历史 AWS 结果不计入本版本。环境由固定源码的 CodeBuild 构建，通过环境 API 管理，绑定 Lambda 数字版本或 ECS 任务定义 revision；依赖升级生成新的环境版本，不合格的依赖在构建时拒绝。流水线按本文绑定环境运行，使用方式见[用户手册](../user-guide.md)。Lambda Managed Instances 与 Durable Functions profile 尚未实现。

## 1. 包依赖属于发布版本

用户在“流水线 → 运行环境”创建 Python 环境，或通过环境 API 提交 requirements.in。pandas、numpy、PyArrow、自有 wheel 等与脚本一起构成版本依赖；运行时不临时解析“最新版”或执行 pip install。

每个 environment_version 保存：

- Python 精确版本、CPU 架构、Linux/基础镜像 digest。
- requirements.in、解析后的全部传递依赖 requirements.lock 及文件 hash。
- 包来源、wheel hash、构建工具/配置版本。
- SQL/脚本适配器版本、ECR 镜像 digest。
- Lambda 函数数字版本或 ECS 模板 revision 的批准绑定。

同一依赖锁可以构建 Lambda 和 ECS 两份适配镜像；入口协议不同，Python 包版本、代码版本和输入输出合同保持一致。两份镜像都有独立 digest，不能把它们当成同一二进制。

## 2. 提交依赖

例如：

```text
pandas==2.2.3
numpy==2.2.6
pyarrow==24.0.0
```

也可在草稿中使用批准的版本范围；解析结果必须锁定到精确版本和 hash 后才能发布。平台检查依赖冲突、Python ABI、架构、许可/来源策略、镜像大小以及 executor/profile 的可用性。

当前批准 profile 只使用二进制 wheel，公共索引为 PyPI，私有包是服务器批准的 S3 VersionId/hash。不接受源码包、系统库构建或私有索引 URL；1.1.0 没有提供这些能力的 profile。私有对象权限只授予可信构建角色，隔离解析容器不接收 AWS 凭证，执行角色不能读取私有 wheel。

纯 Python 与 pandas/NumPy 这样的 native 包都必须针对目标 Python 和 Linux/CPU 架构构建。不能把本地 macOS virtualenv 直接上传 Lambda，也不能将 amd64 锁文件无条件用于 arm64。

## 3. 构建与激活

```text
DRAFT → RESOLVING → BUILDING → VERIFYING → READY
                                    ↘ FAILED
```

构建由受控 AWS CodeBuild 或同等隔离构建服务执行，生成依赖锁、安装清单与镜像，推送 ECR，运行 import smoke check 及任务合同检查。构建服务的镜像上传/部署权限不授予用户脚本。

同时含 DuckDB 与 PyArrow 的环境绑定 SQL profile，SQL 适配器在环境自带的 DuckDB 中执行。检查因此在镜像中实际执行一次受控 SQL 任务：从 Parquet 输入建表、封闭会话、校验结果 schema、流式读取结果并写出 Parquet，分别覆盖全量结果和以分号、注释结尾的分区替换，并确认外部访问被拒绝。适配器与所装 DuckDB 不兼容时（例如 DuckDB 1.0 没有 `query()` 表函数），构建在 VERIFYING 失败，不会产生只能在任务中失败的 READY 环境。

READY 只表示该环境构建校验已通过，不证明数据正确性、执行隔离或故障行为。READY 环境不可原地修改；依赖变更生成新 environment_version，重新构建并发布新的 pipeline_version。同一当前合同下，任务使用启动时绑定的环境；平台破坏性升级可以停止/失效旧任务并重建，不要求跨产品版本继续运行。

定义 API 使用 `runtime_environment: {environment_id, version}` 引用同空间的精确 READY 版本，分别检查环境是否就绪、executor/profile 是否匹配以及发布者的 `pipeline:manage` 权限。该对象取代示例中的合并名称字符串；当前合同不保留旧字符串解析。

API 中无法确认的构建观测显示 UNKNOWN，按原 build identity 核对，不据此重放构建或标记 READY。[机器合同](../../src/colvenna_isle/contracts/v2/contracts.schema.json)要求 READY 返回包锁 hash、image digest 及批准的数字函数版本/Task revision；字段校验不能证明资源真实可用。FAILED 构建使用新的环境版本，不覆盖已固定输入。

发布先确保所有不可变构建产物可读，再更新 DDB 中的环境 READY 记录。流水线发布原子绑定已 READY 的环境及具体执行配置；不在启动作业时才首次解析依赖。

当前运行/回填仍有效时，其引用的镜像和函数版本不能回收。破坏性升级先失效旧定义/运行，再清理旧环境；不提供跨产品版本回滚或旧环境迁移，见[安装部署手册](../installation.md#10-破坏性升级)。

## 4. Lambda 包装

默认 Python 数据任务使用容器镜像，将 pandas/PyArrow、平台 adapter 和批准的入口打包到同一固定环境。Lambda 镜像包含运行时接口客户端；只读 root filesystem 外的临时文件写到本次 execution 的 /tmp 目录。

默认镜像入口是平台 adapter；业务脚本来自 pipeline_version 的不可变 S3 代码包，下载后校验发布时绑定的 hash 再运行。因此，同一 READY 环境可以复用于多份脚本版本。若某个 profile 将业务代码直接烘焙进镜像，代码变更必须生成新 image digest 和函数数字版本，并重新绑定流水线版本。

普通 Lambda 当前配额为单次最多 900 秒、内存最多 10,240 MB，/tmp 为 512–10,240 MB。容器镜像最大未压缩大小为 10 GB。ZIP/Layers 可用于轻量环境：完整未压缩部署包含 Layers 上限为 250 MB，每个函数最多 5 个 Layers；Layers 必须绑定数字版本。

Lambda Managed Instances 异步入口的执行上限可到 5,400 秒，但其执行环境、成本与超时后继续运行的行为不同；必须使用明确、单独校验的 profile，1.1.0 尚未提供该 profile。不能因环境包相同就假定 standard / managed_instances / durable 的生命周期相同。

版本记录绑定 Lambda 已发布数字版本。环境构建/代码发布时生成或更新受控函数，记录部署 receipt；调用任务时不依赖可变的 `$LATEST` 或任意 alias。

## 5. ECS 包装

ECS 使用批准的 runtime image digest 和 Task Definition revision。平台按 resource_profile 配置 Fargate CPU、内存、工作盘和执行角色；RunTask 只传任务上下文引用与已校验参数。

平台提供 Python 脚本 adapter 和 SQL adapter。用户定义业务代码及依赖，不需要为每条 SQL 注册新的 AWS Task Definition；不同权限、环境或资源 profile 可绑定不同的批准模板。

ECS 和 Lambda 的执行角色按空间/流水线批准输入数据集和 staging 前缀，不授权写原生 warehouse。依赖解析、打包和部署属于控制面，不改变 API → SQS → 唯一写者的主接入链路。

## 6. 示例环境

示例 [requirements.in](../../examples/orders/requirements.in) 与 [requirements.lock](../../examples/orders/requirements.lock) 用于 Python 3.12、linux/arm64 本地合同演示。锁文件包含全部解析依赖及所下载 wheel 的 SHA-256；这是一个平台相关锁，不承诺适用于其他 ABI 或架构。

示例额外包含 DuckDB 和 PyYAML，以运行本地 SQL/DAG 演示。其版本是该本地示例的验证口径，不是对生产对象存储扩展版本或 AWS 镜像的批准。生产 profile 必须单独固定并验证 DuckDB/扩展及环境版本。

安装示例依赖：

```bash
python3 -m venv /tmp/colvenna-pipeline-demo-venv
/tmp/colvenna-pipeline-demo-venv/bin/python -m pip install --require-hashes -r examples/orders/requirements.lock
```

实际包管理 API 和 scopes 见 [API 契约](api-contract.md)，执行合同见 [Python 任务合同](python-task-contract.md)。

来源（查阅日期 2026-10-15）：[Lambda 配额](https://docs.aws.amazon.com/lambda/latest/dg/gettingstarted-limits.html)、[Lambda 容器镜像](https://docs.aws.amazon.com/lambda/latest/dg/images-create.html)、[Python Layers](https://docs.aws.amazon.com/lambda/latest/dg/python-layers.html)、[Managed Instances](https://docs.aws.amazon.com/lambda/latest/dg/lambda-managed-instances-execution-environment.html)。

DuckDB is a trademark of the DuckDB Foundation. Colvenna Isle is independent of the DuckDB Foundation and is not endorsed by it.
