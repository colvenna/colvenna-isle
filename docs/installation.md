# Colvenna Isle 安装部署手册

本手册适用于当前未发行的 `2.0.0.dev0` 候选，面向在自己的 AWS 账号和商业区域（partition `aws`）中安装和运维 Colvenna Isle 的运维人员。改名后的 AWS 安装验收为 `NOT_RUN`；1.1.0 的历史验收不代表当前候选通过。涵盖以下内容：

- 准备部署主机与原生存储镜像；
- 构建 release；
- 全新安装、开通空间；
- 开放 / 关闭、破坏性升级、失败续装；
- 备份与隔离还原；
- 卸载。

日常使用（API Key、接入、同步写、查询、流水线、Query Editor 与 Admin Console）见[用户手册](user-guide.md)。

## 快速安装（install.sh）

部署主机满足第 3 节的前置条件，账号中也已有原生存储镜像（第 4 节）时，仓库根目录的 `install.sh` 可以用一条命令完成第 3.4 节到第 8 节的工作：

```bash
git clone https://github.com/colvenna/colvenna-isle.git
cd colvenna-isle
git checkout --detach <已审查的候选提交>
./install.sh fresh --environment prod --account <12 位账号 ID> --region <目标区域> \
  --space sales_prod --token-output /secure/path/sales_prod-admin.token
```

`install.sh` 依次执行：

1. 预检：
   - ARM64、Python 3.12、Docker、Node.js、AWS CLI；
   - 干净的检出；
   - 当前凭证是该账号的实例角色。
2. 在 `--work-dir`（默认 `../colvenna-isle-work`）中按锁定依赖建立构建环境。
3. 检查原生存储镜像；本机没有时，从本账号 ECR 按锁定的 digest 拉取。
4. 构建 release。同一提交已有通过的 release 时直接复用。
5. 运行 `install.py fresh` 或 `upgrade`，回执写入 `<work-dir>/receipts/`。
6. 指定 `--space` 时，开通空间，并把首个管理员 Key 写入 `--token-output` 指定的新文件。
7. 输出 `ApiUrl` 与 `WebUrl`。

其他用法：

- 升级：`./install.sh upgrade --environment prod --account <账号 ID> --region <目标区域>`。升级是破坏性的，执行前先读第 10 节。
- 某一步失败时，先按回执核对该步的结果，再加 `--steps <从失败步骤开始的步骤>` 续装（第 6.1 节）。
- `--check` 只做预检；`--` 之后的参数原样传给 `install.py`。
- 备份、还原和卸载不由 `install.sh` 执行，见第 11、12 节。

下面各节说明 `install.sh` 内部的每一步，以及安装之后的运维操作。

## 1. 支持范围

| 维度 | 当前候选的配置范围（1.1.0 验收为历史） |
| --- | --- |
| AWS 账号 | 任意单个账号。账号 ID 不写在源码中：服务由栈注入 `COLVENNA_ACCOUNT`，运维工具以 `COLVENNA_ACCOUNT` 为准，并与当前凭证核对 |
| 区域 | 按显式配置（`--region` / `COLVENNA_REGION` / `AWS_REGION`）部署到标准商业分区 `aws`。`us-east-2` 已完成全部功能验收，`us-west-2` 已完成回归；其他商业区域未单独验收。验收源码与范围见[发行说明](release-notes.md#验收)。tag `v1.0.0` 只支持 `us-west-2` |
| CPU 架构 | ARM64（Graviton）：写者、查询、计算 Task、Lambda、运行环境构建都只构建 ARM64 |
| 规格 | 写者与查询服务各为 ECS Fargate 2 vCPU / 4 GiB，见[发行说明](release-notes.md#规格) |

安装、使用、备份还原、升级和卸载已在一个 AWS 账号中实际验收。其他账号只是代码层面支持（账号由部署环境决定），没有单独验收。

区域实现于 2026-10-21 在同一账号的 `us-east-2` 与 `us-west-2` 中验收。东区完成全新安装、56 项功能 UAT、同区域备份与隔离还原、不同源码升级、中断部署后的同一制品重部署及卸载；西区完成全新安装、功能回归、同名环境的区域隔离检查与卸载。区域前置条件见[§3.3](#33-区域前置条件)；中国区、GovCloud 与隔离分区不在范围内。

## 2. 部署组成

每套环境由一组 CloudFormation 栈组成，用环境名 `COLVENNA_ENVIRONMENT` 和区域 `COLVENNA_REGION` 选择：

| 栈 | 内容 | 名称（环境 `<name>`） |
| --- | --- | --- |
| 基线 | VPC 与网络、API Gateway 与 authorizer、接入 Lambda 与 SQS FIFO、写者与查询 ECS 服务、同步写代理、控制表、数据桶、CloudFront（Query Editor / Admin Console） | `colvenna-isle-<name>-core` |
| 控制面 | 管理接口、流水线运行控制与调度、回收 | `colvenna-isle-<name>-control` |
| 运行环境构建器 | CodeBuild 项目、运行时角色边界、流水线事件队列 | `colvenna-isle-<name>-builder` |

另有两类账号级资源，独立于所有环境：

- **备份库**：每个目标区域各自的 `colvenna-isle-backups` 栈中的版本化 S3 桶，删除环境时保留。
- **镜像仓库**：ECR `colvenna-isle-images`，存放原生存储镜像、写者与查询镜像，以及运行环境镜像。

环境名为 2–12 位小写字母或数字，以字母开头；`env`、`core`、`control`、`builder` 保留不可用。所有运维工具都要求显式设置 `COLVENNA_ENVIRONMENT`，未设置时拒绝执行。每个栈都带有标签 `Project=colvenna-isle` 与 `Component=core|control|builder`。

同一账号可以在不同区域安装多套环境，它们之间相互隔离：

- 每套环境的运行时资源使用各自的前缀 `colvenna-isle-<namespace>-`（namespace 是环境名与区域的 SHA-256 前 16 位）；
- IAM 通配符不会匹配其他环境或区域的函数、任务定义或角色。IAM 角色属于整个账号，因此同名环境在不同区域也使用不同 namespace；运行时函数名仍不超过 64 字符。
- CloudFront 的 OAC、响应头策略、缓存策略和源请求策略属于账号全局资源；名称包含栈名与区域，避免同名环境跨区安装时冲突。

## 3. 前置条件

### 3.1 部署主机

运维工具只接受 **EC2 实例角色** 凭证，不接受静态 Access Key，也不接受 profile。因此部署和运维都在一台 EC2 实例上进行：

- ARM64（Graviton）实例，Ubuntu 24.04，根盘建议 100 GiB 以上（镜像构建与 release 目录）；
- 实例角色有权在目标账号中创建与删除本手册涉及的资源，包括 CloudFormation、IAM 角色与策略、VPC 与安全组、S3、DynamoDB、SQS、Lambda、ECS、ECR、API Gateway、CloudFront、CodeBuild、CloudWatch Logs、GuardDuty 端点。最简单的做法是使用管理员权限的专用实例角色；
- Python 3.12、Docker（支持 `linux/arm64`，使用默认 docker 构建驱动）、Node.js、git、AWS CLI v2。

### 3.2 环境变量

所有运维命令都在下面的环境中执行。下文用 `$COLVENNA` 代表这一前缀：

```bash
export COLVENNA_ACCOUNT=<12 位账号 ID>          # 要操作的账号，工具会与当前凭证核对
export COLVENNA_REGION=<目标商业区域>
export AWS_REGION="$COLVENNA_REGION" AWS_DEFAULT_REGION="$COLVENNA_REGION"
export COLVENNA="/usr/bin/env -u AWS_PROFILE -u AWS_DEFAULT_PROFILE COLVENNA_ENVIRONMENT=<name>"
```

- `-u AWS_PROFILE`：确保使用实例角色凭证链。
- `COLVENNA_ACCOUNT` 与当前凭证的账号不一致时，工具拒绝执行。
- `COLVENNA_REGION`、`AWS_REGION`、`AWS_DEFAULT_REGION` 中至少一个必须显式设置；设置多个时必须一致，否则在 AWS 调用前拒绝。`install.sh --region` 也必须与已设置的值一致。AWS 凭证属于账号，不绑定某个区域；工具核对凭证账号、SDK 目标区域与被操作栈 ARN 的区域，不推断凭证有独立的区域属性。

### 3.3 区域前置条件

- 目标区域属于标准 AWS 商业 partition `aws`；中国区、GovCloud 和隔离 partition 不在范围内。
- 目标区域提供 ARM64 Fargate、ARM64 Lambda 和 ARM_CONTAINER CodeBuild，并有至少两个可用区。账号已开通该区域，网络、服务配额和 CloudFront 权限满足本手册的安装步骤。
- 原生存储镜像在目标区域的本账号 ECR 中；按第 4 节构建，或从已经验证的区域复制 OCI manifest 和所有 layers。复制后核对锁定的 digest 不变，再用目标区域的完整 ECR URI 拉取到构建主机。
- 备份库按区域部署；同版本的备份和隔离还原在同一区域的另一套环境中进行。本功能不提供跨区域复制、灾备或主动-主动。
- 成本观测只使用目标区域的价目表；没有该区域价目表时返回 `UNKNOWN` / `REGIONAL_PRICES_UNAVAILABLE`，不会套用其他区域的价格或返回零成本。

### 3.4 构建环境

```bash
python3 -m venv /tmp/colvenna-build
/tmp/colvenna-build/bin/python -m pip install --require-hashes --only-binary=:all: \
  -r runtime/contracts-requirements.lock -r runtime/aws-requirements.lock -r runtime/build-requirements.lock \
  -r examples/orders/requirements.lock
```

所有依赖按 hash 锁定，只接受 wheel。orders 示例 lock 提供 pandas，control 构建中的流水线套件会启动真实的 pandas 子进程。

## 4. 原生存储镜像（每个账号与区域一次）

写者与查询镜像都叠加在一个经过验证的原生存储镜像上，镜像内含 DuckDB 1.5.6 与 duckdb-object-storage 扩展。[writer-lock.json](../runtime/writer-lock.json) 锁定了它的仓库名和 digest：

```json
"storage_repository": "colvenna-isle-images",
"storage_digest": "sha256:8ec8ab61…"
```

构建 release 时，这个镜像必须已经发布在本账号的 ECR 中，并且已经拉取到构建主机：

```bash
aws ecr get-login-password --region "$COLVENNA_REGION" | \
  docker login --username AWS --password-stdin $COLVENNA_ACCOUNT.dkr.ecr.$COLVENNA_REGION.amazonaws.com
docker pull $COLVENNA_ACCOUNT.dkr.ecr.$COLVENNA_REGION.amazonaws.com/colvenna-isle-images@sha256:8ec8ab61…
```

账号中还没有这个镜像时，先从固定的上游源码构建并发布：

1. 准备工具链：ARM64 Ubuntu 24.04，GCC 13.3.0、Rust/Cargo 1.96.1、CMake 3.31.6、Ninja 1.13.0。版本固定在 [storage-lock.json](../runtime/storage-lock.json) 中。
2. 构建原生扩展与运行镜像：

   ```bash
   python3 scripts/storage/prepare_runtime.py --workspace /path/outside/repo/storage-build
   ```

   工具校验每个下载的 hash、源码提交和 Cargo.lock，输出构建回执与本地镜像。

3. 生成带存储镜像身份的开发 bundle：

   ```bash
   /tmp/colvenna-build/bin/python scripts/build_release.py --output /path/outside/repo/storage-bundle \
     --storage-receipt /path/outside/repo/storage-build/build-receipt.json --storage-image <本地镜像 tag>
   ```

   这一步在无网络、只读、非 root 的容器中，加载并核对三个扩展。

4. 发布到本账号 ECR（不可变 tag）：

   ```bash
   $COLVENNA /tmp/colvenna-build/bin/python scripts/aws/manage.py publish --bundle /path/outside/repo/storage-bundle \
     --output /path/outside/repo/receipts/storage-publish
   ```

原生构建不保证逐字节可复现。如果重新构建得到的镜像 digest 与 `writer-lock.json` 不同：

1. 把新 digest 写入 `writer-lock.json`；
2. 从这个提交重新构建 release。

这条路径没有经过验收。

## 5. 构建 release

从所选提交的干净检出构建（构建回执绑定精确 commit / tree），构建目录放在仓库之外：

```bash
git clone https://github.com/colvenna/colvenna-isle.git && cd colvenna-isle
git checkout --detach <已审查的候选提交>
COLVENNA_ACCOUNT=$COLVENNA_ACCOUNT /tmp/colvenna-build/bin/python scripts/build_product.py --output /path/outside/repo/release-candidate
```

`build_product.py` 只接受干净的工作区，依次构建并测试：

| 组件 | 内容 | 构建时运行的检查 |
| --- | --- | --- |
| control | control / intake / writer / query 共用的 Lambda ZIP | 合同生成核对，以及 control、intake、writer、query 与流水线套件 |
| writer、query | 叠加在原生存储镜像上的 ECS 镜像 | 镜像内的 DuckDB 1.5.6 原生套件 |
| builder-source | 运行环境构建器源码包 | 源码清单与 hash |
| editor | Query Editor 与 Admin Console 静态文件 | 脚本语法检查 |

release 目录中的 `manifest.json` 记录以下内容，任何一项失败都不会写出 `PASS`：

- 产品版本、源码 commit / tree；
- 每个组件 manifest 的 SHA-256；
- 镜像 ID；
- 存储镜像的仓库名与 digest。

`COLVENNA_ACCOUNT` 只用于在本机找到存储镜像。套件本身使用固定的测试账号 `123456789012`，不访问 AWS。

1.1.0 的写者和查询镜像都安装 hash 锁定的 pyarrow 24.0.0；查询镜像用它批量装载写者提供的快照，构建时核对版本和原生类型测试，见[发行说明](release-notes.md#110-的变更)。

## 6. 全新安装

安装工具要求当前工作区是 release 对应的同一个干净提交：

```bash
$COLVENNA /tmp/colvenna-build/bin/python scripts/aws/install.py fresh \
  --release /path/outside/repo/release-candidate --output /path/outside/repo/receipts/install-<name>
```

`fresh` 依次执行以下步骤。每一步调用仓库自己的部署工具，回执写入 `--output` 下的独立子目录：

| 步骤 | 作用 |
| --- | --- |
| vault | 部署或核对账号级备份库 |
| baseline | 创建基线栈，此时写者与查询服务都停止 |
| environments | 部署运行环境构建器 |
| control | 部署控制面 |
| auth | 先确认 authorizer、control 和私有 probe 都运行 release 的 ZIP，并且在关闭状态下拒绝缺失或无效的凭证，再启用认证。启用后实测公开 API：缺失凭证返回 401，无效凭证返回 403 |
| writer | 发布写者镜像并批准写者（要求认证已启用） |
| control-registry | 以 registry-only 方式更新控制面，绑定写者源码 |
| intake | 部署接入 Lambda |
| query | 发布查询镜像 |
| sync | 部署同步写代理 |
| editor | 发布 Query Editor 与 Admin Console，刷新 CloudFront |
| open | 要求全部组件运行同一源码、认证已启用；随后依次开启接入与同步写、批准写者，再把查询服务扩到健康数量 |

全新安装通常需要约 25 分钟。完成后读取基线栈的输出：

```bash
aws cloudformation describe-stacks --region "$COLVENNA_REGION" --stack-name colvenna-isle-<name>-core \
  --query "Stacks[0].Outputs[?OutputKey=='ApiUrl' || OutputKey=='WebUrl']"
```

| 输出 | 用途 |
| --- | --- |
| `ApiUrl` | 公开 API 地址，所有请求都需要 `Authorization: Bearer <API_KEY>` |
| `WebUrl` | Query Editor（`<WebUrl>/`）与 Admin Console（`<WebUrl>/admin.html`） |

### 6.1 某一步失败时

安装在失败的那一步停止，不会重放结果不确定的操作。处理顺序：

1. 先按该步回执核对原操作的实际结果；
2. 再写入一个**新的**回执目录，用 `--steps` 从该步继续：

```bash
$COLVENNA /tmp/colvenna-build/bin/python scripts/aws/install.py fresh --release <release> \
  --output /path/outside/repo/receipts/install-<name>-resume --steps writer,control-registry,intake,query,sync,editor,open
```

control 与 writer 需要 environments 步骤输出的构建器配置。所以只要选了这两步，工具会自动带上 environments；构建器栈没有变化时，这一步是已确认的 no-op。

对同一 release 重新运行，结果是幂等的：

- 制品以“源码 + SHA-256”为键，镜像使用不可变 tag，重复发布时先核对内容一致，再复用原版本；
- 栈已运行完全相同的模板时，CloudFormation 的 “No updates are to be performed.” 记为已确认的 no-op（回执中 `NoUpdates: true`）；
- 栈仍在 `UPDATE_IN_PROGRESS` 等其他状态时，照常失败。

## 7. 开通空间与首个管理员 Key

空间是数据与权限的隔离单位。运维开通空间，并为它签发首个管理员 Key。这个 Key 只能签发一次：

```bash
$COLVENNA /tmp/colvenna-build/bin/python scripts/aws/control.py provision --space <space> \
  --output /path/outside/repo/receipts/provision-<space>
$COLVENNA /tmp/colvenna-build/bin/python scripts/aws/control.py bootstrap-key --space <space> \
  --expires-at 2027-12-31T00:00:00Z --token-output /secure/path/<space>-admin.token \
  --output /path/outside/repo/receipts/bootstrap-<space>
```

- `<space>` 以字母或下划线开头，只含字母、数字、`_`、`.`、`-`，最长 128 个字符。建议只用小写字母、数字和下划线，例如 `sales_prod`。
- `--token-output` 必须是一个尚不存在的新文件，Key 只写入这个文件，不会出现在回执里。请把它当作密码保管。
- 每个空间（每个 generation）只能签发一次。再次执行返回 `409 BOOTSTRAP_ALREADY_ISSUED`，不会签发新 Key，也不会留下文件。
- 之后的 Key（开发、接入、写入、分析等角色）由管理员通过 API 或 Admin Console 签发，见[用户手册](user-guide.md)。

空间配额由运维设置：

```bash
$COLVENNA /tmp/colvenna-build/bin/python scripts/aws/control.py quota --space <space> --policy-file quota.json \
  --output /path/outside/repo/receipts/quota-<space>
```

一个写者进程在其生命周期内最多打开 2 个空间的数据库。第三个空间的写入、接入或导入会返回 `503 WRITER_DATABASE_LIMIT`，直到写者重启（先 close，再 open）。需要更多活动空间时，请部署多套环境。

## 8. 验证安装

用 [quickstart](../examples/quickstart/README.md) 走一遍 API 接入、同步写、SQL 与 pandas 流水线和查询：

```bash
python3 -m venv /tmp/quickstart && /tmp/quickstart/bin/python -m pip install PyYAML==6.0.1
/tmp/quickstart/bin/python examples/quickstart/quickstart.py --api-url <ApiUrl> --space <space> \
  --admin-token-file /secure/path/<space>-admin.token
```

运行环境构建需要 5–10 分钟。全部结果与预期一致时，脚本最后输出 `PASS`。

## 9. 开放、关闭与计时

```bash
$COLVENNA /tmp/colvenna-build/bin/python scripts/aws/admission.py close --output <new-dir>
$COLVENNA /tmp/colvenna-build/bin/python scripts/aws/admission.py open --output <new-dir> [--query-count 1|2]
$COLVENNA /tmp/colvenna-build/bin/python scripts/aws/admission.py timing --timing on|off --output <new-dir>
```

| 操作 | 作用 |
| --- | --- |
| `close` | 停止写者（取得原任务的 STOPPED 终止回执）、把查询服务缩到 0，并关闭接入与同步写。每次升级、重建或部署写者之前都要先 close |
| `open` | 要求全部组件同源、认证已启用，然后开放 |
| `timing` | 只能在关闭状态下切换分段计时日志（`COLVENNA_TIMING`），用于测量计时开销 |

部署写者时，接入队列与 DLQ 都必须为空。仍有排队的原件时，先开放让写者处理完，或者按原身份核对后再处置。部署工具不会越过未核对的工作。

## 10. 破坏性升级

2.0.0 的 Python、配置、协议与系统列改名是大版本破坏性升级，契约 revision 为 2、原生空间格式为 6，旧名称不再作为别名接受。执行前按[迁移设计](design/identifier-migration.md)停止生产者与调度、核对 UNKNOWN，在旧服务仍可查询时导出完整 ODS 事件（包括删除 tombstone）和 direct 业务行，核对类型与行数并生成受保护的重放计划。然后用旧 tag 的工具备份、关闭、卸载旧运行时，保留历史备份库。用新源码全新安装、注册数据集、重放 ODS 与 direct 业务数据、重新发布并全量重建流水线和键状态。新系统列由写者重新生成，旧回执与水位不沿用。旧原生备份只能按旧版本隔离还原，不能直接还原到格式 6。

新工具不能发现或清理旧资源名与旧标签。存储镜像因 `/opt/colvenna` 路径变更更新了 digest；不能仅复制旧镜像并沿用旧 digest，需将新锁定镜像发布到目标账号与区域的 `colvenna-isle-images`。本地构建可用 `build_product.py --storage-image <local-image-reference>` 验证尚未发布的镜像，仍必须匹配锁定的 image ID / ARM64。`upgrade` 只适用于同命名空间、同契约与原生格式内的源码升级；跨格式必须使用上述导出 / 重建路径。

1.1.0 的区域化改变了运行时 IAM 命名空间。已有 tag `v1.0.0` 环境需先用该版本的工具备份并卸载，再用 1.1.0 全新安装，按当前合同重新生成数据。下面的 `upgrade` 流程用于运行时命名空间相同的源码版本之间升级。

各版本不提供向后兼容、原地自动迁移或零停机升级。2.0.0 的离线业务数据重放不迁移内部账本。升级步骤：

```bash
# 在新版本的干净检出中构建新 release（第 5 节），然后：
$COLVENNA /tmp/colvenna-build/bin/python scripts/aws/install.py upgrade --release <new-release> --output <new-dir>
```

`upgrade` 的步骤为：close → vault → environments → writer → control → auth → intake → query → sync → editor → open。

- 先 close，旧写者取得终止回执之后，才批准新写者。旧调用和迟到的产物不能向新环境提交。
- 合同或存储格式有变化时，按空间重建，旧数据不迁移，需要用当前版本重新生成：

  ```bash
  $COLVENNA /tmp/colvenna-build/bin/python scripts/aws/control.py rebuild --space <space> --output <new-dir>
  ```

  重建后：
  - 空间切换到新的 generation；
  - 旧 generation 的运行、候选、导出和消息都以 `SPACE_GENERATION_CHANGED` 被拒绝；
  - 已发布的流水线版本需要重新发布（`PIPELINE_VERSION_BINDING_CHANGED`）。

- 不再需要的旧 generation：
  1. 先只生成清理计划：

     ```bash
     $COLVENNA /tmp/colvenna-build/bin/python scripts/aws/control.py purge-generation --space <space> --generation <old> --output <new-dir>
     ```

  2. 核对计划无误后，加 `--execute` 执行删除。

升级中途失败或被中断时，环境中会留下源码不一致的组件，`admission.py open` 会因此拒绝开放。对同一 release 重新运行 `upgrade` 即可（见 6.1）。

## 11. 备份与隔离还原

### 11.1 备份

```bash
$COLVENNA /tmp/colvenna-build/bin/python scripts/aws/backup.py create --space <space> \
  --token-file /secure/path/<backup-key>.token --output <new-dir>
```

- 需要一个在该空间持有 `backup:manage` 的 Key。
- 备份由写者在原生锁下的一个读事务中完成，并固定一个提交序号 P：序号不大于 P 的提交全部在备份内，大于 P 的都不在。
- 备份期间仍可接入和同步写：新请求照常得到 `202` 或 `200`，在快照结束后以大于 P 的序号提交。
- 每个对象都以 SHA-256 checksum 条件写入，按版本读回核对。

| 内容 | 处理 |
| --- | --- |
| 原生数据库：全部 schema，含账本、接入 / 写入回执、水位、发布 | 备份点 P 的精确快照 |
| 空间配置、已注册数据集与接入策略 | 原样备份 |
| 流水线定义、版本与代码 | 备份 |
| 运行环境规格 | 备份规格；在目标环境中重新构建 |
| 配额、保留策略与保留锁 | 备份；还原时重新设置 |
| 审计记录 | 只归档，不还原 |
| API Key | **不备份**；还原后为空间签发新的管理员 Key |

核对一个备份及其引用的全部对象：

```bash
$COLVENNA /tmp/colvenna-build/bin/python scripts/aws/backup.py inspect \
  --backup-uri s3://<vault>/backups/<space>/<gen>/<backup_id>/backup.json --version-id <version> --output <new-dir>
```

### 11.2 隔离还原

还原目标必须是**另一套**环境。前置条件：

- 目标环境运行同一 release；原生运行时身份（DuckDB 与三个扩展的 hash）不一致时，还原返回 `BACKUP_RUNTIME_MISMATCH`；
- 该空间在目标环境中尚不存在。

```bash
/usr/bin/env -u AWS_PROFILE -u AWS_DEFAULT_PROFILE COLVENNA_REGION=$COLVENNA_REGION AWS_REGION=$COLVENNA_REGION AWS_DEFAULT_REGION=$COLVENNA_REGION COLVENNA_ENVIRONMENT=<target> \
  /tmp/colvenna-build/bin/python scripts/aws/backup.py restore \
  --backup-uri s3://<vault>/backups/<space>/<gen>/<backup_id>/backup.json --version-id <version> \
  --token-output /secure/path/<space>-restored-admin.token --output <new-dir> [--schedules restore]
```

还原流程：

1. 校验阶段：
   - 原生还原只读取源环境；
   - 用 `--version-id` 指定的精确版本和 SHA-256 核对备份清单；
   - 清单中的每条 DDL 先在封闭的内存目录中预演，必须原样回读，否则以 `RESTORE_MANIFEST_DDL_REJECTED` 拒绝，不产生任何效果。
2. 装载阶段：在一个事务内完成装载并逐表核对行数、摘要与物理指纹。任何不一致都会回滚（`RESTORE_VERIFICATION_FAILED`）。
3. 重建阶段：通过目标环境的公开 API，用新签发的管理员 Key 重建配额、保留策略、运行环境与流水线。调度默认关闭；需要恢复调度时加 `--schedules restore`。

还原回执记录各阶段耗时。项目没有约定 RPO / RTO 目标：

- RPO 等于最近一次备份点之后提交的工作；
- RTO 主要由运行环境的重新构建决定。

实测值见[发行说明](release-notes.md)。

限制：

- 不支持跨产品版本还原。
- 备份期间同步写与 FIFO 提交要等待快照完成，等待时间随数据量线性增长。
- 写者的 `/work`（21 GiB）需要能容纳一次压缩后的导出。

## 12. 卸载

```bash
$COLVENNA /tmp/colvenna-build/bin/python scripts/aws/uninstall.py --output <new-dir>
```

卸载按以下顺序执行：

1. 关闭接入：写者取得终止回执，查询服务缩到 0。
2. 删除控制面栈。
3. 删除构建器创建的运行时 Lambda、任务定义和执行角色。每一项都必须同时带有本环境前缀和 `Project=colvenna-isle` 标签，否则拒绝删除。
4. 删除构建器栈。
5. 删除基线栈。只清空该栈自己的桶，只清理附着在该栈 VPC 上的 GuardDuty 端点。
6. 核对三个栈都已不存在。

账号级备份库与镜像仓库不在删除范围内，需要时单独清理。

## 13. 安全要点

- 不使用静态 Access Key。服务使用各自的最小权限角色；运维工具要求 EC2 实例角色，并核对 `COLVENNA_ACCOUNT`。
- 数据桶与备份库：
  - 开启版本控制、SSE-S3、BucketOwnerEnforced 与全部 Public Access Block；
  - 拒绝非 TLS 访问。
- 公开 API 只接受 `Authorization: Bearer <API_KEY>`：
  - Key 只保存 SHA-256 摘要；
  - Key 有到期时间，可以随时吊销或轮换；
  - 管理接口要求相应 scope。
- 写者、查询服务与同步写代理都在私有子网中，只接受来自指定安全组的流量。
- Key 文件（`--token-output`、`--token-file`）是唯一保存明文 Key 的地方。请限制文件权限，用完删除，不要提交到仓库。

## 14. 故障排查

| 现象 | 原因与处理 |
| --- | --- |
| `Set COLVENNA_ACCOUNT …` / `Expected an assumed role in COLVENNA_ACCOUNT` | 没有设置 `COLVENNA_ACCOUNT`，或当前凭证不是该账号的实例角色。检查第 3.2 节的环境 |
| `Use the EC2 role credential chain; no static keys` | 环境中存在 profile 或静态 key，用 `/usr/bin/env -u AWS_PROFILE …` 执行 |
| 构建报 `docker image inspect` 失败 | 构建主机上没有锁定的存储镜像，见第 4 节 |
| 写者部署拒绝：队列或 DLQ 非空 | 先开放让写者处理完排队的原件，或按原身份核对后再处置 |
| `admission.py open` 拒绝开放 | 组件源码不一致（部署中断），对同一 release 重新运行 `upgrade` |
| `503 WRITER_DATABASE_LIMIT` | 写者已打开 2 个空间，见第 7 节 |
| 查询 `QUERY_TIMEOUT` | 写入高峰期间或刚结束时，分析查询会变慢；设置更长的 `timeout_ms`（最大 300,000）或错开高峰，见[发行说明](release-notes.md#限制) |

DuckDB is a trademark of the DuckDB Foundation. Colvenna Isle is independent of the DuckDB Foundation and is not endorsed by it.
