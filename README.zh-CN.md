<h1 align="center" id="colvenna-isle">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="docs/assets/logo-dark.svg">
    <img src="docs/assets/logo-light.svg" alt="Colvenna Isle" width="480">
  </picture>
</h1>

<p align="center"><strong>S3 上的持久化分析数据库</strong></p>

<p align="center">
  <a href="LICENSE"><img alt="License: Apache-2.0" src="https://img.shields.io/badge/License-Apache--2.0-0b6b69"></a>
  <a href="docs/release-notes.md"><img alt="Release: 1.1.0" src="https://img.shields.io/badge/Release-1.1.0-475569"></a>
</p>

<p align="center">
  <a href="docs/assets/promo.mp4"><img src="docs/assets/promo.gif" alt="一分钟了解 Colvenna Isle：API 接入、Amazon S3 上的持久化数据库、SQL 与 pandas 流水线、Query Editor 和 Admin Console" width="960"></a>
</p>

<p align="center"><a href="docs/assets/promo.mp4">▶ 观看带英文配音的完整版（MP4，1 分钟）</a></p>

[English](README.md) · [发行说明](docs/release-notes.md) · [安装部署手册](docs/installation.md) · [用户手册](docs/user-guide.md) · [Quickstart](examples/quickstart/README.md) · [文档目录](docs/README.md) · [完整产品发布文档](docs/product/press-release.md) · [贡献说明](CONTRIBUTING.md)

Colvenna Isle 是面向 AWS 的数据库与分析产品，整合 API 接入、S3 上的持久化分析数据库、SQL/Python 分层加工、Query Editor 和 Admin Console。

## 1.1.0 发行

下一版本完成代码、配置、协议与数据中的 Colvenna 标识迁移；`main` 为 `2.0.0.dev0`（未发行）。这是大版本破坏性升级，见[迁移设计](docs/design/identifier-migration.md)；候选的 AWS 验收为 `NOT_RUN`。

已发布的 1.1.0（[GitHub Release `v1.1.0`](https://github.com/colvenna/colvenna-isle/releases/tag/v1.1.0)）包含当前合同与能力，均已在实际 AWS 中验收：

- 由你选择的单个 AWS 账号（账号 ID 来自部署环境，不写在源码中）、ARM64；
- 安装时指定的标准 AWS 商业区域（`--region` / `COLVENNA_REGION`）：`us-east-2` 完成全新安装、全部 56 项功能 UAT、备份与隔离还原、升级、中断重部署与卸载，`us-west-2` 完成回归；其他商业区域在配置上可用，但未单独验收；
- 运行在 S3 对象文件系统上的 DuckDB 1.5.6；
- 写者与查询服务规格为 2 vCPU / 4 GiB。

1.1.0 还改用带类型的 Arrow 批次装载 `after_write` / `after_ingestion` 快照，并在复制前检查行数与字节配额。从 1.0.0 升级需要先用 1.0.0 的工具卸载，再全新安装 1.1.0。变更、验收证据、支持矩阵、规格、限制、测得基线和尚未实现的计划项见[发行说明](docs/release-notes.md)，实测基线见[性能报告](docs/performance.md)（不承诺 SLA）。用户验收标准见 [UAT 测试用例](docs/uat/test-cases.md)。

## 文档

| 文档 | 面向 |
| --- | --- |
| [安装部署手册](docs/installation.md) | 运维：部署主机、原生存储镜像、构建 release、全新安装、开通空间、开放 / 关闭、破坏性升级、备份与隔离还原、卸载 |
| [用户手册](docs/user-guide.md) | 开发、数据工程与分析人员：API Key、数据集、接入、同步写、查询、流水线、Query Editor 与 Admin Console |
| [API 契约](docs/design/api-contract.md) | 每个路由、权限、限制和错误码 |
| [性能测试说明](docs/performance-tests.md) | 性能与容量测试的方法；单独执行，不属于 UAT（[实测结果](docs/performance.md)） |
| [成本估算](docs/cost-estimate.md) | 按三个数据档位粗估一个环境每月的 AWS 费用 |
| [设计文档](docs/README.md) | 架构、同步写、流水线、Python 任务合同、运行环境 |

## 开始使用

1. 在 tag `v1.1.0` 的干净检出中运行 `./install.sh fresh --environment <名称> --account <账号 ID> --region <区域>`。它先检查部署主机（带实例角色的 ARM64 EC2，以及 Python 3.12、支持 `linux/arm64` 的 Docker、Node.js），再构建 release 并安装为一个新环境。见[安装部署手册](docs/installation.md)。
2. 开通空间并签发首个管理员 Key，然后运行 [quickstart](examples/quickstart/README.md)。它覆盖 API 接入、同步表写入与 `after_write` 读回，以及发布后的 SQL + pandas 流水线结果查询。
3. 继续阅读[用户手册](docs/user-guide.md)与 [API 契约](docs/design/api-contract.md)。

## 产品特性

- **API 接入**：批量写入、schema 校验、幂等处理、队列缓冲和接入状态查询。
- **同步业务表写入**：注册为 `direct` 的目标支持 insert/upsert/update/delete；`200 COMMITTED` 表示数据、原回执、变更和水位已在同一原生事务中提交。
- **S3 数据库**：保留 DuckDB 原生表、索引、约束与 SQL 能力，持久数据进入对象存储。
- **内置流水线**：SQL/Python DAG、任务级增量/全量/分区重算、环境版本、定时/手动触发、Lambda/ECS 执行及可选 Airflow 集成。
- **分层加工**：ODS、DWD、DWS、ADS 共用数据空间，记录处理水位及发布版本。
- **长任务处理**：固定输入版本、独立计算、S3 暂存、分块导入和版本发布。
- **SQL 编辑器**：目录浏览、SQL 编写、异步执行、取消、结果查看和导出。
- **管理控制台**：观察写者、队列、各层水位、加工和查询状态。
- **API Key 管理**：Bearer 认证、空间和权限绑定、期限、停用及轮换。

## 架构

![Colvenna Isle AWS 架构](docs/assets/architecture.svg)

```text
API Gateway → 接入 Lambda → SQS FIFO → ECS Fargate 写者 → S3 上的 DuckDB
API Gateway → 独立同步 Lambda → 写者私网 8082 → 同一原生事务与持久回执
```

API Gateway 使用 Lambda Authorizer 校验 `Authorization: Bearer <API_KEY>`。每库只有一个活动写者，查询和长计算使用独立资源。

DynamoDB 只使用一张控制面表，保存 API Key、空间路由、流水线/环境 metadata 和异步状态投影。接入 Lambda 直接发送 SQS；原始批次保存 S3 且 SQS 确认发送成功后返回 `202`，持久去重由 DuckDB 账本负责。

同步写使用独立的 `data:write` 权限和有界等待。超时或响应丢失返回原身份 `UNKNOWN`，调用者查询原回执或使用原幂等键核对；它不承诺回滚。四种写操作、原生约束及权威回执见[同步写设计](docs/design/synchronous-writes.md)。

Authorizer 和管理 Lambda 通过 AWS SDK 访问 DDB。SQL 请求通过 API Gateway → VPC Link → 内部 ALB 到 ECS 查询 Service。写者 Service 主动拉取 SQS，无需 ALB；调度 Lambda 调用批准的 Lambda 版本，或用 ECS RunTask 启动独立计算 Task。

数据库在 S3 中的物理对象由 SlateDB 管理，包括 SST、WAL 和 manifest。ODS/DWD/DWS/ADS 是原生 DuckDB 表；长任务使用的 Parquet 快照和暂存结果保存在独立前缀中。

四层先放在同一个数据库的四个 schema 中。长任务读取不可变输入，结果分块导入后发布；持续影响接入的分析写入负载，可以通过拆分接入库与服务库隔离。

Python 通过 context.json 读取命名 Parquet 输入，输出 Parquet 和清单；pandas 等包在发布前锁定全部依赖和 hash，构建固定运行环境。普通 Lambda 上限为 15 分钟，大任务选择 ECS。Lambda Managed Instances 与 Durable Functions profile 属于计划项，不在 1.1.0 范围内。

manifest 轮询不能证明常驻 DuckDB 附件的查询新鲜度，见[工程设计](docs/design/aws-product-design.md)。产品区分“已接收”“已提交”“分析版本已发布”，查询返回实际使用的数据版本。

[交互架构图](docs/design/architecture.html) · [工程设计](docs/design/aws-product-design.md) · [API 契约](docs/design/api-contract.md) · [流水线设计](docs/design/pipeline-design.md)

## 本地预览与检查

在仓库根目录运行：

```bash
python3 -m http.server 8025 --bind 127.0.0.1
```

打开 `http://127.0.0.1:8025/docs/design/architecture.html`。文档预览不需要 AWS 账号或凭证。

检查文档：

```bash
python3 scripts/check_docs.py
```

检查本地链接、SVG/JSON 语法和架构图内容 hash。架构图的浏览器检查见[验证记录](docs/design/architecture-validation.md)；这些证据不代表 AWS 服务验收。

[orders 示例](examples/orders/README.md) 提供可在本地运行的 SQL/pandas 文件合同检查与 Python 3.12/Linux arm64 依赖锁；[quickstart](examples/quickstart/README.md) 在已安装环境中发布同一条流水线。

[score_orders 配置详解](examples/orders/score-orders-guide.md) 说明 Python/Lambda 字段、临时文件及输出清单。

## 贡献与许可证

仓库改动通过独立分支和 Pull Request 提交。请参阅[贡献说明](CONTRIBUTING.md)和[安全说明](SECURITY.md)。

各版本不承诺向后兼容或旧数据迁移。升级是破坏性的：先关闭环境，再部署新 release，并重建受影响的空间（见[安装部署手册](docs/installation.md#10-破坏性升级)）。之后的版本如何处理破坏性变更，在各自的发行说明中说明。

项目源码、文档、设计及原创品牌资源采用 [Apache-2.0](LICENSE)。第三方组件（DuckDB 及其扩展、锁定的 Python 发行包、基础镜像、Archify 架构图运行代码）保留各自许可，详见[第三方许可](THIRD_PARTY_NOTICES.md)。

项目不属于 DuckDB 或 AWS 官方产品。

DuckDB is a trademark of the DuckDB Foundation. Colvenna Isle is independent of the DuckDB Foundation and is not endorsed by it.
