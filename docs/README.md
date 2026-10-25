# Colvenna Isle

**S3 上的持久化分析数据库** / **Durable analytics databases on S3**

## 入门与运维

- [安装部署手册](installation.md)：部署主机、原生存储镜像、构建 release、全新安装、开通空间、开放 / 关闭、破坏性升级、备份与隔离还原、卸载。
- [用户手册](user-guide.md)：API Key、数据集注册、异步接入、同步写、查询与一致性、SQL / Python 流水线、Query Editor 与 Admin Console。
- [Quickstart](../examples/quickstart/README.md)：在已安装环境中运行 API / SQL / Python 示例。
- [UAT 测试用例](uat/test-cases.md)：发行前的用户验收用例与判定标准（只含功能用例）。
- [性能测试说明](performance-tests.md)：性能与容量测试的矩阵、方法与判定；单独执行，默认不在 UAT 中。

## 发行

- [1.1.0 发行说明与支持矩阵](release-notes.md)：变更、验收证据、发行身份、支持的账号 / 区域 / 架构、规格、能力、限制与测得基线。
- [性能与容量基线](performance.md)：1.0.0 在固定规格下的同步写、接入、查询、并行与过载实测，分段计时与成本范围。
- [成本估算](cost-estimate.md)：按小 / 中 / 大三个数据档位，粗估一个环境每月的 AWS 费用。
- [完整产品发布文档 / Press Release](product/press-release.md)：对外产品文案。

## 设计

- [AWS 产品设计](design/aws-product-design.md)：接入、存储、四层数据、长任务、认证与控制台。
- [API 契约](design/api-contract.md)：写入、SQL 查询、监控和 API Key 管理的路由、权限、限制与错误码。
- [同步写设计](design/synchronous-writes.md)：insert / upsert / update / delete、独立权限、幂等身份、等待与 UNKNOWN 核对。
- [内置流水线](design/pipeline-design.md)：SQL / Python DAG、触发、水位、重试、发布及 Airflow 集成。
- [增量与全量](design/processing-modes.md)：任务级处理模式、键状态、受影响分区与输出更新语义。
- [Python 输入输出合同](design/python-task-contract.md)：固定 Parquet 输入、context、产物清单和校验。
- [包依赖与运行环境](design/runtime-environments.md)：pandas 等包的锁定、构建、版本及 Lambda / ECS 包装。
- [交互架构图](design/architecture.html)：独立 HTML，可本地预览；[架构源文件](design/architecture.json)；[架构图验证](design/architecture-validation.md)。

## 示例

- [Orders 示例](../examples/orders/README.md)：SQL / pandas 转换和文件合同检查。
- [score_orders 配置详解](../examples/orders/score-orders-guide.md)：字段、Lambda /tmp 文件布局、输出清单和发布过程。
- [Airflow operator](../examples/airflow/)：由外部 Airflow 编排流水线运行。

## Brand

- [浅色 logo](assets/logo-light.svg)、[深色 logo](assets/logo-dark.svg)、[图标](assets/icon.svg)。
- [README 架构图](assets/architecture.svg)。
- [品牌说明](assets/README.md)。

- [Colvenna 标识与 1.x 业务数据迁移](design/identifier-migration.md)
