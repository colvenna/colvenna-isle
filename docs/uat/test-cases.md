# Colvenna Isle UAT 测试用例

本文定义发行前的用户验收测试（UAT），自 1.0.0 起适用，当前发行版本为 2.0.0（tag `v2.0.0`）。每个用例都按[安装部署手册](../installation.md)和[用户手册](../user-guide.md)中写明的操作执行，并给出明确的判定标准。

执行结果记录在 UAT 报告中，随各版本的 [GitHub Release](https://github.com/colvenna/colvenna-isle/releases) 发布；2.0.0 的逐项结果见 [UAT 报告](v2.0.0-report.md)；1.1.0 的历史区域验收范围见[发行说明](../release-notes.md#验收)。

## 1. 范围

UAT 覆盖以下角色在实际 AWS 环境中的完整使用路径：

| 角色 | 覆盖内容 |
| --- | --- |
| 运维 | 构建、安装、开通空间、开放 / 关闭、升级、重新部署、备份与隔离还原、卸载 |
| 空间管理员 | API Key 生命周期、配额、保留、审计 |
| 应用开发者 | 数据集注册、异步接入、同步写 |
| 数据工程师 | 运行环境、流水线发布、运行、调度、取消与重试、Airflow |
| 分析人员 | 查询与一致性、Query Editor |

UAT 只包含功能用例，**默认不执行性能测试**。性能与容量测试按[性能测试说明](../performance-tests.md)单独执行，结果见[性能报告](../performance.md)。故障注入（例如丢失回复、写者接管和 UNKNOWN 核对）属于鉴定测试，同样不属于 UAT。

## 2. 环境

| 环境 | 用途 | 安装方式 |
| --- | --- | --- |
| A | 功能用例 | 按手册执行 `./install.sh fresh`，并通过 `--` 加上只用于鉴定的 `--writer-probe-space`：它为一个 `probe_*` 空间开放只读的原生观测接口，用来独立核对提交结果，不改变产品行为 |
| B | 全新安装、quickstart、隔离还原、升级 / 重新部署、卸载 | 严格按手册执行 `./install.sh fresh`，不加任何鉴定选项 |

2.0.0 使用 `us-west-2` 的两套新环境，确切候选和用于验证部署源码身份变化的同树提交见 [UAT 报告](v2.0.0-report.md)。两套环境位于同一账号、同一区域，使用同一份 release。历史版本（1.0.0 在 `us-west-2` 执行；1.1.0 的全部功能用例在 `us-east-2` 执行，所用源码与 1.1.0 的运行时代码相同，见[发行说明](../release-notes.md#验收)）。浏览器用例使用 Chromium 148（Playwright 1.60.0）。

## 3. 判定规则

- 用例中的每一项预期结果都满足时，该用例记为 `PASS`；任何一项不满足，记为 `FAIL`。
- 用例因前置条件不满足而无法执行时，记为 `NOT_RUN`，并写明原因。
- `NOT_RUN` 与 `FAIL` 都不计为通过，也不以豁免代替。
- 发现缺陷时，必须修复后用同一个 release 候选重新执行受影响的用例。原失败结果保留在报告中。
- 每个用例的证据包括请求与响应、回执、截图等。证据中不得出现明文 API Key 或 AWS 凭证。
- 发行门槛：全部用例 `PASS`。

## 4. 安装与部署（运维）

| ID | 场景 | 步骤 | 预期结果 | 环境 |
| --- | --- | --- | --- | --- |
| UAT-INS-01 | 构建 release | 在 tag 的干净检出上执行 `build_product.py` | `manifest.json` 为 `PASS`；control、writer、query、builder-source、editor 五个组件都成功，镜像内原生套件通过；manifest 记录源码 commit / tree，不含账号 ID | — |
| UAT-INS-02 | 全新安装 | `./install.sh fresh`（内部执行 `install.py fresh`） | 12 个步骤全部成功；全部组件运行同一源码；公开 API 对缺失凭证返回 401，对无效凭证返回 403 | B |
| UAT-INS-03 | 开通空间与首个管理员 Key | `control.py provision`、`bootstrap-key` | 空间开通成功；Key 只写入指定的新文件；用这个 Key 可以签发角色 Key；同一空间再次签发首个管理员 Key 被拒绝，且不写出文件 | B |
| UAT-INS-04 | Quickstart | 按 quickstart 运行示例 | 输出 `PASS`；ADS 看板结果与文档中的表格一致；输出中不含凭证 | B |
| UAT-INS-05 | 全新环境端到端 | 在新空间注册数据集、构建运行环境、同步写、发布 SQL + Python 流水线 | 同步写提交，并能用 `after_write` 精确读回；流水线发布，结果与独立预期一致；缺少 scope、缺少 Bearer、接入 Key 写直接表都被拒绝 | B |
| UAT-INS-06 | 破坏性升级 | 用 `./install.sh upgrade` 把环境升级到一个新的 release，再 `control.py rebuild`、`admission.py open` | 升级后全部组件运行新源码；旧写者取得终止回执后才批准新写者；重建后可以继续使用 | B |
| UAT-INS-07 | 升级时旧工作被拒绝 | 保留一个运行中的计算尝试，然后升级并 `control.py rebuild` | 旧写者被确认停止，其身份被拒绝；旧候选迟到发布被拒绝；旧运行在新 generation 中不可见；重建后的同步写从序号 1 开始；数据与权限正确 | B |
| UAT-INS-08 | 部署中途中断 | 在部署 control 时强制终止安装进程，并核对已提交的 CloudFormation 操作到终态 | `admission.py open` 拒绝开放源码不一致的环境，写者保持停止 | B |
| UAT-INS-09 | 同一 release 重新部署 | 对中断后的环境再次执行 `upgrade` | 没有变化的栈记为 no-op；全部组件运行同一源码并开放；数据完整，可以继续提交 | B |
| UAT-INS-10 | 卸载 | `uninstall.py` | 三个栈都被删除；运行时 Lambda、任务定义和角色都被删除；账号级备份库保留 | B |

## 5. 访问控制与 API Key（空间管理员）

| ID | 场景 | 步骤 | 预期结果 | 环境 |
| --- | --- | --- | --- | --- |
| UAT-KEY-01 | 按角色签发 Key | 用管理员 Key 签发开发、接入、写入、分析等角色 Key | 每个 Key 只具备所授予的 scope，并能按 scope 访问；超出 scope 的请求被拒绝 | A、B |
| UAT-KEY-02 | 委派边界 | 尝试越权：签发更高 scope、更长有效期、跨空间管理、吊销更高权限的 Key | 全部被拒绝，并返回合同规定的错误码 | A |
| UAT-KEY-03 | 创建、轮换和吊销 | 在 Admin Console 中操作（A），并通过 API 操作（B） | 创建的 Key 可以认证，令牌隐藏后从页面消失；轮换时新旧 Key 有关联且都能认证；吊销立即生效，替代 Key 继续可用；列表中从不显示令牌或摘要 | A、B |
| UAT-KEY-04 | 跨空间隔离 | 用其他空间的 Key 访问目录、查询、写入 | 全部被拒绝 | A |
| UAT-KEY-05 | Key 列表与日志不泄露凭证 | 列出 Key，检查页面与服务日志 | 不出现令牌或摘要 | A |

## 6. 数据集与异步接入（应用开发者）

| ID | 场景 | 步骤 | 预期结果 | 环境 |
| --- | --- | --- | --- | --- |
| UAT-ING-01 | 注册数据集 | 注册接入数据集、直接表与分析数据集 | 注册成功，目录中只列出已注册的数据集 | A |
| UAT-ING-02 | 批量接入 | 提交多批事件，查询接入状态；用同一 Idempotency-Key 重试 | 返回 `202`，接入状态最终为 `COMMITTED`，数据可查询；重试返回同一个 `ingestion_id` | A、B |
| UAT-ING-03 | 幂等与版本 | 重复事件、同一 key 的新旧版本、删除墓碑、迟到的旧版本 | 重复事件只记录一次；新版本覆盖旧版本；墓碑阻止迟到的旧版本复活 | A |
| UAT-ING-04 | 接入负例 | 过期 schema、未注册的操作、类型错误的记录、未注册的数据集、缺少 Idempotency-Key、没有 `ingest:write` | 分别返回 409、403、400、404、428、403，不产生原生效果 | B |
| UAT-ING-05 | 接入可追溯 | 追踪一次接入 | 能看到原始归档、原生提交和消费它的运行 | A |

## 7. 同步写（应用开发者）

| ID | 场景 | 步骤 | 预期结果 | 环境 |
| --- | --- | --- | --- | --- |
| UAT-SYN-01 | 四种操作 | insert、upsert、update、delete，以及影响 0 行的 update / delete | 返回 `200 COMMITTED`；`after_write` 精确读回；每个原件只有一个提交序号；影响 0 行的写入也有持久回执 | A |
| UAT-SYN-02 | 幂等 | 并发重试同一原件；用同一 Idempotency-Key 提交不同内容 | 并发重试只产生一个结果；内容不同返回 `409` | A |
| UAT-SYN-03 | 原生约束 | 违反主键、唯一、CHECK、NOT NULL、外键 | 整批拒绝，拒绝结果被保留，表中没有部分写入 | A |
| UAT-SYN-04 | 权限与目标 | 无 Bearer、接入 scope、只读 scope、非所有者、跨空间、流水线拥有的目标、内部表 | 全部被拒绝，不产生原生效果 | A |
| UAT-SYN-05 | 类型与已知限制 | 写入边界类型；修改被引用行的索引列或 LIST 列 | 边界类型经查询精确读回；修改被引用行的索引列或 LIST 列时整批拒绝（`NATIVE_CONSTRAINT`），普通列可以更新 | A |
| UAT-SYN-06 | 写入与分析发布分离 | 同步写之后查询 published | 直接写入不会发布分析 release；release 单独发布，结果与独立汇总一致 | A |

## 8. 查询（分析人员）

| ID | 场景 | 步骤 | 预期结果 | 环境 |
| --- | --- | --- | --- | --- |
| UAT-QRY-01 | 一致性 | 分别用 published、指定历史 release、after_write 查询 | 结果分别与发布结果、历史 release、写后状态的独立预期一致 | A |
| UAT-QRY-02 | 只读 SQL | 提交 DDL、DML、多语句、COPY、ATTACH、PRAGMA、SET、LOAD、外部文件、账本表、其他 catalog | 全部被拒绝 | A |
| UAT-QRY-03 | 结果归属 | 其他读者、管理员、其他空间读取结果；他人取消查询 | 结果只对所有者可见，导出令牌绑定所有者，他人不能取消 | A |
| UAT-QRY-04 | 配额 | 超过空间查询队列配额 | 以明确的配额错误被拒绝 | A |
| UAT-QRY-05 | 分页、导出、取消与超时 | 以小页大小翻页；读取导出链接；不带令牌访问导出；取消运行中的查询；设置很短的 `timeout_ms` | 各页拼接后与完整结果一致；导出返回同样的类型化结果；不带令牌的导出被拒绝；取消后状态为 `CANCELLED`；超时以 `QUERY_TIMEOUT` 结束 | B |

## 9. 流水线（数据工程师）

| ID | 场景 | 步骤 | 预期结果 | 环境 |
| --- | --- | --- | --- | --- |
| UAT-PIP-01 | 运行环境 | 用锁定依赖构建 pandas 与 DuckDB 运行环境；构建失败的定义 | 构建成功，状态为 READY 并有执行器绑定；构建失败时显示阶段、错误码和构建信息 | A |
| UAT-PIP-02 | 发布与运行 | 创建草稿、上传代码、发布并激活，然后运行 | 运行发布，结果与独立预期一致 | A |
| UAT-PIP-03 | 增量、分区与全量 | 多批数据后分别执行增量和分区重算 | 增量结果等于全量结果，分区重算等于全量结果；未受影响的分区保持原样 | A |
| UAT-PIP-04 | 规则变更 | 修改流水线规则后发布新版本 | 重新 bootstrap，全量重建后与预期一致 | A |
| UAT-PIP-05 | 三种执行器 | 同一转换分别在 writer、Lambda、ECS 中执行 | 三者结果相同，并与独立 DuckDB 计算一致 | A |
| UAT-PIP-06 | 取消与重试 | 发布前取消；对失败的运行重试 | 取消后不发布任何结果；重试必须是所有者并带 Idempotency-Key，是幂等的；已发布的运行不能重试 | A |
| UAT-PIP-07 | 调度 | 启用调度，再暂停；把 owner 设为外部 | 自动运行按窗口发布，不丢失区间；暂停后不再产生新的自动运行；外部 owner 下 Colvenna Isle 不自动运行 | A |
| UAT-PIP-08 | Airflow | 用示例 operator 由外部 Airflow 运行 | 发布后放行下游；重跑和超时都落到同一运行；未授权 Key 与错误版本被拒绝 | A |
| UAT-PIP-09 | 可追溯与试运行 | 追踪一次运行；在控制台做样本试运行 | 能看到输入、计算、导入、发布；试运行不改变 release 或水位 | A |
| UAT-PIP-10 | 流水线权限 | 无权限的角色创建、发布、运行；把定义写入接入目标 | 全部被拒绝 | A |

## 10. Query Editor 与 Admin Console

| ID | 场景 | 步骤 | 预期结果 | 环境 |
| --- | --- | --- | --- | --- |
| UAT-WEB-01 | Query Editor | 用 Key 连接、浏览目录、运行查询、翻页、导出、取消、断开 | 目录只含授权数据集，不含账本；结果与 API 一致；拒绝 DDL 和账本查询；没有 query scope 或用其他空间的 Key 时无法使用；Key 只保存在内存中，断开后页面与存储中不留 Key | A、B |
| UAT-WEB-02 | Admin Console 概览 | 打开概览 | 写者、数据集与准入状态和 API 一致，并显示观测时间与来源；停止或过期的状态显示为 STOPPED / UNKNOWN，不显示为正常 | A |
| UAT-WEB-03 | Admin Console 流水线 | 在页面上构建运行环境、编辑草稿、发布并运行 | 结果与 API 一致；版本冲突和取消竞争如实显示 | A |
| UAT-WEB-04 | 页面安全 | 检查浏览器请求与控制台 | 只访问同源地址，不使用 cookie 或 AWS 凭证；没有页面错误或 CSP 违规 | A |

## 11. 治理（空间管理员 / 运维）

| ID | 场景 | 步骤 | 预期结果 | 环境 |
| --- | --- | --- | --- | --- |
| UAT-GOV-01 | 审计 | 签发 Key、修改定义与运行环境、运行、重放后查看审计 | 每项操作都有审计记录；需要 `audit:read`；记录中没有凭证或摘要 | A |
| UAT-GOV-02 | 诊断与追踪 | 追踪一次失败的运行、一个 release、一次接入 | 追踪结果与原始的 ECS 记录、运行记录和原生账本一致；冲突或缺失时显示 UNKNOWN | A |
| UAT-GOV-03 | 保留与回收 | 设置保留策略和保留锁；先 dry-run，再回收 | dry-run 不删除；回收只删除到期对象，并留下回执和审计；被锁定的 release 仍可读；活跃运行不受影响；原生数据库完好 | A |
| UAT-GOV-04 | 旧 generation 清理 | rebuild 后执行 `purge-generation` | 当前 generation 永不被清理；只删除旧 generation，新 generation 可以继续提交 | A |
| UAT-GOV-05 | 配额与公平 | 运维设置配额后，超额提交制品、流水线、样本、接入、运行、查询 | 超额时按合同拒绝或等待；等待的工作在容量恢复后完成；状态中写明等待的原因 | A |
| UAT-GOV-06 | 成本 | 读取成本报告 | 计算、导入、导出、构建都按实际身份计量；未计量的工作负载逐项列出 | A |

## 12. 备份与隔离还原（运维）

| ID | 场景 | 步骤 | 预期结果 | 环境 |
| --- | --- | --- | --- | --- |
| UAT-BAK-01 | 一致性备份 | 在持续同步写和接入的同时执行 `backup.py create`，再 `inspect` | 备份完成；备份期间工作继续，并在备份点之后提交；清单与每个对象都能按版本核对 | A |
| UAT-BAK-02 | 隔离还原 | 在另一套环境执行 `backup.py restore` | 原生数据只包含备份点及之前的提交；原写入回执被还原；备份点之后的原件不存在；还原后的空间可以查询、继续写入和接入，流水线可以继续发布；源环境的 Key 在这里无效；源环境不受影响 | A → B |
| UAT-BAK-03 | 篡改的备份清单 | 修改清单中的 DDL 或列类型后还原 | 以 `RESTORE_MANIFEST_DDL_REJECTED` 拒绝，目标库没有任何变化（release 构建时在原生套件中执行） | — |

## 13. 文档与发行

| ID | 场景 | 步骤 | 预期结果 | 环境 |
| --- | --- | --- | --- | --- |
| UAT-DOC-01 | 文档检查 | `python3 scripts/check_docs.py` | PASS | — |
| UAT-DOC-02 | 发行身份一致 | 对照 README、发行说明、Press Release 与 `_version.py` | 版本与 tag 一致；发行说明写明区域与 tag | — |
| UAT-DOC-03 | 公开仓库不含部署身份 | 扫描整个仓库 | 不含 AWS 账号 ID、IAM 角色名、凭证或开发环境标识 | — |

DuckDB is a trademark of the DuckDB Foundation. Colvenna Isle is independent of the DuckDB Foundation and is not endorsed by it.
