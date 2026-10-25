# Colvenna Isle 用户手册

当前源码为未发行的 `2.0.0.dev0` 候选。包、配置、协议头与系统列统一采用 Colvenna 名称，契约 revision 为 2，原生格式为 6；1.x 用户需按[迁移设计](design/identifier-migration.md)升级。下文的 AWS 验收属于历史版本；候选的运行时验收为 `NOT_RUN`，见[发行说明](release-notes.md#200-unreleased)。

本手册面向应用开发者、数据工程师和分析人员，说明如何在一个已安装的 Colvenna Isle 环境中工作：

- 管理 API Key；
- 注册数据集；
- 通过异步接入和同步写写入数据；
- 用 SQL 查询数据；
- 构建、发布和运行 SQL / Python 流水线；
- 使用 Query Editor 与 Admin Console。

安装、升级、开通空间、备份还原与卸载属于运维工作，见[安装部署手册](installation.md)。每个路由、字段和错误码的完整定义见 [API 合同](design/api-contract.md)。规格、支持矩阵和已知限制见[发行说明](release-notes.md)。想先跑通一遍完整流程，可以运行 [quickstart](../examples/quickstart/README.md)。


1.1.0 的区域由安装环境指定，为标准 AWS 商业区域；使用者继续使用该环境的 API / Web 地址。验收覆盖 `us-east-2` 全部 56 项功能 UAT 与 `us-west-2` 回归，其他商业区域未单独验收。账号、区域、镜像和备份库前置条件见[安装手册](installation.md#33-区域前置条件)，实际验收范围见[发行说明](release-notes.md#验收)。

## 目录

1. [概述与核心概念](#1-概述与核心概念)
2. [开始之前](#2-开始之前)
3. [API Key](#3-api-key)
4. [数据集注册](#4-数据集注册)
5. [异步接入](#5-异步接入)
6. [同步写](#6-同步写)
7. [查询](#7-查询)
8. [流水线](#8-流水线)
9. [Query Editor](#9-query-editor)
10. [Admin Console](#10-admin-console)
11. [限制与配额汇总](#11-限制与配额汇总)
12. [错误码速查](#12-错误码速查)
13. [常见问题与故障排查](#13-常见问题与故障排查)

## 1. 概述与核心概念

### 1.1 使用入口

| 入口 | 用途 | 地址 |
| --- | --- | --- |
| 公开 API | 写入、查询、流水线与管理操作 | 安装输出 `ApiUrl`，路径以 `/v1/` 开头 |
| Query Editor | 在浏览器中浏览目录、执行只读 SQL | `<WebUrl>/` |
| Admin Console | 在浏览器中管理 Key、流水线和运行环境，查看概览、审计、诊断、保留、配额与成本 | `<WebUrl>/admin.html` |

两个页面调用同一套公开 API。权限只由服务端按 API Key 判断：页面上看得到某个按钮，不代表 Key 有权限；页面隐藏某个按钮，也不能代替服务端的拒绝。

### 1.2 空间与 generation

- **空间（space）** 是数据与权限的隔离单位，由运维开通。每个空间对应一个保存在 S3 上的原生 DuckDB 数据库，包含 `ods`、`dwd`、`dws`、`ads` 四个业务 schema，以及内部的 `ctl` schema。`ctl` 保存账本、水位和发布记录，不能注册为业务数据集，也不出现在查询目录中。
- **generation** 标识空间当前这一代数据库，形如 `gen_` 加 32 位十六进制。运维重建空间（例如破坏性升级时，见[安装部署手册](installation.md#10-破坏性升级)）后，空间切换到新的 generation：旧 generation 的 Key、运行、导出和请求一律被拒绝（`SPACE_GENERATION_CHANGED`），已发布的流水线版本需要重新发布。
- 每个 API Key 只属于一个空间的一个 generation。

### 1.3 数据层

| 层 | schema | 建议内容 | 通常由谁写入 |
| --- | --- | --- | --- |
| ODS | `ods` | 接入的原始业务事件，只追加，保留旧版本和删除事件 | 异步接入 |
| DWD | `dwd` | 类型统一、去重、关联维度后的明细 | 流水线；人工维护的直接表也常放在这一层 |
| DWS | `dws` | 按日期、客户、产品等主题聚合 | 流水线 |
| ADS | `ads` | 面向报表、指标和应用的结果 | 流水线 |

四层是同一个数据库中的四个 schema，不是四个数据库。数据集名写作 `<schema>.<name>`，例如 `ods.orders`、`ads.dashboard`。

### 1.4 数据集类型与写入 owner

每个数据集在注册时声明唯一的写入 owner（`write_owner`）。其他写入路径都不能改写它。

| `write_owner` | 本手册的称呼 | 写入方式 | 位置 |
| --- | --- | --- | --- |
| `ingestion` | 接入数据集 | 异步接入 API 提交 append / upsert / delete 事件 | 只能是 `ods.*` |
| `direct` | 直接表 | 同步写 API 执行 insert / upsert / update / delete，带原生约束 | 任一业务 schema |
| `pipeline` | 流水线目标 | 流水线任务计算后以 release 发布 | `dwd.*`、`dws.*`、`ads.*` |

没有明确声明为 `direct` 的数据集不能同步写入。`ingest:write` 不包含同步写权限，同步写需要 `data:write`。

### 1.5 API Key、principal 与 scope

- 调用者用 API Key 认证。每个 Key 有名称、所属空间、一组 scope 和到期时间。
- **principal** 是 Key 背后的调用者身份。新签发的 Key 有新的 principal；轮换出的替代 Key 沿用原 principal。
- 接入批次、同步写、查询和流水线运行都记录发起它的 principal。默认只有这个 principal 能读取对应的回执、结果或 SQL；管理员需要相应的管理 scope（见各节）。
- **scope** 决定 Key 能调用哪些接口，见 [3.1 节](#31-scope)。

### 1.6 唯一写者、提交序号与水位

- 每个空间的数据库只有一个写者进程。接入批次的提交、同步写、流水线导入和发布都在这个写者上排队，按类别轮流执行（公平调度）。
- 每个成功的写入事务获得空间内单调递增的提交序号（`commit_sequence`）。数据集的水位（watermark）是它最近一次提交的序号。
- 提交序号定义已完成事务的顺序。接入和同步写两个入口之间，不保证按客户端发送的先后提交。
- 一个写者进程最多同时打开 2 个空间的数据库，第三个空间的写入返回 `503 WRITER_DATABASE_LIMIT`，见 [13 节](#13-常见问题与故障排查)。

### 1.7 release 与发布

- 流水线任务每次成功发布，就为目标数据集产生一个不可变的 release（`release_id`）。发布是原子切换：查询要么看到旧 release，要么看到完整的新 release，不会看到写到一半的结果。
- 新 release 计算期间，旧 release 继续可读。旧 release 何时回收由空间的保留策略决定（见 [10.7 节](#107-保留与回收)）。
- 发布以任务（即目标数据集）为单位。一次流水线运行的多个任务分别发布，不构成跨任务的全局事务。需要一致的最终结果时，读 ADS 的 release。

### 1.8 已接收、已提交、已发布

| 状态 | 含义 | 在哪里看到 |
| --- | --- | --- |
| ACCEPTED | 接入批次的原文已存入 S3，队列已确认收到；尚未写入数据库 | 接入 API 返回 `202` |
| COMMITTED | 写入已在数据库事务中持久提交 | 接入状态、同步写回执 |
| PUBLISHED | 流水线任务产生的新 release 已发布 | 流水线运行状态 |

同步写返回 `200 COMMITTED` 时数据已经提交。它不等待也不触发下游流水线，也不产生 release。

### 1.9 UNKNOWN 的含义

`UNKNOWN` 表示服务端暂时无法确认结果，例如回复丢失、写者正在切换或权威记录暂时读不到。它不等于失败，也不等于没有发生：

- 已经开始的写入在 `UNKNOWN` 之后仍可能提交；
- 处理 `UNKNOWN` 的方法是用原来的身份（`ingestion_id`、`write_id`、`query_id`、`run_id` 或原幂等值）核对，而不是换一个新身份重做。

## 2. 开始之前

### 2.1 需要的信息

| 信息 | 来源 |
| --- | --- |
| API 地址 | 运维提供的安装输出 `ApiUrl` |
| Web 地址 | 安装输出 `WebUrl`；Query Editor 是 `<WebUrl>/`，Admin Console 是 `<WebUrl>/admin.html` |
| 空间 ID | 运维开通空间时确定，例如 `sales_prod` |
| API Key | 空间的首个管理员 Key 由运维签发（见[安装部署手册](installation.md#7-开通空间与首个管理员-key)）；其他 Key 由管理员用 API 或 Admin Console 签发 |

本手册的示例使用以下环境变量：

```bash
export COLVENNA_API_URL=<api-url>      # ApiUrl
export COLVENNA_API_KEY=<api-key>      # 具有所需 scope 的 Key
export COLVENNA_SPACE=<space_id>       # 例如 sales_prod
```

### 2.2 认证

所有请求都在请求头中携带 API Key：

```http
Authorization: Bearer <API_KEY>
```

- 每次请求都实时校验 Key：存在、未吊销、未过期、属于空间当前的 generation。吊销立即生效。
- 缺少凭证返回 `401`；Key 无效、已吊销、已过期，或者缺少所需的空间或 scope，返回 `403`。
- 服务端只信任 Key 本身。请求体中的身份、空间或 scope 字段不会扩大权限。不要发送以 `x-colvenna-` 开头的请求头；同步写会以 `400 INTERNAL_IDENTITY_FORBIDDEN` 拒绝这类请求。

### 2.3 请求格式

- JSON 请求体使用 `Content-Type: application/json`。接入、同步写和管理类接口缺少这个头时返回 `415 JSON_REQUIRED`；建议所有带请求体的请求都设置它。
- 请求体最多 1 MiB，超出返回 `413 BODY_TOO_LARGE`。
- 未知字段、重复的 JSON 键、`NaN` / `Infinity` 一律拒绝。
- 标识符（`space_id`、`pipeline_id`、`environment_id`、`client_request_id` 等）以字母或下划线开头，只含字母、数字、`_`、`.`、`-`，最长 128 个字符。列名以字母或下划线开头，只含字母、数字和 `_`。
- 接口中的时间（例如 `expires_at`）使用 ISO 8601，必须带 `Z` 或 `±HH:MM`，例如 `2027-01-15T00:00:00Z`。

### 2.4 值的 JSON 写法

记录、键和参数中的值按注册类型书写：

| 类型 | JSON 写法 | 示例 |
| --- | --- | --- |
| `BIGINT` | 整数；绝对值超过 9007199254740991 时用十进制字符串 | `123`、`"9007199254740993"` |
| `DECIMAL(p,s)` | 字符串，不用指数形式，小数位不超过 `s`；`p` 为 1–38 | `"42.00"` |
| `BOOLEAN` | `true` / `false` | `true` |
| `VARCHAR` | 字符串 | `"first account"` |
| `VARCHAR[]` | 字符串数组 | `["a", "b"]` |
| `DATE` | `YYYY-MM-DD` | `"2026-10-14"` |
| `TIMESTAMP` | `YYYY-MM-DDTHH:MM:SS`，可带最多 6 位小数，**不带**时区 | `"2026-10-14T08:00:00"` |
| `TIMESTAMPTZ` | 同上，**必须**带 `Z` 或 `±HH:MM` | `"2026-10-14T08:00:00Z"` |
| `JSON` | 任意有限的 JSON 值 | `{"source": "pos"}` |
| `BLOB` | 规范的 base64 字符串 | `"AAEC"` |

注意：`TIMESTAMP` 列的值带 `Z` 会被拒绝；需要带时区的时间请注册为 `TIMESTAMPTZ`。

查询结果中，`DECIMAL` 以字符串返回；interval、纳秒时间戳等无法无损转换为 JSON 的值以文本返回，列的 `encoding` 为 `native_text`。

### 2.5 错误响应

错误响应的格式如下：

```json
{
  "code": "IDEMPOTENCY_KEY_REQUIRED",
  "message": "Retry the same Idempotency-Key",
  "request_id": "<request-id>"
}
```

- `code` 是稳定的错误码，程序应按它分支处理；`message` 是固定说明，不回显请求中的值。
- 需要向运维反馈问题时，提供 `request_id`。
- 结果不确定时，响应还带 `status: "UNKNOWN"`、原身份（`ingestion_id` 或 `write_id`）和 `status_url`；背压拒绝带 `retry_after_ms`。

### 2.6 幂等：Idempotency-Key 与 client_request_id

会产生效果的提交接口都要求一个调用方生成的幂等值。同一个幂等值代表同一个业务操作：

| 接口 | 幂等值 | 同一幂等值、相同内容 | 同一幂等值、不同内容 |
| --- | --- | --- | --- |
| `POST .../ingestions` | `Idempotency-Key` 头 | 返回原 `ingestion_id` | `409 IDEMPOTENCY_CONFLICT` |
| `POST .../writes` | `Idempotency-Key` 头 | 返回原 `write_id` 和原结果 | `409 WRITE_BINDING_CONFLICT` |
| `POST .../queries` | 请求体 `client_request_id` | 返回原 `query_id` 和它当前的状态 | `409 QUERY_BINDING_CONFLICT` |
| `POST .../pipelines/{pipeline_id}/versions` | `Idempotency-Key` 头 | 返回原版本 | `409 IDEMPOTENCY_KEY_CONFLICT` |
| `POST .../runs`、`POST .../trial-runs` | `Idempotency-Key` 头 | 返回原 `run_id` | `409 IDEMPOTENCY_KEY_CONFLICT` |
| `POST .../runs/{run_id}/retry` | `Idempotency-Key` 头 | 返回同一次重试 | — |
| `POST .../runtime-environments/{environment_id}/versions/{version}/build` | `Idempotency-Key` 头 | 返回原 `build_id` | 该版本已用其他值构建时 `409 ENVIRONMENT_BUILD_ALREADY_ADMITTED` |
| `POST /v1/admin/spaces/{space_id}/retention/collections` | `Idempotency-Key` 头 | 返回原回收 | `409 IDEMPOTENCY_KEY_CONFLICT` |

- `Idempotency-Key` 为 1–128 个字符，只含字母、数字、`_`、`.`、`:`、`-`。缺少或格式不对时返回 `428 IDEMPOTENCY_KEY_REQUIRED`。
- 每个业务操作生成一个新的幂等值，在发送前保存下来；重试时原样复用；内容改变时必须换新值。
- 接入和同步写的身份由空间、generation、数据集和幂等值决定，不含 principal：另一个 principal 用同一个幂等值写同一个数据集会被拒绝（`403`）。查询、版本发布和运行的身份包含 principal，不同 principal 互不影响。
- 用新 Key 轮换后，principal 不变，原幂等身份仍归同一调用者所有。

### 2.7 条件更新：If-Match

修改共享配置的接口要求 `If-Match` 携带一个带引号的具体值，不支持 `*`：

| 接口 | `If-Match` 值 | 从哪里取 | 缺少时 | 已过期时 |
| --- | --- | --- | --- | --- |
| `POST /v1/admin/spaces/{space_id}/datasets` | `"<generation>"` | `GET .../configuration` 的 `generation` | `428 SPACE_GENERATION_REQUIRED` | `409 SPACE_REBUILT` |
| `PUT .../pipelines/{pipeline_id}/draft`、`POST .../activate`、`PUT .../schedule` | `"<revision>"`（形如 `"rev_…"`） | `GET .../pipelines/{pipeline_id}` 的 `revision` | `428 PIPELINE_REVISION_REQUIRED` | `412 PIPELINE_REVISION_CHANGED` |
| `PUT /v1/admin/spaces/{space_id}/retention` | `"<revision>"`（形如 `"ret_…"`） | `GET .../retention` 的 `revision` | `412 RETENTION_REVISION_CHANGED` | `412 RETENTION_REVISION_CHANGED` |

收到 `412` 时，重新读取最新的 `revision`，确认改动仍然需要后再提交。

### 2.8 429、503 与 504 的处理原则

| 响应 | 含义 | 处理 |
| --- | --- | --- |
| `429` | 确定没有执行：网关限流（默认每条路由 10 rps、突发 20）、接入背压、同步写 `NOT_SUBMITTED`、查询队列已满或空间配额已满 | 按 `retry_after_ms` 或 `Retry-After` 等待，再用**同一个**幂等值重试。配额类错误（`SPACE_*_QUOTA`）需要先释放配额或请运维调整 |
| `503` / `504` 且 `status: "UNKNOWN"` | 结果不确定，可能已经生效 | 先用 `status_url` 核对；也可以用同一个幂等值和相同内容重试，服务端返回原身份的实际结果。不要换新幂等值重发 |
| 其他 `503`（如 `CONTROL_UNAVAILABLE`） | 服务暂时不可用 | 稍后原样重试同一个请求 |
| 其他 `4xx` | 请求本身不被接受 | 按错误码修改请求；内容改变时使用新的幂等值 |

状态接口返回 `UNKNOWN` 时，表示还没有取得权威观测。稍后再查，不要据此判定失败。

## 3. API Key

### 3.1 scope

| scope | 授予的访问 |
| --- | --- |
| `datasets:manage` | 读取空间配置、注册数据集；读取空间内任何同步写回执；对他人的同步写执行 `after_write` 查询 |
| `ingest:write` | 提交接入批次，读取自己的接入状态 |
| `ingest:replay` | 与 `ingest:write` 一起，对自己的原接入批次请求重放 |
| `data:write` | 同步写直接表，核对自己的写入回执 |
| `query:read` | 读取授权目录、提交查询、读取自己的查询结果 |
| `query:manage` | 与 `query:read` 一起，查看同空间他人查询的元数据并取消；不授予他人的 SQL 或结果 |
| `pipeline:run` | 运行流水线、试运行、取消和重试运行 |
| `pipeline:manage` | 创建和编辑流水线定义、发布与激活版本、管理调度 |
| `runtime:manage` | 创建和构建运行环境 |
| `monitor:read` | 查看空间状态、指标、诊断、配额与成本；查看他人的接入状态和运行 |
| `keys:manage` | 在委派上限内管理本空间的 Key |
| `audit:read` | 读取审计记录；在诊断中查看原始 SQL |
| `retention:manage` | 修改保留策略、设置和解除保留锁定、请求回收 |
| `backup:manage` | 授权运维备份工具读取写者的原生快照（见[安装部署手册](installation.md#11-备份与隔离还原)） |

### 3.2 常见角色

按最小权限给每类调用者单独签发 Key。[quickstart](../examples/quickstart/README.md) 使用以下组合：

| 角色 | scope |
| --- | --- |
| 管理员 | 首个管理员 Key 拥有全部 scope，用于签发其他 Key |
| 数据开发 | `datasets:manage`、`pipeline:manage`、`pipeline:run`、`runtime:manage`、`monitor:read` |
| 接入应用 | `ingest:write` |
| 同步写应用 | `data:write`、`query:read` |
| 分析人员 | `query:read` |
| 外部编排（Airflow） | `pipeline:run` |

### 3.3 查看 Key

需要 `keys:manage`：

```bash
curl -s "$COLVENNA_API_URL/v1/admin/spaces/$COLVENNA_SPACE/api-keys" \
  -H "Authorization: Bearer $COLVENNA_API_KEY"
```

返回 `{"keys": [...]}`，每项包含：

| 字段 | 说明 |
| --- | --- |
| `key_id` | Key 的标识，形如 `key_…` |
| `name` | 签发时的名称 |
| `fingerprint` | `key_id` 的末 12 个字符，用于在日志和审计中识别 |
| `scopes` | scope 列表 |
| `created_at`、`expires_at` | 创建与到期时间 |
| `status` | `ACTIVE`、`REVOKED` 或 `EXPIRED` |
| `last_used_at` | 为 `null` 时表示尚未观测到使用记录 |
| `replaces_key_id`、`replacement_key_id` | 轮换关系 |
| `creation_request_id` | 签发请求的网关请求 ID |

列表从不返回完整 token 或其摘要。每个空间最多列出 1,000 个 Key。

### 3.4 签发 Key

需要 `keys:manage`：

```bash
curl -s -X POST "$COLVENNA_API_URL/v1/admin/spaces/$COLVENNA_SPACE/api-keys" \
  -H "Authorization: Bearer $COLVENNA_API_KEY" -H "Content-Type: application/json" \
  -d '{"name": "orders-ingestion", "scopes": ["ingest:write"], "expires_at": "2027-01-15T00:00:00Z"}'
```

返回 `201`：

```json
{
  "key": {
    "key_id": "key_<32 位十六进制>",
    "name": "orders-ingestion",
    "fingerprint": "<key_id 末 12 位>",
    "scopes": ["ingest:write"],
    "created_at": "2026-10-15T08:00:00Z",
    "expires_at": "2027-01-15T00:00:00Z",
    "status": "ACTIVE",
    "last_used_at": null,
    "creation_request_id": "<request-id>"
  },
  "token": "<one-time-api-key>"
}
```

- `token` 只在这一次响应中出现，之后无法再取回。请立即交给使用者，并按密码保管。
- 委派上限：请求的 scope 必须是签发者自己 scope 的子集，`expires_at` 必须晚于当前时间且不晚于签发者自己的到期时间，否则返回 `403 DELEGATION_FORBIDDEN`。
- 签发响应丢失时，不要直接再签发一次。先按名称和创建时间在列表中核对；拿不到 token 的 Key 应吊销，再重新签发。

### 3.5 轮换 Key

轮换为一个 Key 签发唯一的替代 Key，替代 Key 沿用原 principal。原 principal 的接入、写入、查询和运行仍归替代 Key 所有。需要 `keys:manage`：

```bash
curl -s -X POST "$COLVENNA_API_URL/v1/admin/spaces/$COLVENNA_SPACE/api-keys/<key_id>/rotate" \
  -H "Authorization: Bearer $COLVENNA_API_KEY" -H "Content-Type: application/json" \
  -d '{"name": "orders-ingestion-2027", "scopes": ["ingest:write"], "expires_at": "2027-06-30T00:00:00Z"}'
```

- 返回 `201`，格式与签发相同，`key.replaces_key_id` 指向原 Key。
- 新 scope 不能超出原 Key 的 scope，也不能超出调用者自己的委派上限。
- 每个 Key 只能轮换一次；再次轮换返回 `409 ROTATION_ALREADY_ISSUED`。
- 轮换不会吊销原 Key。按以下顺序切换：轮换 → 使用方换成新 token 并确认可用 → 吊销原 Key。

### 3.6 吊销 Key

```bash
curl -s -X POST "$COLVENNA_API_URL/v1/admin/spaces/$COLVENNA_SPACE/api-keys/<key_id>/revoke" \
  -H "Authorization: Bearer $COLVENNA_API_KEY" -H "Content-Type: application/json" -d '{}'
```

- 返回 `200` 和状态为 `REVOKED` 的 Key 元数据。吊销立即生效，Key 的审计身份保留。重复吊销返回同样的结果。
- 只能吊销或轮换 scope 不超出自己的 Key；管理范围较小的 Key 不能吊销权限更大的 Key（`403 FORBIDDEN`）。

### 3.7 到期

Key 到期后状态显示为 `EXPIRED`，所有请求被拒绝。到期时间不能延长。请在到期前轮换，或签发新 Key 后切换。

## 4. 数据集注册

写入和流水线只能使用已注册的数据集。注册需要 `datasets:manage`。

### 4.1 查看空间配置

```bash
curl -s "$COLVENNA_API_URL/v1/admin/spaces/$COLVENNA_SPACE/configuration" \
  -H "Authorization: Bearer $COLVENNA_API_KEY"
```

返回空间的当前配置，主要字段：

| 字段 | 说明 |
| --- | --- |
| `space_id`、`generation` | 空间与当前 generation；注册时把 `generation` 放进 `If-Match` |
| `revision`、`route_version` | 配置修订号与路由版本 |
| `schemas` | 固定为 `["ods", "dwd", "dws", "ads", "ctl"]` |
| `datasets` | 以数据集名为键的已注册定义，每项含 `definition`、`definition_sha256` 和 `registered_revision` |
| `ingestion_policy` | 空间的接入背压阈值（可选，见 [5.6 节](#56-背压)） |

### 4.2 注册字段

`POST /v1/admin/spaces/{space_id}/datasets` 的请求体：

| 字段 | 必填 | 说明 |
| --- | --- | --- |
| `dataset` | 是 | `<schema>.<name>`，schema 为 `ods`、`dwd`、`dws` 或 `ads`，全部小写 |
| `schema_version` | 是 | 正整数；写入请求必须带同一个版本 |
| `schema` | 是 | 1–256 列，每列 `{"name", "type", "nullable"}`，类型见 [2.4 节](#24-值的-json-写法) |
| `key_columns` | 是 | 业务键列，可以为空数组；必须是非空列 |
| `partition_columns` | 是 | 分区列，可以为空数组；必须是非空列 |
| `write_owner` | 建议填写 | `ingestion`、`direct` 或 `pipeline`，见 [1.4 节](#14-数据集类型与写入-owner) |
| `ingestion` | 接入数据集必填 | `operations`、`event_id_column`、`source_version_column` |
| `constraints` | 仅直接表 | `primary_key`、`unique`、`foreign_keys`、`checks` |
| `indexes` | 仅直接表 | 索引列组的数组 |
| `publication_state` | 仅增量流水线目标 | 见 [8.7 节](#87-处理模式) |

注册成功返回 `201` 和 `{"definition", "definition_sha256", "registered_revision"}`。

### 4.3 接入数据集

[quickstart](../examples/quickstart/README.md) 注册的订单事件表：

```bash
GENERATION=$(curl -s "$COLVENNA_API_URL/v1/admin/spaces/$COLVENNA_SPACE/configuration" \
  -H "Authorization: Bearer $COLVENNA_API_KEY" | python3 -c 'import json,sys; print(json.load(sys.stdin)["generation"])')

curl -s -X POST "$COLVENNA_API_URL/v1/admin/spaces/$COLVENNA_SPACE/datasets" \
  -H "Authorization: Bearer $COLVENNA_API_KEY" -H "Content-Type: application/json" \
  -H "If-Match: \"$GENERATION\"" -d @- <<'JSON'
{
  "dataset": "ods.orders",
  "schema_version": 1,
  "write_owner": "ingestion",
  "schema": [
    {"name": "event_id", "type": "VARCHAR", "nullable": false},
    {"name": "order_id", "type": "BIGINT", "nullable": false},
    {"name": "source_version", "type": "BIGINT", "nullable": false},
    {"name": "event_time", "type": "TIMESTAMP", "nullable": false},
    {"name": "amount", "type": "DECIMAL(18,2)", "nullable": false}
  ],
  "key_columns": ["order_id"],
  "partition_columns": [],
  "ingestion": {
    "operations": ["upsert", "delete"],
    "event_id_column": "event_id",
    "source_version_column": "source_version"
  }
}
JSON
```

规则：

- 只有 `ods.*` 可以声明 `ingestion`，`write_owner` 必须是 `ingestion` 或省略。
- `operations` 是 `append`、`upsert`、`delete` 的非空子集；接入请求只能使用其中列出的操作。
- `event_id_column` 必须是非空 `VARCHAR`，用于事件去重。
- 允许 `upsert` 或 `delete` 时，必须有 `key_columns`，并且 `source_version_column` 必须是非空 `BIGINT`。业务版本由这一列决定，与到达顺序无关。
- 只允许 `append` 时可以省略 `source_version_column`；这时只按事件 ID 去重，不比较版本。

### 4.4 直接表

[quickstart](../examples/quickstart/README.md) 中的直接表：

```json
{
  "dataset": "dwd.accounts",
  "schema_version": 1,
  "write_owner": "direct",
  "schema": [
    {"name": "id", "type": "BIGINT", "nullable": false},
    {"name": "name", "type": "VARCHAR", "nullable": false},
    {"name": "balance", "type": "DECIMAL(18,2)", "nullable": false}
  ],
  "key_columns": ["id"],
  "partition_columns": [],
  "constraints": {"primary_key": ["id"]}
}
```

直接表可以声明 DuckDB 原生约束和索引，写入时由数据库实际执行：

```json
{
  "dataset": "dwd.customers",
  "schema_version": 1,
  "write_owner": "direct",
  "schema": [
    {"name": "id", "type": "BIGINT", "nullable": false},
    {"name": "email", "type": "VARCHAR", "nullable": false},
    {"name": "tier", "type": "VARCHAR", "nullable": false},
    {"name": "credit_limit", "type": "DECIMAL(18,2)", "nullable": false}
  ],
  "key_columns": ["id"],
  "partition_columns": [],
  "constraints": {
    "primary_key": ["id"],
    "unique": [["email"]],
    "checks": [{"column": "credit_limit", "operator": ">=", "value": "0"}]
  },
  "indexes": [["tier"]]
}
```

外键只能引用先注册好的直接表：

```json
{
  "dataset": "dwd.customer_accounts",
  "schema_version": 1,
  "write_owner": "direct",
  "schema": [
    {"name": "account_id", "type": "BIGINT", "nullable": false},
    {"name": "customer_id", "type": "BIGINT", "nullable": false}
  ],
  "key_columns": ["account_id"],
  "partition_columns": [],
  "constraints": {
    "primary_key": ["account_id"],
    "foreign_keys": [{"columns": ["customer_id"], "references": "dwd.customers", "referenced_columns": ["id"]}]
  }
}
```

规则：

- `constraints` 和 `indexes` 只能用于 `write_owner: direct`。
- `upsert`、`update`、`delete` 依赖 `key_columns`。直接表有 `key_columns` 时，它必须正好等于声明的 `primary_key` 或某个 `unique` 列组。只有 `insert` 的表可以没有键。
- 主键、唯一键和索引的列不能是 `JSON`、`BLOB` 或 `VARCHAR[]`；主键列必须非空。
- 外键引用的数据集必须已注册为直接表（否则 `409 FOREIGN_KEY_TARGET_UNREGISTERED`），`referenced_columns` 必须是它的主键或唯一键，对应列类型必须相同（否则 `409 FOREIGN_KEY_TARGET_MISMATCH`）。不能引用自身。
- `checks` 每项为 `{"column", "operator", "value"}`，`operator` 为 `=`、`<>`、`<`、`<=`、`>`、`>=`，`value` 按列类型书写；列不能是 `JSON`、`BLOB` 或 `VARCHAR[]`。

### 4.5 流水线目标

流水线任务的输出数据集注册为 `write_owner: pipeline`，并且 `schema`、`key_columns`、`partition_columns` 必须与任务定义中的 `output` **完全一致**（列的顺序、类型和 `nullable` 都要相同），否则发布时返回 `422 PIPELINE_TARGET_SCHEMA_OR_OWNER_MISMATCH`。例如 orders 示例的 `ads.dashboard`：

```json
{
  "dataset": "ads.dashboard",
  "schema_version": 1,
  "write_owner": "pipeline",
  "schema": [
    {"name": "order_date", "type": "DATE", "nullable": false},
    {"name": "revenue", "type": "DECIMAL(38,2)", "nullable": false},
    {"name": "order_count", "type": "BIGINT", "nullable": false},
    {"name": "high_value_count", "type": "BIGINT", "nullable": false}
  ],
  "key_columns": ["order_date"],
  "partition_columns": []
}
```

增量任务（`merge_keys`）的目标还要声明 `publication_state`，见 [8.7 节](#87-处理模式)。

### 4.6 命名与上限

- 数据集名全部小写。列名在 DuckDB 中不区分大小写，同一数据集内不能只靠大小写区分。
- 列名不能以 `_colvenna_` 开头；这个前缀保留给系统列。
- 每个空间最多 64 个数据集（`413 DATASET_LIMIT`），整个空间配置最多 350,000 字节（`413 CONFIGURATION_TOO_LARGE`）。

### 4.7 版本与变更规则

- 注册的定义不可修改。用完全相同的定义重试，返回原来的注册结果（包括响应丢失后的重试）。
- 同名数据集用不同定义注册，返回 `409 DATASET_CONFLICT`。没有原地修改 schema 的接口。
- 需要改变 schema 时，由运维重建空间（新 generation，旧数据不迁移，见[安装部署手册](installation.md#10-破坏性升级)），再按新定义注册。

## 5. 异步接入

异步接入面向批量事件：原文先归档到 S3、进入 FIFO 队列，再由写者在一个事务中写入 ODS。返回 `202` 只表示已接收，不表示已提交。

### 5.1 提交批次

需要 `ingest:write` 和 `Idempotency-Key`：

```bash
curl -s -X POST "$COLVENNA_API_URL/v1/spaces/$COLVENNA_SPACE/ingestions" \
  -H "Authorization: Bearer $COLVENNA_API_KEY" -H "Content-Type: application/json" \
  -H "Idempotency-Key: orders-2026-10-14-batch-001" -d @- <<'JSON'
{
  "dataset": "orders",
  "schema_version": 1,
  "operation": "upsert",
  "records": [
    {"event_id": "order-1-v1", "order_id": 1, "source_version": 1, "event_time": "2026-10-14T00:00:00", "amount": "10.00"},
    {"event_id": "order-1-v2", "order_id": 1, "source_version": 2, "event_time": "2026-10-14T00:00:00", "amount": "12.34"}
  ]
}
JSON
```

| 字段 | 说明 |
| --- | --- |
| `dataset` | ODS 数据集名**不带** `ods.` 前缀，例如 `ods.orders` 写作 `orders` |
| `schema_version` | 必须等于注册版本，否则 `409 SCHEMA_VERSION_MISMATCH` |
| `operation` | `append`、`upsert` 或 `delete`，必须在注册的 `operations` 中，否则 `403 INGESTION_OPERATION_FORBIDDEN` |
| `records` | 1–10,000 条记录，整个请求体不超过 1 MiB |

返回 `202`：

```json
{
  "ingestion_id": "ing_<64 位十六进制>",
  "status": "ACCEPTED",
  "status_url": "/v1/spaces/sales_prod/ingestions/ing_<64 位十六进制>"
}
```

`ACCEPTED` 表示原文已存入 S3、队列已确认收到。此时数据尚未提交，也不能被查询到。

### 5.2 记录格式

- 一个请求只包含一种操作。
- `append` / `upsert`：每条记录必须包含注册的**全部**列；可空列的值可以是 `null`。
- `delete`：必须包含业务键、事件 ID 列和源版本列；其他列可以省略或为 `null`。
- 未注册的列、类型不符、必填值缺失时，整个请求以 `400 INVALID_REQUEST` 拒绝，不会归档。

### 5.3 幂等与身份

- `ingestion_id` 由空间、generation、数据集和 `Idempotency-Key` 确定，与请求内容无关。
- 同一个 `Idempotency-Key` 加相同内容重试（JSON 空白不同也算相同内容），返回同一个 `ingestion_id`，原文保持第一次的版本。
- 同一个 `Idempotency-Key` 加不同内容，返回 `409 IDEMPOTENCY_CONFLICT`；另一个 principal 使用同一个 `Idempotency-Key`，返回 `403 INGESTION_OWNER_MISMATCH`。

### 5.4 ODS 事件语义

ODS 是只追加的事件历史，不覆盖、不物理删除旧事件：

- **重复事件**：同一 `event_id`、相同内容和操作的事件不会重复写入。
- **事件冲突**：同一 `event_id` 但内容或操作不同，整个批次被拒绝（`EVENT_IDENTITY_CONFLICT`）。
- **版本冲突**：同一业务键、同一 `source_version` 出现不同的事件，整个批次被拒绝（`BUSINESS_VERSION_CONFLICT`）。
- **迟到的旧版本**：比当前版本低的事件仍然保留，但标记为过期，不会改变该键的当前版本或删除状态。
- **删除**：更高版本的 `delete` 记录一个删除标记（tombstone）；之后到达的更低版本不能使记录复活，更高版本的 `upsert` 可以。
- **重复批次**：同一个批次重复投递时，返回它原来的提交结果和提交序号，不会再次写入。

每个新的有效批次获得一个提交序号。

提示：直接查询 ODS 表时，只返回注册的业务列。删除事件显示为非键列为 `NULL` 的行，结果中不包含操作类型。要得到“每个键的当前状态”，请用流水线（流水线的 ODS 输入额外带 `_colvenna_operation` 和 `_colvenna_commit_sequence` 两列，见 [8.5 节](#85-输入输出与-sql-任务)）。

### 5.5 查询接入状态

批次的原 principal（持有 `ingest:write`），或持有 `monitor:read` 的 Key，可以读取状态：

```bash
curl -s "$COLVENNA_API_URL/v1/spaces/$COLVENNA_SPACE/ingestions/<ingestion_id>" \
  -H "Authorization: Bearer $COLVENNA_API_KEY"
```

```json
{
  "ingestion_id": "ing_<64 位十六进制>",
  "space_id": "sales_prod",
  "dataset": "ods.orders",
  "status": "COMMITTED",
  "commit_sequence": 42,
  "committed_at": "2026-10-15T08:00:03Z",
  "projection_version": 7,
  "observed_at": "2026-10-15T08:00:03Z",
  "authority": "NATIVE_LEDGER",
  "projection_state": "CURRENT",
  "reconciling": false,
  "publication": {"status": "NOT_PUBLISHED"},
  "layers": {
    "ods": {"status": "COMMITTED", "watermark": "ing_<64 位十六进制>"},
    "dwd": {"status": "NOT_IMPLEMENTED"},
    "dws": {"status": "NOT_IMPLEMENTED"},
    "ads": {"status": "NOT_IMPLEMENTED"}
  }
}
```

| `status` | 含义 | 下一步 |
| --- | --- | --- |
| `UNKNOWN` | 还没有权威观测：写者尚未处理该批次，或暂时读不到权威记录（此时 `reconciling: true`） | 稍后再查；不要据此判定失败，也不要换新 `Idempotency-Key` 重发 |
| `COMMITTED` | 已在数据库中提交，`commit_sequence` 为提交序号 | 数据已可查询（见 [7.7 节](#77-一致性模式)） |
| `FAILED` | 写者确定地拒绝了该批次，`error_code` 给出原因 | 修正数据后用**新的** `Idempotency-Key` 提交新批次 |

- `authority` 说明结果来源：`NATIVE_LEDGER`（数据库账本，权威）、`PROJECTION`（状态投影，可能滞后）或 `UNAVAILABLE`。
- `observed_at` 和 `projection_version` 是该观测的时间和版本；状态投影可能滞后于实际提交。
- 1.1.0 中这个接口只报告 ODS 的提交：`publication.status` 始终为 `NOT_PUBLISHED`，`dwd`、`dws`、`ads` 三层为 `NOT_IMPLEMENTED`。下游加工进度请看流水线运行状态（[8.11 节](#811-运行)）或 Admin Console 的诊断页。

### 5.6 背压

接入在归档之前检查队列积压和写者处理能力。超出阈值时直接拒绝，不产生归档或队列消息：

| 错误码 | HTTP | 原因 |
| --- | --- | --- |
| `SPACE_BATCH_QUOTA` | 429 | 批次记录数或字节数超过空间阈值 |
| `QUEUE_BACKLOG` | 429 | 队列中待处理的消息过多 |
| `QUEUE_AGE` | 429 | 最老的消息等待过久 |
| `WRITER_DRAIN_BUDGET` | 429 | 按写者实测处理能力，排空队列所需时间超过阈值 |
| `WRITER_CAPACITY_UNAVAILABLE` | 429 | 写者处理能力未知，且积压已达到缓冲上限 |
| `QUEUE_AGE_UNKNOWN` | 503 | 队列非空但取不到最老消息的年龄 |

`429` 响应带 `retry_after_ms: 5000` 和 `Retry-After: 5`。等待后用同一个 `Idempotency-Key` 和相同内容重试。

阈值来自空间配置的 `ingestion_policy`，未设置时使用缺省值：

| 字段 | 缺省值 |
| --- | --- |
| `max_pending_messages` | 1,000 条消息 |
| `max_oldest_seconds` | 3,600 秒 |
| `max_drain_seconds` | 1,800 秒 |
| `unavailable_writer_buffer` | 20 条消息 |
| `max_batch_records` | 10,000 条记录 |
| `max_batch_bytes` | 1 MiB |

整个环境共用一个 FIFO 队列。每个空间的阈值按整个队列的积压保守计算，因此其他空间的积压也会触发背压。

### 5.7 结果不确定

归档或发送队列的结果无法确认时，返回 `503`：

```json
{
  "code": "QUEUE_SEND_UNCONFIRMED",
  "message": "Reconcile or retry the same Idempotency-Key",
  "request_id": "<request-id>",
  "ingestion_id": "ing_<64 位十六进制>",
  "status": "UNKNOWN",
  "status_url": "/v1/spaces/sales_prod/ingestions/ing_<64 位十六进制>"
}
```

错误码可能是 `ARCHIVE_UNCONFIRMED`、`ARCHIVE_IDENTITY_UNAVAILABLE`、`QUEUE_SEND_UNCONFIRMED` 或 `CONTROL_READ_UNCONFIRMED`。处理方法：用同一个 `Idempotency-Key` 和相同内容重试，直到得到 `202`，或者查询 `status_url`。原文与 `ingestion_id` 保持不变，数据库账本保证同一批次只提交一次。

### 5.8 失败、诊断与重放

- 写者确定拒绝的批次，状态为 `FAILED` 并带 `error_code`。同一条消息最多接收 5 次，之后进入死信队列（DLQ）。接入队列保留消息 7 天，DLQ 保留 14 天；原文至少保留 45 天。
- 持有 `monitor:read` 的 Key 可以读取诊断：

  ```bash
  curl -s "$COLVENNA_API_URL/v1/spaces/$COLVENNA_SPACE/ingestions/<ingestion_id>/diagnostics" \
    -H "Authorization: Bearer $COLVENNA_API_KEY"
  ```

  返回原生状态 `native_status`（`COMMITTED`、`FAILED` 或 `UNKNOWN`）、最多 32 次处理尝试（`attempts`，含 `error_code` 和观测时间）、DLQ 核对记录、队列积压与写者处理能力的观测（`queue`）、保留期限（`retention`）和剩余重放窗口（`replay_window_seconds`）。诊断不返回记录内容。

- 原批次因暂时性故障没有提交时，可以请求重放。重放重新发送**原来归档的内容**，不能修改数据。需要批次的原 principal，同时持有 `ingest:write` 和 `ingest:replay`：

  ```bash
  curl -s -X POST "$COLVENNA_API_URL/v1/spaces/$COLVENNA_SPACE/ingestions/<ingestion_id>/replay" \
    -H "Authorization: Bearer $COLVENNA_API_KEY" -H "Content-Type: application/json" -d '{}'
  ```

  | 响应 | 含义 |
  | --- | --- |
  | `200`，`status: COMMITTED` | 原批次已经提交，未重新发送 |
  | `202`，`status: ACCEPTED` | 已重新发送，带审计 ID `audit_id`；之后查询状态 |
  | `409 REPLAY_WINDOW_EXPIRED` | 超过原文创建后 30 天的重放窗口 |
  | `429` | 背压，见 [5.6 节](#56-背压) |

- 内容本身有问题（如 `EVENT_IDENTITY_CONFLICT`）的批次，重放也会失败。请修正后用新的 `Idempotency-Key` 提交。
- 死信队列的清理由运维处理。

## 6. 同步写

同步写直接修改注册为 `direct` 的业务表，在原生 DuckDB 事务持久提交后才返回。设计说明见[同步写设计](design/synchronous-writes.md)。

### 6.1 前提

- 目标数据集已注册为 `write_owner: direct`（否则 `403 DIRECT_WRITE_TARGET_REQUIRED`）。
- Key 持有 `data:write`。
- 每个请求带 `Idempotency-Key`。

### 6.2 请求

`POST /v1/spaces/{space_id}/writes`：

| 字段 | 说明 |
| --- | --- |
| `dataset` | 完整数据集名，例如 `dwd.accounts` |
| `schema_version` | 必须等于注册版本，否则 `409 SCHEMA_VERSION_MISMATCH` |
| `operation` | `insert`、`upsert`、`update` 或 `delete` |
| `records` / `updates` / `keys` | 按操作选择其中一种，见下表；最多 1,000 项 |
| `timeout_ms` | 等待预算，1–20,000 毫秒，缺省 10,000 |

| 操作 | 数据字段 | 行为 |
| --- | --- | --- |
| `insert` | `records`：完整的行（包含全部列） | 插入；违反主键或唯一约束时整批拒绝 |
| `upsert` | `records`：完整的行 | 按 `key_columns` 插入，或替换已有行的非键列 |
| `update` | `updates`：每项 `{"key": {...完整键}, "set": {...非键列}}`，`set` 不能为空 | 只更新 `set` 中的列，不修改键 |
| `delete` | `keys`：每项是完整的键 | 删除匹配的行 |

- `upsert`、`update`、`delete` 要求数据集有 `key_columns`；同一请求内同一个键只能出现一次。
- `timeout_ms` 覆盖鉴权、排队和等待提交的整个过程。它是等待上限，不是延迟承诺。
- 不支持任意 SQL、DDL、跨请求事务或跨表事务。

### 6.3 示例

插入一行：

```bash
curl -s -X POST "$COLVENNA_API_URL/v1/spaces/$COLVENNA_SPACE/writes" \
  -H "Authorization: Bearer $COLVENNA_API_KEY" -H "Content-Type: application/json" \
  -H "Idempotency-Key: account-1-create" \
  -d '{"dataset": "dwd.accounts", "schema_version": 1, "operation": "insert",
       "records": [{"id": 1, "name": "first account", "balance": "100.00"}]}'
```

其他三种操作的请求体：

```json
{"dataset": "dwd.accounts", "schema_version": 1, "operation": "upsert",
 "records": [{"id": 1, "name": "first account", "balance": "120.00"},
             {"id": 2, "name": "second account", "balance": "0.00"}]}
```

```json
{"dataset": "dwd.accounts", "schema_version": 1, "operation": "update",
 "updates": [{"key": {"id": 1}, "set": {"balance": "42.00"}}],
 "timeout_ms": 10000}
```

```json
{"dataset": "dwd.accounts", "schema_version": 1, "operation": "delete",
 "keys": [{"id": 2}]}
```

### 6.4 成功回执

返回 `200`：

```json
{
  "write_id": "write_<64 位十六进制>",
  "space_id": "sales_prod",
  "generation": "gen_<32 位十六进制>",
  "dataset": "dwd.accounts",
  "schema_version": 1,
  "status": "COMMITTED",
  "commit_sequence": 57,
  "affected_rows": 1,
  "committed_at": "2026-10-15T08:10:00Z",
  "dataset_watermark": 57,
  "retry_until": "2026-11-14T08:10:00Z"
}
```

- `COMMITTED` 表示业务数据、写入回执、变更记录和数据集水位已在同一个原生事务中持久提交。
- `affected_rows`：insert / upsert 为写入的行数；update 为匹配到的行数（值没有变化的行也计入）；delete 为实际删除的行数。
- 没有匹配到任何行的 update 或 delete 也会成功，`affected_rows` 为 0，同样获得提交序号。
- `retry_until` 是这个写入身份的重试窗口（30 天）。

### 6.5 拒绝与约束错误

批次是原子的：任意一项失败，整批都不生效。

| 响应 | 含义 | 处理 |
| --- | --- | --- |
| `422`，`status: REJECTED`，`code: NATIVE_CONSTRAINT` | 违反原生约束（主键、唯一、外键、非空或 CHECK），事务已整体回滚。拒绝结果被持久记录，带 `rejected_at`，没有提交序号 | 同一个 `Idempotency-Key` 重试会得到同一个拒绝。修正数据后用新的 `Idempotency-Key` 提交 |
| `422 INVALID_WRITE_REQUEST` | 请求不合法：未知字段、类型不符、缺少列、空批次、同一请求内键重复、键不完整等 | 修正请求；没有产生写入身份 |
| `409 SCHEMA_VERSION_MISMATCH` | `schema_version` 不是当前注册版本 | 使用当前版本 |
| `404 DATASET_NOT_FOUND` | 数据集未注册 | 先注册 |
| `403 DIRECT_WRITE_TARGET_REQUIRED` | 目标不是直接表 | 只能同步写 `direct` 数据集 |

DuckDB 1.5.6 的一个限制：如果某一行被其他表的外键引用，那么改变它的索引列（显式索引、主键、唯一键）或 `VARCHAR[]` 列的值，会以 `NATIVE_CONSTRAINT` 整批拒绝。只要这些列的值不变，被引用的行仍可以正常 update / upsert 其他列；值完全相同的 upsert 也计为一行受影响。

### 6.6 超时与 UNKNOWN

| 响应 | `status` | 含义 | 处理 |
| --- | --- | --- | --- |
| `200` | `COMMITTED` | 已提交 | 完成 |
| `422` | `REJECTED` | 确定被拒绝，未产生任何变更 | 见 6.5 节 |
| `429` | `NOT_SUBMITTED` | 执行前明确拒绝（`WRITER_QUEUE_FULL`：写者队列已满；`WAIT_BUDGET_BEFORE_EXECUTION`：开始执行前等待预算已用完），带 `retry_after_ms` 和 `Retry-After: 1` | 等待后用同一个 `Idempotency-Key` 重试 |
| `504` | `UNKNOWN` | `WRITE_WAIT_TIMEOUT`：等待预算用完，写入可能已经提交或正在完成 | 按 `write_id` 核对，见 6.7 节 |
| `503` | `UNKNOWN` | 结果无法确认，例如 `WRITE_RESULT_UNKNOWN`、`NATIVE_RESULT_UNKNOWN`、`WRITER_OWNERSHIP_UNKNOWN` | 按 `write_id` 核对，见 6.7 节 |
| 连接断开或没有响应 | — | 无法确定 | 用同一个 `Idempotency-Key` 和相同内容重发，见 6.7 节 |

`UNKNOWN` 响应在可能时带原 `write_id` 和 `status_url`：

```json
{
  "write_id": "write_<64 位十六进制>",
  "status": "UNKNOWN",
  "code": "WRITE_WAIT_TIMEOUT",
  "message": "Reconcile or retry the same Idempotency-Key",
  "request_id": "<request-id>",
  "status_url": "/v1/spaces/sales_prod/writes/write_<64 位十六进制>"
}
```

超时不会回滚已经开始的事务，也不会把请求改成异步接入。

### 6.7 按 write_id 核对

原 principal（持有 `data:write`），或持有 `datasets:manage` 的 Key，可以读取写入回执：

```bash
curl -s "$COLVENNA_API_URL/v1/spaces/$COLVENNA_SPACE/writes/<write_id>" \
  -H "Authorization: Bearer $COLVENNA_API_KEY"
```

- 返回原回执（`COMMITTED` 或 `REJECTED`）。回执来自写者的原生账本，是权威结果。
- 账本暂时不可读、写者正在切换或写入仍在执行时，返回 `503` 和 `status: UNKNOWN`。稍后再查；一次“查不到”不能证明没有提交。

收到 `UNKNOWN` 或连接中断后，按以下顺序处理：

1. 有 `write_id` 时，反复读取 `status_url`，直到得到 `COMMITTED` 或 `REJECTED`。
2. 没有 `write_id`（例如连接断开）时，用**同一个** `Idempotency-Key` 和**相同内容**重发。服务端返回该身份的原结果；仍在执行时返回 `UNKNOWN` 和 `write_id`。重发时可以修改 `timeout_ms`，它不属于业务内容。
3. 不要换一个新的 `Idempotency-Key` 重发：如果原写入已经提交，新请求会再执行一次。

其他规则：

- 同一个 `Idempotency-Key` 加不同内容，返回 `409 WRITE_BINDING_CONFLICT`；另一个 principal 使用它，返回 `403 WRITE_OWNER_REQUIRED`。
- 写入身份在 30 天重试窗口内有效。窗口过期后，同一个身份的请求返回 `409 WRITE_IDENTITY_EXPIRED`，不会被当作新写入执行。

### 6.8 与查询、流水线的关系

- 写入后立即读到这次写入，用 `after_write` 一致性查询（[7.7 节](#77-一致性模式)）。直接表没有 release，用缺省的 `published` 模式查询直接表会失败（`PUBLICATION_NOT_AVAILABLE`）。
- 同步写不推进任何流水线水位，也不产生 release。流水线把直接表作为输入时，在运行开始时固定读取当时的完整快照。

## 7. 查询

查询是异步的：提交后得到 `query_id`，再轮询状态和结果。断开 HTTP 连接不会取消或重跑查询。

### 7.1 目录

需要 `query:read`：

```bash
curl -s "$COLVENNA_API_URL/v1/spaces/$COLVENNA_SPACE/catalog" -H "Authorization: Bearer $COLVENNA_API_KEY"
```

返回 `{"datasets": [{"dataset": "ads.dashboard", "schema": [...]}, ...]}`，只包含已注册、可查询的数据集及其列和类型。内部账本和未发布的数据不在其中。

### 7.2 提交查询

需要 `query:read`：

```bash
curl -s -X POST "$COLVENNA_API_URL/v1/spaces/$COLVENNA_SPACE/queries" \
  -H "Authorization: Bearer $COLVENNA_API_KEY" -H "Content-Type: application/json" \
  -d '{"sql": "SELECT order_date, revenue, order_count FROM ads.dashboard ORDER BY order_date",
       "client_request_id": "dashboard_2026_10_15_001",
       "consistency": "published", "max_rows": 1000, "timeout_ms": 30000}'
```

| 字段 | 必填 | 说明 |
| --- | --- | --- |
| `sql` | 是 | 一条只读 SQL，最多 64 KiB |
| `client_request_id` | 是 | 调用方生成的标识，作用同幂等键 |
| `max_rows` | 否 | 返回行数上限，1–10,000，缺省 1,000 |
| `timeout_ms` | 否 | 执行期限，1–300,000 毫秒，缺省 30,000 |
| `consistency` | 否 | `published`（缺省）、`current`、`after_write`、`after_ingestion`，见 7.7 节 |
| `release_id` | 否 | 指定历史 release，只能与 `published` 一起使用 |
| `write_id` | `after_write` 时必填 | 同步写回执的 `write_id` |
| `ingestion_id` | `after_ingestion` 时必填 | 接入批次的 `ingestion_id` |

返回 `202`：

```json
{"query_id": "qry_<64 位十六进制>", "status": "QUEUED", "status_url": "/v1/spaces/sales_prod/queries/qry_<64 位十六进制>"}
```

SQL 规则：

- 只能是一条 `SELECT`（包括 `WITH`），或对一条 `SELECT` 的 `EXPLAIN`。其他语句（DML、DDL、`ATTACH`、`COPY`、`PRAGMA`、`SET`、多条语句）以 `READ_ONLY_SQL_REQUIRED` 失败。
- 不能访问文件、网络或扩展（`EXTERNAL_ACCESS_FORBIDDEN`）。语法错误或引用了未注册、无权访问的表，以 `INVALID_OR_UNAUTHORIZED_SQL` 失败。
- 用 `<schema>.<name>` 引用数据集，名称不区分大小写。SQL 方言为 DuckDB 1.5.6，会话时区为 UTC。

幂等与排队：

- 同一个 `client_request_id` 加相同请求，返回同一个 `query_id` 和它当前的状态；改变请求内容返回 `409 QUERY_BINDING_CONFLICT`。
- 已经结束（包括失败）的查询不会因重试而重跑。要重新执行，换一个新的 `client_request_id`。
- 每个空间同时最多 8 个排队或运行中的查询，同一时刻只运行其中一个；整个环境最多 32 个排队、4 个同时运行。超出时提交返回 `429 QUERY_QUEUE_QUOTA`。

### 7.3 轮询与结果

```bash
curl -s "$COLVENNA_API_URL/v1/spaces/$COLVENNA_SPACE/queries/<query_id>" -H "Authorization: Bearer $COLVENNA_API_KEY"
```

| 状态 | 含义 |
| --- | --- |
| `QUEUED` | 排队中 |
| `RUNNING` | 执行中 |
| `SUCCEEDED` | 成功，结果可读 |
| `FAILED` | 失败，`error_code` 给出原因 |
| `CANCELLING` | 已请求取消，正在终止执行进程 |
| `CANCELLED` | 已取消 |
| `UNKNOWN` | 执行结果无法确认；不会自动重跑，也不会作为成功发布结果 |

查询所有者读取已成功的查询时，响应直接包含第一页结果：

```json
{
  "query_id": "qry_<64 位十六进制>",
  "status": "SUCCEEDED",
  "view": "result",
  "elapsed_ms": 4820,
  "columns": [
    {"name": "order_date", "type": "DATE", "encoding": "native_json"},
    {"name": "revenue", "type": "DECIMAL(38,2)", "encoding": "native_json"},
    {"name": "order_count", "type": "BIGINT", "encoding": "native_json"}
  ],
  "rows": [["2026-10-14", "610.00", 3], ["2026-10-15", "50.00", 1]],
  "row_count": 2,
  "truncated": false,
  "data_versions": [
    {"dataset": "ads.dashboard", "release_id": "<release_id>", "generation": "gen_<32 位十六进制>",
     "commit_sequence": 31, "snapshot_sequence": 35}
  ],
  "next_page_token": null,
  "export_url": "<api-url>/v1/spaces/sales_prod/queries/qry_<64 位十六进制>/export?token=<token>"
}
```

| 字段 | 说明 |
| --- | --- |
| `columns` | 列名、DuckDB 类型和编码；同名列按位置区分 |
| `rows` | 本页数据，按 `columns` 的位置排列 |
| `row_count` | 结果总行数（不超过 `max_rows`） |
| `truncated` | 为 `true` 时，实际结果超过 `max_rows`，只保留了前 `max_rows` 行 |
| `data_versions` | 实际读取的每个数据集的版本：`release_id`（直接表和 ODS 为 `null`）、`generation`、数据集提交序号 `commit_sequence` 和读取快照序号 `snapshot_sequence` |
| `next_page_token` | 下一页的 token；为 `null` 时已是最后一页 |
| `export_url` | 导出整个结果的地址，见 7.4 节 |
| `error_code` | 失败原因（仅失败时） |

查询参数 `view` 选择返回内容：`metadata` 只返回状态和耗时；`result` 返回结果（缺省，仅所有者、仅成功时）；`sql` 返回原 SQL，不含结果。

### 7.4 分页与导出

- 下一页：`GET .../queries/<query_id>?page_token=<next_page_token>&page_size=<n>`。`page_size` 为 1–10,000，缺省 500。分页读取同一份已保存的结果，不会重新执行 SQL。
- 导出：每次读取成功的查询，都会生成一个新的 `export_url`，有效期最长 2 分钟。请求它时同样要带 `Authorization` 头；它返回完整的类型化 JSON 结果（`columns`、`rows`、`data_versions`、`truncated`）。复制出去的地址不能被其他 principal 使用。

  ```bash
  curl -s "<export_url>" -H "Authorization: Bearer $COLVENNA_API_KEY" -o result.json
  ```

- 结果在生成后 10 分钟过期。过期后分页返回 `410 QUERY_RESULT_EXPIRED`，导出链接过期返回 `410 QUERY_LINK_EXPIRED`。过期结果不会自动重算；需要时用新的 `client_request_id` 再提交一次。

### 7.5 历史与原 SQL

- `GET /v1/spaces/{space_id}/queries` 返回当前 principal 最近的 50 个查询，最新的在前。
- 已结束查询的历史和原 SQL 保留 24 小时。读取原 SQL：`GET .../queries/<query_id>?view=sql`。

### 7.6 取消

```bash
curl -s -X POST "$COLVENNA_API_URL/v1/spaces/$COLVENNA_SPACE/queries/<query_id>/cancel" \
  -H "Authorization: Bearer $COLVENNA_API_KEY" -H "Content-Type: application/json" -d '{}'
```

- 排队中、尚未开始执行的查询直接变为 `CANCELLED`。运行中的查询先变为 `CANCELLING`，执行进程终止后变为 `CANCELLED`。
- 取消与完成同时发生时，先提交的一方生效：已经成功的查询保持 `SUCCEEDED`。已结束的查询保持原状态。
- 取消只影响这个查询，不影响写者或其他查询。

### 7.7 一致性模式

| `consistency` | 附加字段 | 读到的数据 | 可以引用的数据集 | 适用场景 |
| --- | --- | --- | --- | --- |
| `published`（缺省） | 无 | 流水线目标当前已发布的 release | 只能是已发布的流水线目标；引用 ODS、直接表或尚未发布的目标时以 `PUBLICATION_NOT_AVAILABLE` 失败 | 报表、看板、对外指标 |
| `published` | `release_id` | 该 release 所属数据集读这一历史 release；其他数据集读当前 release | 同上 | 复现某次结果、与下游对账 |
| `current` | 无 | 直接表和 ODS 的最新已提交状态，以及流水线目标的当前 release | 全部可查询的数据集 | 运营查询、检查 ODS 或直接表、读大表 |
| `after_write` | `write_id` | 至少包含这次同步写提交的当前状态 | 全部可查询的数据集 | 同步写之后立即读回 |
| `after_ingestion` | `ingestion_id` | 至少包含这次接入提交的当前状态 | 全部可查询的数据集 | 接入提交后立即核对 |

说明：

- **只暂存 SQL 引用的数据集。** 每次查询把 SQL 实际引用的数据集复制成私有快照，再在快照上执行 SQL。`data_versions` 恰好列出这些数据集；另外，指定的 `release_id` 所属的数据集和 `after_write` / `after_ingestion` 回执所在的数据集即使没有被引用，也会确认版本并列出。不引用任何数据集的 SQL（如 `SELECT 1`）在任何模式下都成功，`current` 和 `published` 下 `data_versions` 为空。
- **指定 release 不会被替换。** 查询服务尚未看到指定的 release 时，查询以 `PUBLICATION_RELEASE_NOT_READY` 失败，不会改读其他 release。稍后用新的 `client_request_id` 再提交。已被保留策略回收的 release 以 `PUBLICATION_RELEASE_COLLECTED` 失败。
- **`current` 不等于“刚刚提交”。** `current` 读取查询打开数据库时可见的最新提交，刚提交的写入可能还没有出现。结果中的 `commit_sequence` 标明实际读到的提交。需要确保读到某次写入时，用 `after_write` 或 `after_ingestion`。
- **提交后读取经由写者。** `after_write` / `after_ingestion` 由写者确认回执已提交后，在写者进程内复制快照。结果至少包含该提交，也可能包含之后已提交的更新或删除；它不是该提交时刻的历史快照，也不等待下游流水线。
  - `after_write` 只允许写入的原 principal，或同时持有 `datasets:manage` 的 Key；`after_ingestion` 只允许接入的原 principal，或同时持有 `monitor:read` 的 Key。
  - 回执尚未确认提交时以 `COMMIT_RECEIPT_UNCONFIRMED` 失败（`after_ingestion` 请在接入状态为 `COMMITTED` 后再提交）；同步写被拒绝时以 `COMMIT_RECEIPT_NOT_COMMITTED` 失败；没有可用的写者时以 `QUERY_SOURCE_NOT_READY` 失败；复制回复丢失时以 `QUERY_SOURCE_UNCONFIRMED` 失败，不会自动重读。
  - 快照上限为全部引用数据集合计 100,000 行、2 MiB，复制预算 5 秒。复制前在同一读事务中检查行数及保守的 JSON 字节估算（包含 Unicode 转义、schema/版本信息和类型格式开销）；超出时以 `QUERY_SOURCE_QUOTA` 失败。查询沙箱按 Arrow 批次装载并保留精确类型。复制超时返回 `QUERY_SOURCE_TIMEOUT`，查询自身的期限仍由 `timeout_ms` 控制。1.0.0 逐行装载快照，万行级表会超时；1.1.0 的变更与验收见[发行说明](release-notes.md#110-的变更)。
- `release_id`、`write_id`、`ingestion_id` 三者互斥，并且分别只能用于 `published`、`after_write`、`after_ingestion`；组合错误的请求在提交时以 `400 INVALID_QUERY_REQUEST` 拒绝。

### 7.8 暂存、超时与结果配额

| 限制 | 值 | 超出时 |
| --- | --- | --- |
| 暂存行数 | SQL 引用的全部数据集合计 2,000,000 行；按数据集的全部行计算，与 `WHERE` 条件无关 | 复制前以 `QUERY_STAGING_LIMIT` 失败 |
| 私有快照大小 | 1 GiB | `QUERY_SNAPSHOT_TOO_LARGE` |
| 结果行数 | `max_rows`，最多 10,000 | 截断，`truncated: true` |
| 结果大小 | JSON 结果最多 8 MiB | `QUERY_RESULT_TOO_LARGE` |
| 执行期限 | `timeout_ms`，缺省 30 秒，最长 300 秒 | `QUERY_TIMEOUT` |
| 内存与临时盘 | 由查询服务限定 | `QUERY_MEMORY_LIMIT`、`QUERY_DISK_QUOTA` |
| 结果保留 | 每个结果保留 10 分钟；整个环境最多同时保留 64 个结果、合计 64 MiB，每个空间 32 MiB | 查询执行完后以 `QUERY_RESULT_RETENTION_QUOTA` 失败 |

以上在执行中出现的错误都表现为查询状态 `FAILED` 加对应的 `error_code`。

结果保留配额意味着整个环境持续的查询吞吐约为每 10 分钟 64 个。每条查询都在新进程中打开数据库，服务端耗时中位数约 4–6 秒。写入高峰期间或刚结束时，读取同一张表的查询会明显变慢，可以把 `timeout_ms` 调大（最多 300,000）或错开高峰。实测数据见[性能报告](performance.md)。

### 7.9 查看与取消他人的查询

同时持有 `query:read` 和 `query:manage` 的 Key，可以查看同空间其他 principal 的查询状态（`view=metadata`）并取消它们，但不能读取他人的 SQL 或结果（`403 QUERY_OWNER_REQUIRED`）。

## 8. 流水线

流水线把 ODS 的事件逐层加工成 DWD、DWS、ADS 的数据集。一条流水线是若干任务组成的 DAG；每个任务是一段 SQL 或 Python 代码，读取固定版本的输入，产出一个目标数据集，再由写者校验、导入并发布为新的 release。

### 8.1 概念

| 概念 | 说明 |
| --- | --- |
| 定义（definition） | 流水线的 YAML / JSON 描述：任务、依赖、输入、输出、处理模式、参数和调度 |
| 草稿（draft） | 可编辑的定义加代码文件。`revision` 用于条件更新；`draft_revision` 标识这一版代码草稿 |
| 版本（`pipeline_version`） | 从草稿发布出的不可变版本，固定代码、运行环境、执行配置和 schema。之后编辑草稿不影响已发布的版本 |
| 激活版本（`active_version`） | 自动调度使用的版本 |
| 运行（run） | 对一个版本的一次执行，`run_id` 标识 |
| 任务执行（job）与尝试（attempt） | 一次运行中每个任务的执行为一个 job（`job_id`），每次计算尝试为一个 attempt（`attempt_id`） |
| 运行环境（runtime environment） | 锁定依赖的 Python 环境版本，供 Lambda / ECS 上的任务使用 |

### 8.2 贯穿示例：orders

[examples/orders](../examples/orders/README.md) 的 [pipeline.yaml](../examples/orders/pipeline.yaml) 定义四个任务：

```text
ods.orders
  → clean_orders（SQL，writer）→ dwd.orders
      → score_orders（Python / pandas，Lambda）→ dwd.order_scores
      → daily_sales（SQL，ECS，依赖 clean_orders 和 score_orders）→ dws.daily_sales
          → dashboard（SQL，writer）→ ads.dashboard
```

| 任务 | 代码 | 执行 | 输入 → 输出 |
| --- | --- | --- | --- |
| `clean_orders` | [clean_orders.sql](../examples/orders/sql/clean_orders.sql) | SQL / `writer_short` | ODS 事件 → 每个订单的最新有效版本 |
| `score_orders` | [score_orders.py](../examples/orders/python/score_orders.py) | Python / `python_lambda_standard`，环境 `pandas-demo` | 订单 → 高价值 / 退款分类 |
| `daily_sales` | [daily_sales.sql](../examples/orders/sql/daily_sales.sql) | SQL / `sql_ecs_medium`，环境 `sql-demo` | 订单 + 分类 → 每日营业额 |
| `dashboard` | [dashboard.sql](../examples/orders/sql/dashboard.sql) | SQL / `writer_short` | 每日营业额 → ADS 看板 |

四个任务都使用 `processing.mode: full` 和 `write_mode: versioned_replace`，每次运行完整重算。[quickstart](../examples/quickstart/README.md) 在已安装环境中注册数据集、构建两个运行环境、发布并运行这条流水线，并核对 ADS 结果。

### 8.3 定义格式

顶层字段：

| 字段 | 说明 |
| --- | --- |
| `spec_version` | 固定为 `1` |
| `pipeline_id` | 流水线标识，创建后不变 |
| `name` | 显示名称 |
| `schedule` | `{owner, expression, timezone}`，见 [8.10 节](#810-调度) |
| `max_active_runs` | 固定为 `1`：同一流水线同时最多一个活动运行 |
| `coalesce_pending` | 固定为 `true`：运行期间到达的数据合并到下一次运行 |
| `parameters` | 可选，参数名到 `{type, default, minimum, maximum}` 的映射，最多 128 个 |
| `tasks` | 1–128 个任务 |

参数类型为 `decimal_string`、`date`、`timestamp`、`timestamptz`、`integer`、`boolean`、`string`；`minimum` / `maximum` 只能用于 `decimal_string` 和 `integer`。参数不能是凭证或执行资源（名称不能是 `api_key`、`token`、`role`、`executor`、`target` 等）。

任务字段：

| 字段 | 必填 | 说明 |
| --- | --- | --- |
| `task_id` | 是 | 任务标识 |
| `depends_on` | 否 | 依赖的任务；必须存在且无环 |
| `language` | 是 | `sql` 或 `python` |
| `processing` | 是 | 处理模式，见 [8.7 节](#87-处理模式) |
| `executor` | 是 | `writer`、`lambda` 或 `ecs_task` |
| `entrypoint` | 是 | 代码包内的相对路径；SQL 以 `.sql` 结尾，Python 以 `.py` 结尾 |
| `resource_profile` | 是 | 批准的资源 profile，见 8.4 节 |
| `timeout_seconds` | 是 | 单次尝试的执行期限，不超过 profile 上限 |
| `max_attempts` | 是 | 尝试次数上限，1–10，包括首次执行 |
| `runtime_environment` | `lambda` / `ecs_task` 必填 | `{environment_id, version}`，引用同空间已 READY 的环境；`writer` 任务不能填写 |
| `input_mode` | `lambda` / `ecs_task` 必填 | `local_files` 或 `s3_files`；SQL 任务必须是 `local_files`；`writer` 任务不能填写 |
| `inputs` | 是 | 输入别名到输入绑定的映射，见 8.5 节 |
| `output` | 是 | 目标数据集与写入方式，见 8.5 节 |

orders 示例中 `score_orders` 的定义：

```yaml
  - task_id: score_orders
    language: python
    processing: {mode: full}
    executor: lambda
    entrypoint: python/score_orders.py
    runtime_environment: {environment_id: pandas-demo, version: v1}
    resource_profile: python_lambda_standard
    input_mode: local_files
    timeout_seconds: 300
    max_attempts: 3
    depends_on: [clean_orders]
    inputs:
      orders:
        task: clean_orders
        selection: snapshot
    output:
      dataset: dwd.order_scores
      write_mode: versioned_replace
      key_columns: [order_id]
      allow_empty: true
      schema:
        - {name: order_id, type: BIGINT, nullable: false}
        - {name: high_value, type: BOOLEAN, nullable: false}
        - {name: value_band, type: VARCHAR, nullable: false}
```

每个字段的逐项解释见 [score_orders 配置详解](../examples/orders/score-orders-guide.md)。

### 8.4 执行器与资源 profile

| `resource_profile` | `executor` | 语言 | 最长 `timeout_seconds` | 工作盘 | 输出上限 |
| --- | --- | --- | --- | --- | --- |
| `writer_short` | `writer` | SQL | 30 | 256 MiB（内存 256 MiB） | 100,000 行 / 32 MiB |
| `sql_lambda_standard` | `lambda` | SQL | 900 | 512 MiB（内存 1 GiB） | 1,000,000 行 / 128 MiB |
| `python_lambda_standard` | `lambda` | Python | 900 | 512 MiB | 1,000,000 行 / 128 MiB |
| `sql_ecs_medium` | `ecs_task` | SQL | 86,400 | 1 GiB（Task 1 vCPU / 4 GiB） | 2,000,000 行 / 256 MiB |
| `python_ecs_medium` | `ecs_task` | Python | 86,400 | 1 GiB（Task 1 vCPU / 4 GiB） | 2,000,000 行 / 256 MiB |

- Python 不能在 `writer` 上执行。`writer` 适合短小的 SQL 转换，使用平台固定的环境。
- `lambda` 适合有界的计算（标准 Lambda，最长 15 分钟）；长计算、大内存或大临时文件选 `ecs_task`。
- 外部执行器（`lambda`、`ecs_task`）需要 READY 的运行环境。SQL 任务的环境必须同时包含 `duckdb` 和 `pyarrow`（例如示例的 `sql-demo`），且 `duckdb` 为 1.1 或更高版本：SQL 任务在环境自带的 DuckDB 中执行，分区替换用 `query()` 表函数排序。构建时会实际执行一次 SQL 任务，不满足的版本在构建阶段失败（见 [8.8 节](#88-运行环境)）。
- 不能在定义或运行请求中指定函数、Task 模板、镜像或 IAM 角色；只能选择批准的 profile。空间配额可以限制允许使用的 profile（`422 SPACE_PROFILE_QUOTA`，见 [10.8 节](#108-配额与成本)）。

### 8.5 输入、输出与 SQL 任务

输入绑定有三种形式：

| 形式 | 写法 | 含义 |
| --- | --- | --- |
| 数据集 | `{dataset: ods.orders, selection: snapshot}` | 读取一个注册数据集（根输入） |
| 上游任务 | `{task: clean_orders, selection: snapshot}` | 读取同一次运行中该任务已发布的结果；该任务必须列在 `depends_on` 中 |
| 目标键状态 | `{state: target_key_state, selection: affected_keys}` | 增量任务读取自己目标上一 release 的键状态，见 8.7 节 |

`selection`：

| `selection` | 含义 |
| --- | --- |
| `snapshot` | 完整的固定快照：ODS 为截至某个提交序号的全部事件；流水线数据集为一个完整 release；直接表为运行固定输入时的完整数据 |
| `changes` | ODS 在 `(from, to]` 提交区间内的事件，按提交序号排序；只用于增量任务 |
| `affected_partitions` | 上游流水线数据集中受影响分区的完整行；只用于分区重算 |
| `affected_keys` | 只与 `state: target_key_state` 一起使用 |

输入在任务开始前一次性固定，运行期间不会变成“最新”。下游任务只能读取上游**已发布**的结果；上游计算结束但尚未发布时，下游保持等待。

输出字段：

| 字段 | 说明 |
| --- | --- |
| `dataset` | 目标数据集，只能是 `dwd.*`、`dws.*`、`ads.*`，已注册为 `write_owner: pipeline`；同一条流水线中每个目标只能由一个任务产出 |
| `write_mode` | `versioned_replace`、`merge_keys` 或 `replace_partitions`，与处理模式对应 |
| `key_columns` | 业务键，必须非空；全部输出中键必须唯一 |
| `allow_empty` | 是否允许发布后目标（或被替换的分区）为空 |
| `schema` | 输出列，与注册定义完全一致 |
| `partition_columns` | `replace_partitions` 必填 |
| `change_metadata` | `merge_keys` 必填，见 8.7 节 |

SQL 任务：

- 每个输入别名在 SQL 中是一张只读表，例如 orders 示例的 `events`、`orders`、`scores`、`daily`。
- 代码文件必须恰好是一条 `SELECT`（可带 `WITH`）。DML、DDL、`ATTACH`、`COPY`、`PRAGMA`、`SET` 和多条语句以 `SQL_READ_ONLY_REQUIRED` 失败；读取文件的函数以 `SQL_EXTERNAL_ACCESS_FORBIDDEN` 失败。
- 结果列的名称、顺序和 DuckDB 类型必须与 `output.schema` 逐列相同（`merge_keys` 为下文的 wire schema），否则以 `SQL_SCHEMA_MISMATCH` 失败。不会做隐式类型转换，请用 `CAST` 写出声明的类型，例如 `CAST(sum(amount) AS DECIMAL(38,2))`。
- ODS 输入除注册列外还有 `_colvenna_operation`（`append` / `upsert` / `delete`）和 `_colvenna_commit_sequence` 两列。`snapshot` 包含全部历史事件（包括旧版本和删除），需要自行按键取最新版本，例如：

  ```sql
  WITH ranked AS (
      SELECT *,
             row_number() OVER (
                 PARTITION BY order_id
                 ORDER BY source_version DESC, _colvenna_commit_sequence DESC
             ) AS version_rank
      FROM events
  )
  SELECT order_id,
         CAST(event_time AS DATE) AS order_date,
         CAST(amount AS DECIMAL(18,2)) AS amount
  FROM ranked
  WHERE version_rank = 1 AND _colvenna_operation <> 'delete';
  ```

### 8.6 Python 任务

Python 任务读取固定的 Parquet 输入，输出 Parquet 文件和一份 `output-manifest.json`。它不能直接写入数据库；结果由平台独立校验后，交给写者导入和发布。完整合同见 [Python 任务合同](design/python-task-contract.md)，示例见 [score_orders.py](../examples/orders/python/score_orders.py)。

- 调用形式：`python <entrypoint> --context <context.json 路径> --output-dir <输出目录>`。
- `context.json` 提供运行身份（`run_id`、`job_id`、`attempt_id`、`execution_id`）、参数 `parameters`、每个输入的 release、水位、schema 和文件清单，以及输出的目标数据集、schema、写入方式和目录。路径每次执行都不同，必须从 context 读取，不能写死。
- `input_mode: local_files` 时，平台先下载并校验输入，`files[].local_path` 为本地路径；`s3_files` 时只提供固定的 S3 对象和版本，脚本需要显式支持按版本读取。
- 输出写到 `output.directory`。关闭全部 Parquet 文件后，最后写入 `output-manifest.json`，列出每个文件的相对路径、行数、字节数和 SHA-256。
- 类型按注册 schema 输出，不写 pandas index；金额用 `decimal.Decimal`，不要经过 float。
- 空结果也要写出带正确 schema 的空 Parquet 和零行清单（或 `files: []`、`row_count: 0`）。
- 退出码 0 和脚本自己写的清单都不是成功的证明。平台会重新打开每个 Parquet，核对 schema、非空、行数、hash、键唯一性和分区；任何不符都使该尝试失败，不会发布。
- stdout / stderr 只作日志，每次执行合计最多 64 KiB。

Python 输出的类型映射、增量任务的 `context.processing`、`merge_keys` 与 `replace_partitions` 的清单要求，见 [Python 任务合同](design/python-task-contract.md)。

### 8.7 处理模式

每个任务独立选择处理模式，平台不会根据 SQL 自动推断：

| `processing.mode` | 必须搭配的 `write_mode` | 输入要求 | 用途 |
| --- | --- | --- | --- |
| `full` | `versioned_replace` | 全部输入为 `snapshot` | 每次完整重算，输出代表目标的完整数据 |
| `incremental` | `merge_keys` | 恰好两个输入：一个 ODS 的 `changes`，加 `{state: target_key_state, selection: affected_keys}` | 只处理新提交的事件，输出每个受影响键的最终 UPSERT / DELETE |
| `partition_recompute` | `replace_partitions` | 至少一个 `affected_partitions` 输入，不能有 `changes` | 只重算受影响的分区，输出这些分区的完整数据 |

**增量（`incremental` + `merge_keys`）**

定义片段（完整 SQL 见 [clean_orders_incremental.sql](../examples/orders/sql/clean_orders_incremental.sql)）：

```yaml
  - task_id: clean_orders
    language: sql
    processing: {mode: incremental, bootstrap: full, on_missing_state: fail}
    executor: writer
    entrypoint: sql/clean_orders_incremental.sql
    resource_profile: writer_short
    timeout_seconds: 30
    max_attempts: 3
    inputs:
      changes: {dataset: ods.orders, selection: changes}
      key_state: {state: target_key_state, selection: affected_keys}
    output:
      dataset: dwd.orders
      write_mode: merge_keys
      key_columns: [order_id]
      allow_empty: true
      schema:
        - {name: order_id, type: BIGINT, nullable: false}
        - {name: order_date, type: DATE, nullable: false}
        - {name: amount, type: "DECIMAL(18,2)", nullable: false}
      change_metadata:
        - {name: _colvenna_version, type: BIGINT, nullable: false}
        - {name: _colvenna_sequence, type: BIGINT, nullable: false}
```

- `processing.bootstrap` 必须是 `full`，并且必须声明 `on_missing_state`（`fail` 或 `full`）。首次运行，或任务定义、代码、执行器、环境、参数相对上一 release 有变化时，从空状态完整重建；上一 release 缺少状态时按 `on_missing_state` 处理。
- `change_metadata` 固定为上面的两列。任务输出的 wire schema 依次为 `_colvenna_op`（`UPSERT` / `DELETE`）、`schema` 中的业务列（DELETE 行的非键列可以为空）、`_colvenna_version`、`_colvenna_sequence`。
- `key_state` 输入包含受影响键在上一 release 的 `_colvenna_version`、`_colvenna_sequence` 和 `_colvenna_deleted`（包括已删除的键），用来阻止迟到的旧版本覆盖新状态或使已删除的记录复活。比已接受状态更旧的变化以 `PUBLICATION_STALE_CHANGE` 拒绝。
- 目标数据集注册时声明键状态：

  ```json
  "publication_state": {
    "protocol": "key_state_v1",
    "key_columns": ["order_id"],
    "allow_empty": true,
    "schema": [
      {"name": "order_id", "type": "BIGINT", "nullable": false},
      {"name": "_colvenna_version", "type": "BIGINT", "nullable": false},
      {"name": "_colvenna_sequence", "type": "BIGINT", "nullable": false},
      {"name": "_colvenna_deleted", "type": "BOOLEAN", "nullable": false}
    ]
  }
  ```

- 零行输出表示“没有键变化”：目标保持不变，水位照常推进。

**分区重算（`partition_recompute` + `replace_partitions`）**

- `processing.partition_columns` 必须等于 `output.partition_columns`，分区列类型只能是 `VARCHAR`、`BIGINT`、`DATE` 或 `BOOLEAN`，并且必须声明 `on_unbounded_impact`（`fail` 或 `full`）。
- 受影响分区由上游上一次消费的 release 与本次 release 的逐行差异计算：一条记录从周一改到周二，两个日期都会重算；删除后变空的分区也会出现在替换清单中。
- 无法确定影响范围时（首次运行、其他输入有变化、规则或版本变化），按 `on_unbounded_impact` 执行完整重算或失败。没有受影响分区时不发布新 release。
- 单个任务的受影响分区超过 4,000 个时，任务以 `TASK_IMPACT_RECORD_LIMIT` 失败。

计算增量不等于物理写入只有变化的行：发布时仍要从上一 release 构建新的 generation。各模式的语义与选择建议见[增量与全量](design/processing-modes.md)。

### 8.8 运行环境

Lambda 和 ECS 上的任务使用锁定依赖的运行环境。环境版本不可修改；依赖变化时创建新版本，并发布新的流水线版本。

创建环境版本（需要 `runtime:manage`）：

```bash
curl -s -X POST "$COLVENNA_API_URL/v1/spaces/$COLVENNA_SPACE/runtime-environments" \
  -H "Authorization: Bearer $COLVENNA_API_KEY" -H "Content-Type: application/json" \
  -d '{"environment_id": "pandas-demo", "version": "v1", "python_version": "3.12",
       "platform": "linux_arm64", "build_profile": "binary_wheels",
       "dependency_source": {"format": "requirements", "content": "numpy==2.2.6\npandas==2.2.3\npyarrow==24.0.0\n"}}'
```

返回 `201`：`{"environment_id": "pandas-demo", "version": "v1", "status": "DRAFT"}`。同一个 `environment_id` / `version` 重复提交相同内容返回原结果；内容不同返回 `409 ENVIRONMENT_VERSION_IMMUTABLE`。

构建（需要 `runtime:manage` 和 `Idempotency-Key`）：

```bash
curl -s -X POST "$COLVENNA_API_URL/v1/spaces/$COLVENNA_SPACE/runtime-environments/pandas-demo/versions/v1/build" \
  -H "Authorization: Bearer $COLVENNA_API_KEY" -H "Content-Type: application/json" \
  -H "Idempotency-Key: build-pandas-demo-v1" -d '{}'
```

返回 `202`：`{"build_id": "<build_id>", "status": "QUEUED"}`。轮询版本状态（`runtime:manage` 或 `monitor:read`）：

```bash
curl -s "$COLVENNA_API_URL/v1/spaces/$COLVENNA_SPACE/runtime-environments/pandas-demo/versions/v1" \
  -H "Authorization: Bearer $COLVENNA_API_KEY"
```

| `status` | 含义 |
| --- | --- |
| `DRAFT` | 已创建，尚未构建 |
| `RESOLVING` | 解析完整的传递依赖 |
| `BUILDING` | 构建 Lambda 与 ECS 两份镜像 |
| `VERIFYING` | 在镜像中做导入和合同检查；同时含 `duckdb` 与 `pyarrow` 的环境还会实际执行一次受控 SQL 任务（全量结果与分区替换），并确认外部访问被拒绝 |
| `READY` | 可被流水线引用；返回依赖锁 hash `dependency_lock_sha256`、镜像 digest 和批准的执行绑定 |
| `FAILED` | 构建失败，`failure` 给出阶段、错误码和说明 |
| `UNKNOWN` | 构建状态暂时无法确认；不会再次派发构建 |

规则：

- 平台为 CPython 3.12、`linux_arm64`；只接受 PyPI 上的二进制 wheel，不做源码构建。
- `content` 是受限的 requirements 语法：包名、可选 extras、精确版本或比较范围，最多 128 行，可以有空行和整行注释。URL、文件路径、可编辑安装、pip 选项和环境 marker 一律拒绝（`422 DEPENDENCY_SOURCE_NOT_APPROVED`）。没有对应 wheel 的依赖在解析阶段失败，`failure.code` 为 `DEPENDENCY_NOT_FOUND`。
- 构建通常需要 5–10 分钟。`READY` 和 `FAILED` 都不能重新构建；修改依赖请使用新的 `version`。
- 每个空间最多 64 个环境版本；同时进行的构建受空间配额（缺省 2 个）和全局上限（4 个）限制，超出返回 `429`。
- 任务引用的环境必须是 `READY`（`422 ENVIRONMENT_NOT_READY`），并且有与任务执行器和 profile 匹配的绑定（`422 ENVIRONMENT_EXECUTOR_PROFILE_MISMATCH`）。

列表：`GET /v1/spaces/{space_id}/runtime-environments`。设计说明见[包依赖与运行环境](design/runtime-environments.md)。

### 8.9 创建、校验、发布与激活

以下步骤都需要 `pipeline:manage`。完整的可执行流程见 [quickstart.py](../examples/quickstart/quickstart.py)。

**准备**

1. 注册全部输入和输出数据集（[第 4 节](#4-数据集注册)）。
2. 构建任务引用的运行环境，直到 `READY`（8.8 节）。
3. 把 YAML 定义转成 JSON。API 和 Admin Console 都只接收解析后的 JSON：

   ```bash
   python3 -c 'import json,sys,yaml; print(json.dumps(yaml.safe_load(open(sys.argv[1]))))' \
     examples/orders/pipeline.yaml > /tmp/orders-definition.json
   ```

**第 1 步：创建草稿。** 请求体为 `{"definition": <定义>, "artifacts": [...]}`。`artifacts` 列出每个代码文件的相对路径、SHA-256 和字节数；此时还没有上传，`upload_id` 先填占位值：

```json
{
  "definition": {"spec_version": 1, "pipeline_id": "orders", "...": "..."},
  "artifacts": [
    {"path": "python/score_orders.py", "upload_id": "pending", "sha256": "<文件 sha256>", "bytes": <字节数>},
    {"path": "sql/clean_orders.sql", "upload_id": "pending", "sha256": "<文件 sha256>", "bytes": <字节数>}
  ]
}
```

```bash
curl -s -X POST "$COLVENNA_API_URL/v1/spaces/$COLVENNA_SPACE/pipelines" \
  -H "Authorization: Bearer $COLVENNA_API_KEY" -H "Content-Type: application/json" -d @draft.json
```

返回 `201`：`{"pipeline_id": "orders", "revision": "rev_<…>", "status": "DRAFT"}`。新流水线的调度处于 `PAUSED`。同名流水线已存在时返回 `409 PIPELINE_ALREADY_EXISTS`。

**第 2 步：上传代码文件。** 每个文件二选一：

- 预签名上传：`POST .../pipelines/orders/artifact-uploads`，请求体 `{"path", "sha256", "bytes"}`，返回 `201` 和 `{"upload_id", "upload_url", "upload_headers", "expires_at"}`。在 5 分钟内用 PUT 上传文件内容，并**原样发送 `upload_headers` 中的每个头**：

  ```bash
  curl -s -X PUT --data-binary @examples/orders/sql/clean_orders.sql "<upload_url>" \
    -H "<upload_headers 中的每个头>: <值>"
  ```

  `upload_url` 相当于临时凭证，不要记录到日志或保存。

- 内联上传：适合 768 KiB 以内的 UTF-8 文本文件，`POST .../pipelines/orders/artifacts`，请求体 `{"path", "sha256", "content"}`，返回 `201` 和 `{"upload_id", "path", "sha256", "bytes"}`。

**第 3 步：用实际的 `upload_id` 替换草稿。** 先读取当前 `revision`：

```bash
curl -s "$COLVENNA_API_URL/v1/spaces/$COLVENNA_SPACE/pipelines/orders" -H "Authorization: Bearer $COLVENNA_API_KEY"
```

返回元数据：`pipeline_id`、`revision`、`draft_revision`、`status`（`ENABLED` / `PAUSED` / `ARCHIVED`）、`active_version`、`schedule_owner`、`schedule`、`definition` 和 `artifacts`。然后：

```bash
curl -s -X PUT "$COLVENNA_API_URL/v1/spaces/$COLVENNA_SPACE/pipelines/orders/draft" \
  -H "Authorization: Bearer $COLVENNA_API_KEY" -H "Content-Type: application/json" \
  -H 'If-Match: "rev_<…>"' -d @draft-with-upload-ids.json
```

返回 `200`：`{"pipeline_id": "orders", "revision": "rev_<…>", "status": "DRAFT"}`。这里的 `revision` 就是新的 `draft_revision`。

**第 4 步：校验。**

```bash
curl -s -X POST "$COLVENNA_API_URL/v1/spaces/$COLVENNA_SPACE/pipelines/orders/validate" \
  -H "Authorization: Bearer $COLVENNA_API_KEY" -H "Content-Type: application/json" \
  -d '{"draft_revision": "rev_<…>"}'
```

返回 `{"valid": true, "diagnostics": []}`，或 `valid: false` 和诊断，例如：

```json
{"valid": false, "diagnostics": [{"path": "/definition/tasks/1", "code": "PIPELINE_DATASET_NOT_REGISTERED", "message": "..."}]}
```

校验检查 DAG、数据集注册与 schema、目标 owner、代码文件、参数、运行环境和 profile。`draft_revision` 不是当前草稿时返回 `412 PIPELINE_DRAFT_CHANGED`。

**第 5 步：发布版本**（需要 `Idempotency-Key`）：

```bash
curl -s -X POST "$COLVENNA_API_URL/v1/spaces/$COLVENNA_SPACE/pipelines/orders/versions" \
  -H "Authorization: Bearer $COLVENNA_API_KEY" -H "Content-Type: application/json" \
  -H "Idempotency-Key: orders-publish-001" -d '{"draft_revision": "rev_<…>"}'
```

返回 `201`：`{"pipeline_id": "orders", "pipeline_version": "<pipeline_version>", "status": "PUBLISHED"}`。发布固定完整的代码包、环境版本、执行配置和 schema。响应丢失时用同一个 `Idempotency-Key` 和 `draft_revision` 重试，得到同一个版本。

**第 6 步：激活。** 重新读取 `revision`，然后：

```bash
curl -s -X POST "$COLVENNA_API_URL/v1/spaces/$COLVENNA_SPACE/pipelines/orders/activate" \
  -H "Authorization: Bearer $COLVENNA_API_KEY" -H "Content-Type: application/json" \
  -H 'If-Match: "rev_<…>"' -d '{"pipeline_version": "<pipeline_version>"}'
```

激活只决定自动调度使用的版本。手动运行可以指定任何已发布的版本。

代码包规则：

- 单个文件最多 10 MiB，整个包最多 50 MiB，最多 128 个文件；每个任务的 `entrypoint` 必须在 `artifacts` 中。
- 路径为相对路径，不能包含 `..` 或反斜杠；`.colvenna/` 是保留路径。
- 发布时 Python 文件只做语法编译检查，不会执行；SQL 文件必须是非空文本。如果代码包里包含 YAML 定义文件，它必须与草稿定义一致（`422 YAML_DEFINITION_MISMATCH`）。
- 已上传的代码字节计入空间配额 `max_stored_code_bytes`（缺省 512 MiB），超出返回 `429 SPACE_ARTIFACT_QUOTA`。
- 读取草稿中某个文件：`GET .../pipelines/orders/draft/artifacts?path=<相对路径>`，768 KiB 以内的 UTF-8 文本返回原文，其他文件只返回 hash 和大小。
- 列表与版本：`GET /v1/spaces/{space_id}/pipelines`、`GET .../pipelines/{pipeline_id}/versions`、`GET .../versions/{pipeline_version}`。

平台破坏性升级后，旧版本以 `409 PIPELINE_VERSION_BINDING_CHANGED` 被拒绝，需要重新发布；被保留策略回收的旧版本返回 `410 PIPELINE_VERSION_COLLECTED`。

### 8.10 调度

一条流水线的自动触发方由 `schedule.owner` 决定：

| `owner` | 含义 |
| --- | --- |
| `colvenna` | 由 Colvenna Isle 按 `expression` 和 `timezone` 自动创建运行 |
| `external` | 由外部编排（如 Airflow）调用运行 API；Colvenna Isle 不自动创建运行 |
| `none` | 只允许手动运行 |

修改调度（需要 `pipeline:manage` 和 `If-Match`）：

```bash
curl -s -X PUT "$COLVENNA_API_URL/v1/spaces/$COLVENNA_SPACE/pipelines/orders/schedule" \
  -H "Authorization: Bearer $COLVENNA_API_KEY" -H "Content-Type: application/json" \
  -H 'If-Match: "rev_<…>"' \
  -d '{"owner": "colvenna", "state": "ENABLED", "expression": "rate(5 minutes)", "timezone": "UTC"}'
```

- `state` 为 `ENABLED` 或 `PAUSED`。`ENABLED` 要求流水线已有激活版本（`409 PIPELINE_ACTIVE_VERSION_REQUIRED`）。
- `owner: colvenna` 必须提供 `expression` 和 `timezone`（IANA 时区名）。
- `expression` 支持 `rate(<n> <unit>)`，单位为 `minute(s)`、`hour(s)`、`day(s)`（数量为 1 时用单数）；以及六个字段的 `cron(分 时 日 月 星期 年)`，“日”和“星期”两个字段中恰好有一个是 `?`。最小粒度为 1 分钟。
- 以 `owner: colvenna` 启用调度时，调用的 Key 还必须持有 `pipeline:run`（`403 SCHEDULE_RUN_SCOPE_REQUIRED`）；定时运行以这个 Key 的身份创建。

定时行为：

- 每个 rate / cron 时间窗口最多创建一次运行，并且只在根输入有尚未处理的新提交时创建。
- 同一流水线已有活动运行时不创建新运行；运行结束后，下一个窗口从已处理的水位推进到当前水位，期间到达的数据全部包含在内。
- 调度器按分钟唤醒，不提供亚分钟级的处理时效。
- `PAUSED`、`external`、`none` 只停止新的自动运行，不取消已有运行。

### 8.11 运行

手动运行需要 `pipeline:run` 和 `Idempotency-Key`：

```bash
curl -s -X POST "$COLVENNA_API_URL/v1/spaces/$COLVENNA_SPACE/pipelines/orders/runs" \
  -H "Authorization: Bearer $COLVENNA_API_KEY" -H "Content-Type: application/json" \
  -H "Idempotency-Key: orders-run-2026-10-15" \
  -d '{"pipeline_version": "<pipeline_version>", "mode": "snapshot", "parameters": {"high_value_threshold": "500.00"}}'
```

| 字段 | 说明 |
| --- | --- |
| `pipeline_version` | 已发布的版本 |
| `mode` | `snapshot`（常规运行）或 `backfill`（按分区回填） |
| `parameters` | 可选，覆盖定义中的参数缺省值；按声明的类型和范围校验 |
| `inputs` | 可选，键为 `<task_id>.<input_alias>`，值为 `{"release_id": "..."}`。只能为读取流水线数据集的根输入指定 release（否则 `422 RUN_INPUT_RELEASE_REQUIRES_PIPELINE_DATASET`）；ODS 和直接表的根输入在准入时固定到当前已提交的水位 |
| `partitions` | `backfill` 必填：要回填的分区列表，每项是分区列到值的映射；定义中必须有 `partition_recompute` 任务 |

返回 `202`：

```json
{"run_id": "run_<…>", "pipeline_version": "<pipeline_version>", "status": "QUEUED",
 "status_url": "/v1/spaces/sales_prod/pipelines/orders/runs/run_<…>"}
```

- 同一个 `Idempotency-Key` 加相同请求返回同一个运行（包括 HTTP 超时后的重试）；改变请求返回 `409 IDEMPOTENCY_KEY_CONFLICT`。
- 省略的输入在运行开始时一次性固定，运行期间不会改读更新的数据。
- 流水线的调度方为 `external` 时，API 创建的运行记为 `external` 触发。

读取运行状态：运行的发起者（`pipeline:run`），或持有 `monitor:read` 的 Key：

```bash
curl -s "$COLVENNA_API_URL/v1/spaces/$COLVENNA_SPACE/pipelines/orders/runs/<run_id>" \
  -H "Authorization: Bearer $COLVENNA_API_KEY"
```

```json
{
  "run_id": "run_<…>",
  "pipeline_id": "orders",
  "pipeline_version": "<pipeline_version>",
  "status": "RUNNING",
  "mode": "snapshot",
  "trigger": "api",
  "parameters": {"high_value_threshold": "500.00"},
  "created_at": "2026-10-15T08:05:00Z",
  "observed_at": "2026-10-15T08:06:10Z",
  "projection_version": 8,
  "cancel_requested": false,
  "tasks": [
    {"task_id": "clean_orders", "job_id": "job_<…>", "status": "PUBLISHED", "executor": "writer", "release_id": "<release_id>"},
    {"task_id": "score_orders", "job_id": "job_<…>", "status": "RUNNING", "executor": "lambda", "attempt_id": "<attempt_id>"},
    {"task_id": "daily_sales", "job_id": "job_<…>", "status": "BLOCKED", "executor": "ecs_task"},
    {"task_id": "dashboard", "job_id": "job_<…>", "status": "BLOCKED", "executor": "writer"}
  ]
}
```

每个任务还可能包含 `attempts`（每次尝试的派发身份、时间、执行 ID 和错误码）、`input_versions`（实际绑定的输入版本）、`processing`（处理区间或分区）、`progress`（导入进度 `imported_chunks` / `total_chunks`）、`error_code` 和 `transient_code`。运行级还有 `root_inputs`、`finished_at`、`error_code` 和 `waiting`。

运行状态：

| 状态 | 含义 |
| --- | --- |
| `QUEUED` | 已准入，尚未开始；`waiting.code` 说明在等什么：`PIPELINE_RUN_ACTIVE`（同一流水线已有运行）、`SPACE_RUN_QUOTA`（空间活动运行配额已满）、`WRITER_AUTHORITY_UNAVAILABLE`（写者不可用） |
| `RUNNING` | 执行中 |
| `PUBLISHED` | 每个任务都已发布（各自独立发布，不是一个全局事务） |
| `FAILED` | 有任务失败；已发布的上游结果保留 |
| `CANCELLING` / `CANCELLED` | 取消中 / 已取消 |
| `VALIDATED` | 试运行结束且样本校验通过（见 8.13 节） |
| `UNKNOWN` | 状态暂时无法确认，需要与权威记录核对 |

任务状态依次为 `BLOCKED → READY → DISPATCHING → RUNNING → STAGED → VALIDATING → IMPORTING → PUBLISHED`；异常分支为 `RETRY_WAIT`、`FAILED`、`CANCELLING`、`CANCELLED` 和 `UNKNOWN`。任务的派发数超过空间配额时保持 `DISPATCHING`，`transient_code` 为 `SPACE_TASK_QUOTA`。只有写者发布成功（`PUBLISHED`）才会放行下游任务；Lambda 返回或 ECS 任务退出只表示计算结束。写者暂时不可用时，已开始的任务显示 `UNKNOWN` 并保留所处阶段，写者恢复后按原身份继续。

运行列表：`GET .../pipelines/orders/runs?limit=<1–100>&page_token=<token>`，缺省 20 条，最新的在前。只有 `pipeline:run` 的 Key 只看到自己的运行；持有 `monitor:read` 或 `pipeline:manage` 的 Key 看到全部。

### 8.12 取消与重试

取消（需要 `pipeline:run`；取消他人的运行还需要 `monitor:read` 或 `pipeline:manage`）：

```bash
curl -s -X POST "$COLVENNA_API_URL/v1/spaces/$COLVENNA_SPACE/pipelines/orders/runs/<run_id>/cancel" \
  -H "Authorization: Bearer $COLVENNA_API_KEY" -H "Content-Type: application/json" -d '{}'
```

- 返回 `{"run_id", "status"}`。取消阻止尚未发布的任务继续计算、导入和发布。
- 已经发布的任务结果保持发布。取消与发布同时发生时，先提交的一方生效；发布先完成的任务保持 `PUBLISHED`。

重试（需要 `pipeline:run` 和 `Idempotency-Key`）：

```bash
curl -s -X POST "$COLVENNA_API_URL/v1/spaces/$COLVENNA_SPACE/pipelines/orders/runs/<run_id>/retry" \
  -H "Authorization: Bearer $COLVENNA_API_KEY" -H "Content-Type: application/json" \
  -H "Idempotency-Key: orders-run-2026-10-15-retry-1" -d '{}'
```

- 只能重试 `FAILED` 的运行（否则 `409 RUN_NOT_RETRYABLE`；试运行不能重试）。有任务处于 `UNKNOWN` 时先要核对，返回 `409 RUN_RECONCILIATION_REQUIRED`。
- 重试为失败的任务创建新的尝试，沿用原来的定义、代码、环境、输入和已发布的上游结果。每个任务的尝试总数最多 10 次（`409 RUN_ATTEMPT_LIMIT`）。原输入已超过保留期时以 `INPUT_RETENTION_EXPIRED` 失败，不会改用新输入。
- 需要使用新输入、新代码或重算已发布的任务时，创建新的运行。

### 8.13 样本试运行

试运行只执行一个任务，读取输入的前若干行，按正式发布的规则校验输出，但**永远不发布**，不推进水位、不改变 release。需要 `pipeline:run` 和 `Idempotency-Key`：

```bash
curl -s -X POST "$COLVENNA_API_URL/v1/spaces/$COLVENNA_SPACE/pipelines/orders/trial-runs" \
  -H "Authorization: Bearer $COLVENNA_API_KEY" -H "Content-Type: application/json" \
  -H "Idempotency-Key: orders-trial-score-001" \
  -d '{"pipeline_version": "<pipeline_version>", "task_id": "score_orders", "sample_rows": 100}'
```

- 返回 `202` 和运行回执。用运行状态接口查看：`mode` 为 `trial`，`trial.validation.status` 为 `PENDING`、`PASSED`、`REJECTED` 或 `UNKNOWN`；通过时运行状态为 `VALIDATED`。
- 上游任务的输入绑定该任务当前已发布的 release；上游尚未发布时以 `TRIAL_UPSTREAM_NOT_PUBLISHED` 失败。
- `sample_rows` 受空间配额 `max_trial_sample_rows`（缺省 1,000）限制，超出返回 `422 SPACE_TRIAL_SAMPLE_QUOTA`。
- 查看样本输出需要同时持有 `pipeline:run` 和 `query:read`：`GET .../runs/<run_id>/preview?rows=<1–100>`（缺省 20 行），值以文本精确表示。
- 试运行与正式运行互不阻塞，但计入空间的活动运行和派发配额。

### 8.14 Airflow

Airflow 是可选的外部编排方，负责跨系统依赖；流水线内部的 DAG、固定输入、计算和发布仍由 Colvenna Isle 完成。示例 operator 见 [colvenna_operator.py](../examples/airflow/colvenna_operator.py)，示例 DAG 见 [colvenna_orders.py](../examples/airflow/dags/colvenna_orders.py)：

1. 把流水线的调度方设为 `owner: external`，避免 Colvenna Isle 与 Airflow 同时自动触发。
2. 为 Airflow 签发只含 `pipeline:run` 的专用 Key，存入 Airflow Variable `colvenna_pipeline_token`；API 地址存入 `colvenna_api_url`。
3. operator 提交固定的 `pipeline_version`。`Idempotency-Key` 由 DAG ID、Airflow `run_id` 和 task ID 派生，**不含** try number，因此 Airflow 重试、worker 重启或断线重连都对应同一个 Colvenna Isle 运行。
4. operator 轮询运行状态：只有 `PUBLISHED` 才成功，并通过 XCom 返回每个任务的 release；`FAILED` 或 `CANCELLED` 使 Airflow 任务失败，下游不放行。
5. Airflow 等待超时只结束本次 Airflow 尝试，不会取消 Colvenna Isle 运行。需要取消时显式调用取消接口。

operator 只使用 Python 标准库调用公开 API，不需要 AWS 凭证。

### 8.15 流水线的其他限制

| 项目 | 上限 |
| --- | --- |
| 每个任务的输入 | 最多 16 个；每个输入最多 100,000 行，一次输入导出合计最多 32 MiB（超出时以 `PARQUET_ROWS_QUOTA` / `PARQUET_BYTES_QUOTA` 失败） |
| 每个任务的输出 | 按 profile，见 8.4 节 |
| 同一流水线的活动运行 | 1 |
| 每个空间 | 见 [10.8 节](#108-配额与成本) 的配额 |
| 导入速度 | 写者分块导入（每块 512 行），实测约 150 行/秒，见[性能报告](performance.md) |

更多设计细节见[内置流水线设计](design/pipeline-design.md)。

## 9. Query Editor

Query Editor 是浏览器中的只读 SQL 工作台，调用第 7 节的查询 API。地址为 `<WebUrl>/`。在 Chromium 148 中验证。

### 9.1 连接

1. 打开页面，输入**数据空间**和一个持有 `query:read` 的 **API Key**，点击“连接”。
2. Key 只保存在当前页面的内存中：连接后密码框立即清空；页面不使用 localStorage、sessionStorage 或 cookie，也不接收 AWS 凭证。
3. 断开、刷新页面或切换空间后，需要重新输入 Key。

### 9.2 编辑与运行

- 左侧“授权目录”只显示 API 返回的数据集和列，可以搜索、展开列信息或插入到编辑器。
- 编辑器高亮关键字、字符串、带引号的标识符、数字和注释。Ctrl/⌘ + Enter 执行选中的 SQL，没有选中时执行整个编辑框；Ctrl + Space 补全 SQL 关键字和目录中的数据集与列。
- “查看计划”提交同一 SQL 的 `EXPLAIN` 查询。
- “读取方式”对应一致性模式：发布版本（`published`，缺省）、当前状态（`current`）、同步写入之后（`after_write`）、接入提交之后（`after_ingestion`）。发布版本可以填写 Release ID；后两种分别填写 Write ID 或 Ingestion ID。
- “最多返回”对应 `max_rows`（缺省 1,000），“超时（秒）”对应 `timeout_ms`（缺省 30 秒，最长 300 秒）。

### 9.3 结果与导出

- 结果区显示查询 ID、状态、耗时，以及服务端返回的实际数据版本。
- “上一页” / “下一页”读取同一份结果，不重新执行 SQL。`DECIMAL` 和大整数按原值显示，不会被舍入。
- “导出 JSON”通过认证 API 下载完整的类型化结果。
- “取消查询”只取消当前的 `query_id`；关闭页面不会取消服务端的查询。
- 查询历史按服务端的保留策略列出；“载入 SQL”读取历史查询的原 SQL。
- 结果过期（10 分钟）、被拒绝或 Key 被撤权时，页面清除当前结果。

### 9.4 请求未确认时

提交响应丢失或网络中断时，页面显示待核对并保留原查询身份。“核对原提交”用相同内容和同一个 `client_request_id` 重新提交，得到原 `query_id` 的实际状态；此时修改 SQL 不会改变待核对的原请求。只有服务端实际返回 `UNKNOWN` 时，页面才显示 `UNKNOWN`。

## 10. Admin Console

Admin Console 是空间的管理界面，地址为 `<WebUrl>/admin.html`。连接方式与 Query Editor 相同：输入数据空间和 API Key，Key 只保存在页面内存中。每个页面使用自己需要的 scope，服务端返回的 `401` / `403` 直接显示在该页面上。

请求结果未确认时，页面显示“请求未确认”并保留原幂等键或请求体；“核对原请求”重发同一个请求，不会生成新的身份。

### 10.1 页面一览

| 页面 | 功能 | 所需 scope |
| --- | --- | --- |
| 概览 | 写者、队列与 DLQ、各层与数据集版本、运行、查询、调度器的状态；运行指标 | `monitor:read` |
| API Keys | 列出、创建、轮换、吊销 Key | `keys:manage` |
| 流水线 | 定义列表与详情、编辑定义（JSON）与代码文件、DAG 预览、参数、校验、发布、激活、调度、运行、回填、取消、重试、历史、样本试运行 | 编辑与发布 `pipeline:manage`；运行与试运行 `pipeline:run`；只读查看 `monitor:read`；样本预览另需 `query:read` |
| 运行环境 | 环境列表、版本详情（含失败原因）、创建与构建 | `runtime:manage`；只读 `monitor:read` |
| 审计 | 审计记录，按操作筛选、分页 | `audit:read` |
| 诊断 | 从一个运行、接入、查询或 release 出发追踪关联记录 | `monitor:read`；查看原 SQL 另需 `audit:read` |
| 保留与回收 | 保留策略、保留锁定、回收与回收报告、分类规则 | 修改 `retention:manage`；只读 `monitor:read` |
| 配额与成本 | 空间配额与用量、按时间窗口的成本估算 | `monitor:read` |

### 10.2 概览

数据来自 `GET /v1/admin/spaces/{space_id}/status`。每个区块是一条观测，带 `status`、`observed_at`、`age_seconds`、`source` 和原因：

| 区块 | 内容 |
| --- | --- |
| `writer` | 写者的身份、心跳和最近提交 |
| `queue` / `dlq` | 接入队列积压、最老消息年龄、DLQ 消息数（共享队列的保守值） |
| `layers` / `datasets` | 各层水位；每个数据集的 owner、提交序号、当前 release 和发布时间 |
| `jobs` | 活动运行 |
| `queries` | 查询队列 |
| `scheduler` | 写者公平调度器的排队、等待与拒绝 |

| 观测状态 | 含义 |
| --- | --- |
| `OK` | 正常 |
| `DEGRADED` | 可用但有问题，例如队列超出空间阈值、DLQ 非空、有 `UNKNOWN` 运行、写者消费暂停 |
| `FAILED` | 失败 |
| `STOPPED` | 已停止，例如写者已被运维关闭 |
| `UNKNOWN` | 取不到观测，例如写者心跳缺失或超过 15 秒；不按正常处理 |

页面只在服务端返回 `OK` 时显示健康。

指标来自 `GET /v1/admin/spaces/{space_id}/metrics?window_seconds=<60–86400>`（缺省 3,600 秒）：写者心跳年龄、接入积压与最老消息年龄、DLQ 数量、接入从发送到提交的延迟、写者处理能力、调度器各类工作的等待时间、运行和查询的数量与耗时。每个序列带值、单位、状态、观测时间和样本数；缺少数据时为 `UNKNOWN` 并给出原因。

### 10.3 API Keys

列表、创建、轮换和吊销与第 3 节的 API 相同。完整 token 只在创建或轮换的响应中显示一次；隐藏、切换页面、刷新列表或断开连接时，token 同时从页面中清除。`last_used_at` 为空时显示“未观测”。

### 10.4 流水线与运行环境

- 定义以 JSON 编辑，浏览器不解析 YAML（转换方法见 [8.9 节](#89-创建校验发布与激活)）。
- 保存草稿时，浏览器计算文件的 SHA-256 并内联上传，再以 `If-Match` 替换草稿。超过请求体上限的文件请改用 API 的预签名上传。
- 校验失败的诊断带 JSON Pointer 路径（如 `/definition/tasks/1`）和错误码，不回显请求的值。
- 运行详情显示每次尝试的派发身份、执行 ID、代码和时间，导入期间显示 `imported_chunks/total_chunks`；排队中的运行显示 `waiting.code`。
- 运行环境构建失败时显示阶段、错误码和说明（例如 `DEPENDENCY_NOT_FOUND` 会列出没有 `linux_arm64` CPython 3.12 二进制 wheel 的依赖）。

### 10.5 审计

`GET /v1/admin/spaces/{space_id}/audit?limit=<1–200>&action=<操作>&page_token=<token>`（缺省 50 条），最新的在前。审计记录与它记录的变更在同一个事务中写入：变更成功则记录一定存在；幂等重复不会写第二条。

每条记录包含操作者（`API_KEY`、`IAM_OPERATOR` 或 `SYSTEM`，以及 principal、`key_id` 和指纹）、目标、请求 ID、结果和有限的细节，不含 token、Key 摘要、SQL 文本、结果值或云凭证。记录的操作：

| 类别 | 操作 |
| --- | --- |
| Key | `KEY_BOOTSTRAPPED`、`KEY_CREATED`、`KEY_ROTATED`、`KEY_REVOKED` |
| 数据集 | `DATASET_REGISTERED` |
| 流水线 | `PIPELINE_CREATED`、`PIPELINE_DRAFT_REPLACED`、`PIPELINE_ARTIFACT_ADMITTED`、`PIPELINE_VERSION_PUBLISHED`、`PIPELINE_ACTIVATED`、`PIPELINE_SCHEDULE_CHANGED` |
| 运行环境 | `ENVIRONMENT_CREATED`、`ENVIRONMENT_BUILD_ADMITTED` |
| 运行 | `RUN_CREATED`、`TRIAL_CREATED`、`RUN_CANCEL_REQUESTED`、`RUN_RETRIED` |
| 接入 | `INGESTION_REPLAY_INTENT` |
| 保留 | `RETENTION_POLICY_CHANGED`、`RETENTION_HOLD_PLACED`、`RETENTION_HOLD_RELEASED`、`RETENTION_COLLECTION_REQUESTED`、`RETENTION_COLLECTION_COMPLETED`、`GENERATION_PURGED` |
| 运维 | `QUOTA_POLICY_CHANGED`、`BACKUP_CREATED`、`SPACE_RESTORED` |

### 10.6 诊断

`GET /v1/admin/spaces/{space_id}/trace?kind=<kind>&id=<id>[&pipeline_id=<pipeline_id>]` 从一个身份出发，返回关联节点，每个节点带权威来源和观测时间：

| `kind` | 起点 | 关联节点 |
| --- | --- | --- |
| `run` | `run_id`，必须同时给 `pipeline_id` | 运行记录、版本、每个任务（与写者的发布记录对照）、输入导出、每次尝试、产物 |
| `ingestion` | `ingestion_id` | 原生提交状态与状态投影的对照、S3 原文、处理尝试与 DLQ 核对、首次包含该提交的流水线运行 |
| `release` | `release_id` | 写者的 release 记录（输入水位与版本）和产生它的运行 |
| `query` | `query_id` | 查询记录（执行身份、时间、结果大小与 hash）；SQL 只给出 sha256 与字节数 |

- 投影与权威记录不一致时，节点为 `UNKNOWN`，并在 `conflict` 中给出两边的原值；权威来源读不到时也为 `UNKNOWN` 并给出原因。trace 整体为 `COMPLETE` 或 `PARTIAL`。
- 同时持有 `audit:read` 的 Key 可以在 query 节点看到原 SQL（最多 1,024 个字符）。任何 trace 都不返回查询结果。

### 10.7 保留与回收

`GET /v1/admin/spaces/{space_id}/retention` 读取策略（`retention:manage` 或 `monitor:read`），`PUT` 修改（`retention:manage`，`If-Match` 为 `revision`）：

```bash
curl -s -X PUT "$COLVENNA_API_URL/v1/admin/spaces/$COLVENNA_SPACE/retention" \
  -H "Authorization: Bearer $COLVENNA_API_KEY" -H "Content-Type: application/json" \
  -H 'If-Match: "ret_<n>"' \
  -d '{"state": "ENABLED", "policy": {"release_keep": 5, "staging_seconds": 259200}}'
```

| 字段 | 含义 | 缺省 | 最小值 |
| --- | --- | --- | --- |
| `raw_seconds` | 接入原文的保留时间 | 45 天 | 45 天 |
| `replay_grace_seconds` | 最近一次重放之后原文继续保留的时间 | 14 天 | 14 天 |
| `upload_seconds` | 未被草稿或版本引用的代码上传 | 1 天 | 1 小时 |
| `staging_seconds` | 结束的运行的暂存数据文件 | 7 天 | 1 小时 |
| `pipeline_version_seconds` | 未激活、未被引用的流水线版本包 | 30 天 | 1 天 |
| `release_keep` | 每个数据集至少保留的 release 个数 | 3 | 1 |
| `release_seconds` | 超出 `release_keep` 的 release 的最短保留时间 | 7 天 | 1 小时 |
| `failed_build_seconds` | 失败构建的产物 | 7 天 | 1 小时 |

- 新空间的保留策略为 `DISABLED`（不做定时回收），`revision` 为 `ret_0`，第一次修改时 `If-Match` 填 `"ret_0"`。`policy` 中省略的字段使用缺省值。
- 低于最小值返回 `422 RETENTION_POLICY_BELOW_MINIMUM`。`state` 为 `ENABLED` 时，修改策略的 Key 成为定时回收的执行身份；`DISABLED` 时不做定时回收。
- 当前 release、被保留锁定或活动运行引用的 release、仍有效的输入所依赖的 release 不会被回收。读取已回收的 release 以 `PUBLICATION_RELEASE_COLLECTED` 失败，不会改读其他版本；已回收的流水线版本返回 `410 PIPELINE_VERSION_COLLECTED`。
- 查询结果（10 分钟）和查询历史（24 小时）由查询服务自行回收；原生数据库的文件只由其自身协议维护。

保留锁定（`retention:manage`）用于回填、审计和备份期间保护对象：

```bash
curl -s -X POST "$COLVENNA_API_URL/v1/admin/spaces/$COLVENNA_SPACE/retention/holds" \
  -H "Authorization: Bearer $COLVENNA_API_KEY" -H "Content-Type: application/json" \
  -d '{"target": {"kind": "release", "id": "<release_id>", "dataset": "ads.dashboard"},
       "reason": "Q4 audit", "expires_at": "2027-01-31T00:00:00Z"}'
```

- `target.kind` 为 `release`、`pipeline_version`、`run`、`ingestion` 或 `environment`，可附带 `pipeline_id` 和 `dataset`。
- `expires_at` 必须在未来 366 天以内（`422 RETENTION_HOLD_EXPIRY`）；每个空间最多 256 个有效锁定（`429 RETENTION_HOLD_QUOTA`）。
- 解除：`POST .../retention/holds/<hold_id>/release`。

回收（`retention:manage` 和 `Idempotency-Key`）：`POST .../retention/collections`，请求体 `{"dry_run": true}` 只生成计划，`false` 实际删除；返回 `202` 和 `collection_id`。报告：`GET .../retention/collections/<collection_id>`，状态为 `QUEUED`、`RUNNING`、`COMPLETED`、`FAILED` 或 `UNKNOWN`，列出每个对象的类别、版本、大小、动作和原因。回收只删除经权威记录确认无引用的精确对象版本。

### 10.8 配额与成本

配额由运维设置（见[安装部署手册](installation.md#7-开通空间与首个管理员-key)），空间内的 Key 不能修改。`GET /v1/admin/spaces/{space_id}/quotas`（`monitor:read`）返回策略和每项的用量、上限、观测时间与执行方式（`REJECT`、`WAIT` 或 `BACKPRESSURE`）：

| 配额 | 缺省 | 超出时 |
| --- | --- | --- |
| `max_active_runs` | 4 | 新运行保持 `QUEUED`，`waiting.code` 为 `SPACE_RUN_QUOTA` |
| `max_dispatched_jobs` | 8 | 任务保持 `DISPATCHING`，`transient_code` 为 `SPACE_TASK_QUOTA` |
| `max_concurrent_builds` | 2（全局 4） | `429 SPACE_BUILD_QUOTA` |
| `max_pipelines` | 64 | `429 SPACE_PIPELINE_QUOTA` |
| `max_pipeline_versions` | 200 | `429 SPACE_PIPELINE_VERSION_QUOTA` |
| `max_stored_code_bytes` | 512 MiB | `429 SPACE_ARTIFACT_QUOTA` |
| `max_trial_sample_rows` | 1,000 | `422 SPACE_TRIAL_SAMPLE_QUOTA` |
| `allowed_profiles` | 全部 profile | `422 SPACE_PROFILE_QUOTA` |
| `max_task_memory_bytes`、`max_task_workspace_bytes` | 2 GiB、1 GiB | `422 SPACE_PROFILE_QUOTA` |

同一接口还报告平台的其他空间限额，例如接入背压、查询队列与结果保留。被配额拒绝的请求在产生任何效果之前就被拒绝；等待中的工作保留原身份，不会被丢弃。

成本估算：`GET /v1/admin/spaces/{space_id}/costs?since=<时间>&until=<时间>`（`monitor:read`），窗口缺省为最近 24 小时，最长 7 天。结果为实测用量乘以当前环境区域的公开单价（`basis: MEASURED_USAGE_X_LIST_PRICE`），不含免费额度、折扣和税，**不是账单**：

| 类别 | 计量对象 |
| --- | --- |
| `lambda_compute` | Lambda 任务尝试的执行时长 × 内存 |
| `ecs_compute` | ECS 任务尝试的运行窗口 × vCPU / 内存 |
| `writer_compute` | 写者上的 SQL 任务、输入导出和导入耗时 |
| `s3_storage`、`s3_requests` | 输入导出的存储与请求 |
| `codebuild`、`ecr_storage` | 运行环境的构建时间与镜像存储 |
| `query_compute` | 查询的执行时间 |

共享基础设施（写者与查询任务的空闲时间、负载均衡、网络、API Gateway、CloudFront 等）列在 `excluded` 中，不做分摊。无法计量的工作负载列在 `unmeasured` 中，不按零计价；有这类项目时 `status` 为 `PARTIAL`。

没有当前区域的价目表时，`status` 为 `UNKNOWN`，`prices.region` 仍为当前区域；明细与合计为空，`unmeasured` 注明 `REGIONAL_PRICES_UNAVAILABLE`。这表示没有可用单价，不表示零成本。当前源码包含 `us-west-2`（2026-10-18）和 `us-east-2`（2026-10-21）从 AWS Price List API 核对的价目表，其他区域按此规则返回 `UNKNOWN`。

## 11. 限制与配额汇总

以下为 1.1.0 的准入上限，不是性能承诺。规格与实测基线见[发行说明](release-notes.md)和[性能报告](performance.md)。

| 领域 | 项目 | 上限 |
| --- | --- | --- |
| API | 请求体 | 1 MiB |
| API | 网关限流 | 默认每条路由 10 rps，突发 20 |
| 数据集 | 每个空间 | 64 个；配置合计 350,000 字节 |
| 数据集 | 每个数据集的列 | 256 |
| 接入 | 每批记录 | 10,000（且受空间 `max_batch_records` / `max_batch_bytes` 限制） |
| 接入 | 重放窗口 | 原文创建后 30 天 |
| 同步写 | 每请求项数 | 1,000 |
| 同步写 | `timeout_ms` | 1–20,000，缺省 10,000 |
| 同步写 | 重试窗口 | 30 天 |
| 查询 | SQL 文本 | 64 KiB |
| 查询 | `max_rows` | 10,000，缺省 1,000 |
| 查询 | `timeout_ms` | 300,000，缺省 30,000 |
| 查询 | 暂存行数 | 2,000,000（`QUERY_STAGING_LIMIT`） |
| 查询 | 私有快照 | 1 GiB |
| 查询 | 结果 JSON | 8 MiB |
| 查询 | `after_write` / `after_ingestion` 快照 | 100,000 行、保守 JSON 字节估算 2 MiB、5 秒复制预算；复制前检查配额 |
| 查询 | 结果保留 | 10 分钟；环境内最多 64 个、64 MiB，每空间 32 MiB |
| 查询 | 并发 | 每空间 8 个排队或运行、同时运行 1 个；环境内 32 个排队、4 个运行 |
| 查询 | 历史 | 24 小时，列表最多 50 条 |
| 流水线 | 每条流水线 | 128 个任务、128 个参数、同时 1 个活动运行 |
| 流水线 | 代码 | 单文件 10 MiB、整包 50 MiB、128 个文件 |
| 流水线 | 每个任务 | 尝试最多 10 次；最多 16 个输入，每个输入 100,000 行，导出合计 32 MiB |
| 流水线 | 输出 | 按 profile：100,000 / 1,000,000 / 2,000,000 行 |
| 流水线 | 受影响分区 | 每个任务 4,000 个 |
| 运行环境 | 每个空间 | 64 个版本；依赖源最多 128 行 |
| 写者 | 活动空间 | 每个写者进程最多 2 个（`WRITER_DATABASE_LIMIT`） |
| Key | 每个空间列表 | 1,000 个 |

已知限制（详见[发行说明](release-notes.md#限制)）：

- 每个数据库只有一个写者，所有写入在它上面排队。
- 被外键引用的行，其索引列和 `VARCHAR[]` 列的值不能改变。
- 同步写和导入逐行执行：1,000 行的同步写约 5 秒，导入约 150 行/秒。
- 没有约定的 SLA、RPO 或 RTO。

## 12. 错误码速查

常用错误码如下。完整列表见 [API 合同](design/api-contract.md)。“查询失败”一列的错误码出现在查询状态 `FAILED` 的 `error_code` 中，而不是提交请求的 HTTP 响应中。

**通用**

| HTTP | 错误码 | 含义 | 处理 |
| --- | --- | --- | --- |
| 400 | `INVALID_REQUEST` | 请求体不符合合同 | 按合同修正 |
| 401 | — | 缺少 `Authorization` | 添加 Bearer Key |
| 403 | `FORBIDDEN` | Key 无效，或缺少空间 / scope 权限 | 检查 Key 和 scope |
| 403 | `CREDENTIAL_EXPIRED` | Key 已过期 | 换用有效 Key |
| 403 | `SPACE_GENERATION_CHANGED` | 空间已重建，Key 或身份属于旧 generation | 使用新 generation 的 Key 和身份 |
| 404 | `ROUTE_NOT_FOUND` | 路由或方法不存在 | 检查路径 |
| 413 | `BODY_TOO_LARGE` | 请求体超过 1 MiB | 拆分请求 |
| 415 | `JSON_REQUIRED` | 缺少 `Content-Type: application/json` | 添加请求头 |
| 428 | `IDEMPOTENCY_KEY_REQUIRED` | 缺少或格式错误的 `Idempotency-Key` | 添加幂等值 |
| 429 | — | 网关限流 | 退避后重试 |
| 503 | `CONTROL_UNAVAILABLE` | 控制面暂时不可用 | 原样重试 |
| 503 | `WRITER_DATABASE_LIMIT` | 写者已打开 2 个空间 | 联系运维，见 13 节 |

**Key 与注册**

| HTTP | 错误码 | 含义 |
| --- | --- | --- |
| 403 | `DELEGATION_FORBIDDEN` | 请求的 scope 或到期时间超出签发者的上限 |
| 409 | `ROTATION_ALREADY_ISSUED` | 该 Key 已经轮换过 |
| 428 | `SPACE_GENERATION_REQUIRED` | 注册缺少 `If-Match: "<generation>"` |
| 409 | `SPACE_REBUILT` | `If-Match` 中的 generation 已过期 |
| 409 | `DATASET_CONFLICT` | 同名数据集已用不同定义注册 |
| 409 | `FOREIGN_KEY_TARGET_UNREGISTERED` / `FOREIGN_KEY_TARGET_MISMATCH` | 外键目标未注册为直接表，或列不匹配 |
| 413 | `DATASET_LIMIT` | 超过 64 个数据集 |

**接入**

| HTTP | 错误码 | 含义 |
| --- | --- | --- |
| 403 | `INGESTION_OPERATION_FORBIDDEN` | 操作不在注册的 `operations` 中 |
| 403 | `INGESTION_OWNER_MISMATCH` | 另一个 principal 的接入身份 |
| 409 | `IDEMPOTENCY_CONFLICT` | 同一 `Idempotency-Key` 用于不同内容 |
| 409 | `SCHEMA_VERSION_MISMATCH` | `schema_version` 不是注册版本 |
| 409 | `REPLAY_WINDOW_EXPIRED` | 超过 30 天重放窗口 |
| 429 | `SPACE_BATCH_QUOTA`、`QUEUE_BACKLOG`、`QUEUE_AGE`、`WRITER_DRAIN_BUDGET`、`WRITER_CAPACITY_UNAVAILABLE` | 背压，见 5.6 节 |
| 503 | `QUEUE_AGE_UNKNOWN` | 取不到队列年龄观测，原样重试 |
| 503 | `QUEUE_SEND_UNCONFIRMED`、`ARCHIVE_UNCONFIRMED` 等，`status: UNKNOWN` | 结果不确定，见 5.7 节 |
| 状态 `FAILED` | `EVENT_IDENTITY_CONFLICT`、`BUSINESS_VERSION_CONFLICT` | 批次内容冲突，见 5.4 节 |

**同步写**

| HTTP | 错误码 / 状态 | 含义 |
| --- | --- | --- |
| 403 | `DIRECT_WRITE_TARGET_REQUIRED` | 目标不是直接表 |
| 403 | `WRITE_OWNER_REQUIRED` | 写入身份属于另一个 principal |
| 409 | `WRITE_BINDING_CONFLICT` | 同一 `Idempotency-Key` 用于不同内容 |
| 409 | `WRITE_IDENTITY_EXPIRED` | 超过 30 天重试窗口 |
| 422 | `REJECTED` / `NATIVE_CONSTRAINT` | 违反原生约束，整批回滚 |
| 422 | `INVALID_WRITE_REQUEST` | 请求不合法 |
| 429 | `NOT_SUBMITTED`（`WRITER_QUEUE_FULL`、`WAIT_BUDGET_BEFORE_EXECUTION`） | 执行前拒绝，可用原键重试 |
| 503 | `UNKNOWN`（`WRITE_RESULT_UNKNOWN`、`NATIVE_RESULT_UNKNOWN` 等） | 结果不确定，按 `write_id` 核对 |
| 504 | `UNKNOWN`（`WRITE_WAIT_TIMEOUT`） | 等待超时，可能已提交，按 `write_id` 核对 |

**查询**

| HTTP / 位置 | 错误码 | 含义 |
| --- | --- | --- |
| 400 | `INVALID_QUERY_REQUEST` | 请求字段不合法，或一致性条件组合错误 |
| 409 | `QUERY_BINDING_CONFLICT` | 同一 `client_request_id` 用于不同请求 |
| 429 | `QUERY_QUEUE_QUOTA` | 查询队列已满 |
| 429 | `QUERY_EXECUTOR_QUOTA` | 读取目录时执行槽已满，稍后重试 |
| 403 | `QUERY_OWNER_REQUIRED` | 读取他人的 SQL 或结果 |
| 404 | `QUERY_NOT_FOUND` | 查询不存在 |
| 410 | `QUERY_RESULT_EXPIRED` / `QUERY_LINK_EXPIRED` | 结果或导出链接已过期 |
| 查询失败 | `READ_ONLY_SQL_REQUIRED` | 不是单条只读语句 |
| 查询失败 | `INVALID_OR_UNAUTHORIZED_SQL` | 语法错误，或引用未注册 / 无权访问的表 |
| 查询失败 | `EXTERNAL_ACCESS_FORBIDDEN` | 访问文件、网络或扩展 |
| 查询失败 | `PUBLICATION_NOT_AVAILABLE` | `published` 模式引用了 ODS、直接表或未发布的目标 |
| 查询失败 | `PUBLICATION_RELEASE_NOT_READY` | 指定的 release 尚未可见 |
| 查询失败 | `PUBLICATION_RELEASE_COLLECTED` | 指定的 release 已被回收 |
| 查询失败 | `COMMIT_RECEIPT_UNCONFIRMED` / `COMMIT_RECEIPT_NOT_COMMITTED` | 提交后读取的回执未确认提交 / 写入被拒绝 |
| 查询失败 | `QUERY_SOURCE_NOT_READY` / `QUERY_SOURCE_QUOTA` / `QUERY_SOURCE_UNCONFIRMED` | 提交后读取：写者不可用 / 超过行数或字节配额 / 复制回复丢失 |
| 查询失败 | `QUERY_STAGING_LIMIT` | 暂存超过 2,000,000 行 |
| 查询失败 | `QUERY_SNAPSHOT_TOO_LARGE` / `QUERY_RESULT_TOO_LARGE` | 快照超过 1 GiB / 结果超过 8 MiB |
| 查询失败 | `QUERY_TIMEOUT` | 超过 `timeout_ms` |
| 查询失败 | `QUERY_MEMORY_LIMIT` / `QUERY_DISK_QUOTA` | 内存 / 临时盘不足 |
| 查询失败 | `QUERY_RESULT_RETENTION_QUOTA` | 结果保留配额已满 |

**流水线与运行环境**

| HTTP / 位置 | 错误码 | 含义 |
| --- | --- | --- |
| 404 | `PIPELINE_NOT_FOUND` / `PIPELINE_VERSION_NOT_FOUND` / `RUN_NOT_FOUND` | 对象不存在 |
| 409 | `PIPELINE_ALREADY_EXISTS` | 同名流水线已存在 |
| 409 | `PIPELINE_ACTIVE_VERSION_REQUIRED` | 启用调度前需要激活版本 |
| 409 | `PIPELINE_VERSION_BINDING_CHANGED` | 版本的绑定已变化（如平台升级），需要重新发布 |
| 409 | `RUN_NOT_RETRYABLE` / `RUN_RECONCILIATION_REQUIRED` / `RUN_ATTEMPT_LIMIT` | 不能重试 / 先核对 `UNKNOWN` 任务 / 尝试次数用尽 |
| 410 | `PIPELINE_VERSION_COLLECTED` | 版本包已被回收 |
| 412 | `PIPELINE_REVISION_CHANGED` / `PIPELINE_DRAFT_CHANGED` | `revision` / `draft_revision` 已过期 |
| 403 | `SCHEDULE_RUN_SCOPE_REQUIRED` | 启用 Colvenna Isle 调度的 Key 缺少 `pipeline:run` |
| 422 | `PIPELINE_DATASET_NOT_REGISTERED` | 定义引用了未注册的数据集 |
| 422 | `PIPELINE_TARGET_SCHEMA_OR_OWNER_MISMATCH` | 输出与目标注册不一致，或目标不是 `pipeline` |
| 422 | `RUN_REQUEST_INVALID` | 运行参数、输入或分区不合法 |
| 422 | `RUN_INPUT_RELEASE_REQUIRES_PIPELINE_DATASET` | 只能为流水线数据集的根输入指定 release |
| 422 | `ENVIRONMENT_NOT_READY` / `ENVIRONMENT_EXECUTOR_PROFILE_MISMATCH` | 环境未就绪 / 没有匹配执行器与 profile 的绑定 |
| 422 | `DEPENDENCY_SOURCE_NOT_APPROVED` / `DEPENDENCY_LIMIT` | 依赖源语法不允许 / 超过 128 行 |
| 409 | `ENVIRONMENT_VERSION_IMMUTABLE` | 环境版本已用不同内容创建 |
| 任务失败 | `SQL_READ_ONLY_REQUIRED` / `SQL_EXTERNAL_ACCESS_FORBIDDEN` / `SQL_SCHEMA_MISMATCH` | SQL 不是单条 SELECT / 访问外部资源 / 结果列与声明不一致 |
| 任务失败 | `PARQUET_ROWS_QUOTA` / `PARQUET_BYTES_QUOTA` | 输入导出超过行数或字节上限 |
| 任务失败 | `TASK_IMPACT_RECORD_LIMIT` | 受影响分区超过 4,000 个 |
| 任务失败 | `INPUT_RETENTION_EXPIRED` | 重试时原输入已过保留期 |
| 试运行失败 | `TRIAL_UPSTREAM_NOT_PUBLISHED` | 上游任务尚无已发布的 release |

**保留**

| HTTP | 错误码 | 含义 |
| --- | --- | --- |
| 412 | `RETENTION_REVISION_CHANGED` | `If-Match` 缺失或已过期 |
| 422 | `RETENTION_POLICY_BELOW_MINIMUM` | 低于产品最小值 |
| 422 | `RETENTION_HOLD_EXPIRY` | 锁定到期时间不在未来 366 天内 |
| 429 | `RETENTION_HOLD_QUOTA` | 有效锁定超过 256 个 |

## 13. 常见问题与故障排查

**接入返回了 202，但查询不到数据。**
`202` 只表示已接收。先查询接入状态，等到 `COMMITTED`。之后：

- `published` 模式查不到 ODS（会以 `PUBLICATION_NOT_AVAILABLE` 失败）；
- 用 `current` 查询时，刚提交的数据可能还要稍等才可见；
- 需要立即读到时，用 `after_ingestion` 带上 `ingestion_id`。

**接入状态一直是 UNKNOWN。**
写者可能尚未处理该批次，或者暂时读不到权威记录。查看 Admin Console 概览中写者和队列的状态，或用诊断接口查看处理尝试。不要换新的 `Idempotency-Key` 重发。写者被运维关闭（`STOPPED`）时，批次保留在队列中，重新开放后继续处理。

**接入状态是 FAILED。**
查看 `error_code`。`EVENT_IDENTITY_CONFLICT` 或 `BUSINESS_VERSION_CONFLICT` 表示批次内容与已有事件冲突，修正数据后用新的 `Idempotency-Key` 提交。

**同步写返回 504 或连接中断，要不要重试？**
不要换新键重发。有 `write_id` 时读取 `GET .../writes/<write_id>`；没有时用同一个 `Idempotency-Key` 和相同内容重发。见 [6.7 节](#67-按-write_id-核对)。

**同步写返回 422 NATIVE_CONSTRAINT，修正后重发仍然得到同样的拒绝。**
同一个 `Idempotency-Key` 永远返回原结果。修正数据后请使用新的 `Idempotency-Key`。

**直接表在 Query Editor 中查询失败，提示 PUBLICATION_NOT_AVAILABLE。**
直接表没有 release。把读取方式改为“当前状态”（`current`），或在同步写之后用 `after_write`。

**after_write 查询以 QUERY_TIMEOUT 失败。**
1.1.0 使用批量装载并在复制前检查配额（1.0.0 的逐行装载会造成万行级超时）。仍以 `QUERY_TIMEOUT` 失败时，核对查询的 `timeout_ms` 与写者负载。超过快照配额时，减少引用的数据集，或在可以接受读者可见版本时使用 `current`。

**查询以 QUERY_STAGING_LIMIT 失败，但 SQL 带了 WHERE 条件。**
暂存按 SQL 引用的数据集的全部行计算，与 `WHERE` 无关。改为查询已聚合的 DWS / ADS 数据集，或用流水线预先缩小数据。

**查询失败后重试，得到的还是同一个失败。**
同一个 `client_request_id` 对应同一个查询。重新执行时换一个新的 `client_request_id`。

**查询以 QUERY_RESULT_RETENTION_QUOTA 失败。**
环境内保留的结果已达上限（64 个或 64 MiB），每个结果 10 分钟后释放。降低查询频率，或减小结果（更少的行或列）。

**查询很慢。**
每条查询都在新进程中打开数据库，服务端耗时中位数约 4–6 秒；同一空间的查询依次执行。写入高峰期间分析查询会更慢，可以调大 `timeout_ms` 或错开高峰。

**写入、接入或流水线导入返回 503 WRITER_DATABASE_LIMIT。**
一个写者进程最多打开 2 个空间的数据库，当前空间是第三个。由运维重启写者（先关闭再开放），或为更多空间部署独立的环境，见[安装部署手册](installation.md#7-开通空间与首个管理员-key)。

**发布流水线时校验失败。**
按诊断的 `path` 定位到具体任务，常见原因：

- 数据集未注册（`PIPELINE_DATASET_NOT_REGISTERED`）；
- 目标的 `schema`、`key_columns`、`partition_columns` 与 `output` 不完全一致，或目标不是 `write_owner: pipeline`（`PIPELINE_TARGET_SCHEMA_OR_OWNER_MISMATCH`）；
- 运行环境未 `READY`，或没有匹配的执行器 / profile 绑定；
- 处理模式、写入方式和 `selection` 的组合不被允许（`INVALID_DEFINITION`）。

**流水线运行一直是 QUEUED。**
查看 `waiting.code`：`PIPELINE_RUN_ACTIVE` 表示同一流水线已有运行；`SPACE_RUN_QUOTA` 表示空间的活动运行配额已满；`WRITER_AUTHORITY_UNAVAILABLE` 表示写者不可用。

**Lambda 返回成功了，但任务没有发布。**
计算结束不等于发布。平台还要校验产物，写者分块导入后才发布；校验失败的尝试不会发布。查看任务的 `status`、`error_code` 和 `attempts`，或在 Admin Console 的诊断页按 `run` 追踪。

**SQL 任务以 SQL_SCHEMA_MISMATCH 失败。**
结果列的名称、顺序和类型必须与 `output.schema` 完全一致。用 `CAST` 写出声明的类型，例如 `SUM` 的结果要 `CAST(... AS DECIMAL(38,2))`，`COUNT` 要 `CAST(... AS BIGINT)`。

**流水线任务以 PARQUET_ROWS_QUOTA 失败。**
单个输入超过 100,000 行。改用增量处理（只读取新提交的区间），或按分区重算，让每次任务读取的行数变少。

**运行环境构建失败。**
查看版本详情的 `failure`。`DEPENDENCY_NOT_FOUND` 表示某些依赖在 PyPI 上没有 CPython 3.12 / `linux_arm64` 的二进制 wheel；换用有 wheel 的版本，并用新的 `version` 重新创建。`ENVIRONMENT_IMPORT_CHECK_FAILED` 表示镜像中的导入、合同或 SQL 任务检查失败，`failure.message` 给出检查日志的末尾；SQL 环境中常见的原因是 `duckdb` 版本低于 1.1。

**升级后流水线报 PIPELINE_VERSION_BINDING_CHANGED。**
平台破坏性升级后，旧版本的绑定失效。用当前草稿重新发布并激活。空间重建后还需要重新注册数据集、构建运行环境。

**Key 丢失或泄露。**
token 无法找回。签发新 Key 后立即吊销旧 Key；需要保留原 principal 对已有工作的所有权时，用轮换代替签发。

DuckDB is a trademark of the DuckDB Foundation. Colvenna Isle is independent of the DuckDB Foundation and is not endorsed by it.
