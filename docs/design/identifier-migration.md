# Colvenna 标识迁移（2.0.0）

本设计对应 [Issue #22](https://github.com/colvenna/colvenna-isle/issues/22)。当前候选为 `2.0.0.dev0`，尚未发行。采用大版本破坏性迁移，不提供双名称过渡期。

| 接口 | 当前名称与处理 |
| --- | --- |
| Python 发行包 / 导入 | `colvenna-isle` / `colvenna_isle`；旧模块不能导入 |
| 服务与运维配置 | `COLVENNA_*`；旧配置不读取，必需配置缺失时按当前工具的预检规则拒绝，账号仍可通过 STS 获取 |
| HTTP 身份与诊断头 | `x-colvenna-*`；Gateway 覆盖身份头，查询服务独立核对 Key，旧身份头不能授权 |
| SQL / Parquet 系统列 | `_colvenna_*`；流水线定义、SQL、代码、context 和输出 manifest 一起重新发布 |
| 契约 | `contract_version=2`，生成文件在 `contracts/v2/`；旧 revision 拒绝。公开 API 路由仍为 `/v1/`，路由版本与文档契约 revision 分开 |
| 原生数据库 | `ctl.space_identity.format_version=6`；格式 5 拒绝打开为当前空间，读写与备份检查同一 identity |
| 原生镜像 | `/opt/colvenna`；重新打包固定上游二进制，镜像 ID / digest 更新，不修改 DuckDB 与扩展版本 |
| AWS 资源 | `colvenna-isle-*`，保持 Issue #19 的环境 / 区域隔离与认证要求 |

旧发行接口的原样记录保存在[历史标识快照](../history/v1.1.0-identifiers.json)。历史发行说明、tag 和 Release 不重写。现行代码没有兼容别名。

## 已有数据的选择

采用**业务数据导出与重放**，不在运行中的旧库里执行列改名，不将旧原生备份直接还原成格式 6。旧备份仅用于同版本恢复或升级失败后的回退。

1. 停止外部生产者、外部调度与内部调度；核对所有写入 / 接入 / 流水线到终态，UNKNOWN 按原身份解决。设置保留锁，避免迁移期间回收事件。
2. 使用旧 tag 的 `backup.py create` 生成一致性备份，再用同一版本的 `backup.py inspect` 核对备份及其引用。旧服务仍运行供备份与核对；不要提前调用会停掉写者的 `close`。原生备份的 ODS Parquet 含完整事件历史、删除 tombstone、操作和提交序号；公开查询仅暴露业务列，不能作为这一迁移的输入。
3. 从旧备份回执的 `manifest.native` 引用下载精确 `bucket` / `key` / `version_id` 的原生 manifest，保留引用中的 SHA-256。再下载所选 ODS 表的全部 `tables[].files[]` 精确对象版本到私有目录，文件名为 `<sha256>.parquet`。不执行 manifest 中的 DDL，不连接原数据库。
4. 生成新空间的重放计划；工具核对 manifest SHA-256、每个 Parquet 的大小和 SHA-256、列顺序与类型、总行数、操作和业务值，再按提交序号与事件 ID 排序。Decimal、BLOB 和时间转换为合同精确编码，JSON 文本解码为原 JSON 值。每个事件一个请求，避免混合 upsert / delete；稳定幂等键写入计划，丢失回复后沿用原键核对。
5. 从同一备份 Parquet 导出 direct 表的业务行并核对类型、业务键与行数。使用旧 tag 的工具关闭并卸载旧运行时，保留历史备份库与导出文件。用新源码全新安装，重新注册数据集、签发 Key 和构建运行环境。
6. 按计划顺序重放 ODS 请求。direct 表按外键依赖顺序通过 `/writes` insert 业务行。流水线拥有的 DWD / DWS / ADS 与键状态通过新 SQL / Python 全量重算或 bootstrap 重建，不导入历史内部表。
7. 核对 ODS 总行数、业务值、最高源版本、tombstone、direct 表约束与行数、下游重算结果，再恢复生产者与调度。

原生系统列由新写者重新生成；旧接入 / 写入 / 查询 ID、commit sequence、接收时间、权限、审计、发布 release 与流水线状态不沿用。跨版本的请求不能用旧回执执行 `after_write` / `after_ingestion`，必须使用新回执。

## 离线 ODS 重放工具

在新的锁定构建环境中执行，输入文件只保存在受保护的本地目录：

```bash
SOURCE_PREFIX=$(python3 -c 'import json; print(json.load(open("docs/history/v1.1.0-identifiers.json"))["system_prefix"])')
PYTHONPATH=src /tmp/colvenna-build/bin/python -m colvenna_isle.migration \
  --definition /secure/ods-definition.json --native-manifest /secure/native.json \
  --manifest-sha256 <旧回执的-native-sha256> --parts-directory /secure/ods-parts \
  --source-system-prefix "$SOURCE_PREFIX" --output /secure/replay.json
```

`--definition` 是新版本的 `dataset_registration`，业务 schema 与旧空间一致；迁移工具使用锁定的 PyArrow，需安装 orders 依赖 lock。原生 Parquet 包含全部业务列和历史 `ingestion_id` / `commit_sequence` / `operation` / `disposition` / `received_at` 系统列。工具不会连接 AWS、发送请求、执行备份 DDL 或修改原库；输出以新文件、权限 `0600` 创建，重复路径拒绝。对象校验失败或无法精确编码的值拒绝迁移。SQL JSON null 字面值不能通过接入合同表示为 SQL NULL，遇到时拒绝，需人工制定转换；正常接入产生的 nullable JSON 空值保持 SQL NULL。

输出的 `batches[]` 含 `idempotency_key` 与 `body`。将每个 body 发送到新空间的 `POST /v1/spaces/{space_id}/ingestions`，设置 `Authorization` 和该 `Idempotency-Key`；核对 `COMMITTED` 后再处理下一个请求。不要在 UNKNOWN 时换键重发。计划的 PASS 仅表示离线输入校验通过，不表示已完成数据迁移。

## 验证边界

本地测试覆盖新包、配置、身份头和契约，以及旧格式拒绝；原生测试覆盖精确金额、删除 tombstone、迟到旧版本、原幂等键重试和新系统列。安装脚本、模板、Lambda ZIP、写者 / 查询镜像、示例和浏览器分别验证。AWS 全新安装和升级数据验收必须单独记录，未实际执行时为 `NOT_RUN`；历史 1.1.0 PASS 不计入本候选。
