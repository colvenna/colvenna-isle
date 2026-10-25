# Quickstart：在已安装环境中运行 API / SQL / Python 示例

本示例在一个已安装、已开放的环境（见[安装部署手册](../../docs/installation.md)）中，通过公开 API 走完一次完整流程，并核对结果。

## 前置

1. 运维为新空间开通并签发首个管理员 Key（只能签发一次）：

   ```bash
   COLVENNA_ENVIRONMENT=<env> scripts/aws/control.py provision --space <space> --output <dir>
   COLVENNA_ENVIRONMENT=<env> scripts/aws/control.py bootstrap-key --space <space> \
     --expires-at 2026-12-31T00:00:00Z --token-output <private-file> --output <dir>
   ```

2. 读取 API 地址：基线栈输出 `ApiUrl`。

## 运行

```bash
python3 -m pip install PyYAML==6.0.1
python3 examples/quickstart/quickstart.py --api-url <ApiUrl> --space <space> --admin-token-file <private-file>
```

脚本依次执行：

1. 签发 developer / ingest / writer / analyst 四把角色 Key。
2. 注册 [orders 示例](../orders/README.md) 的 `ods.orders`、四个流水线输出数据集，以及直接写表 `dwd.accounts`。
3. 构建 `pandas-demo`（pandas）与 `sql-demo`（DuckDB）两个运行环境，二者都是锁定依赖、ARM64。
4. 发布并激活 `examples/orders/pipeline.yaml`，其中包含 writer SQL、Lambda pandas、ECS SQL 和 writer SQL 四个任务。调度改为 `owner: none`。
5. 写入 7 个事件，覆盖同一订单的新旧版本、退款和删除；然后运行流水线，直到 PUBLISHED。
6. 查询发布后的 ADS 看板，同步写入一行 `dwd.accounts`，并用 `after_write` 读回。

每个请求的状态都会打印出来。结果与下表一致时，脚本输出 `PASS` 并以 0 退出：

| order_date | revenue | order_count | high_value_count |
| --- | --- | --- | --- |
| 2026-10-14 | 610.00 | 3 | 1 |
| 2026-10-15 | 50.00 | 1 | 0 |

运行环境构建通常需要 5–10 分钟。脚本不写入任何凭证文件，签发的 Key 只保存在进程内存中。

DuckDB is a trademark of the DuckDB Foundation. Colvenna Isle is independent of the DuckDB Foundation and is not endorsed by it.
