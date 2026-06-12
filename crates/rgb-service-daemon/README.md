# rgb-service-daemon

HTTP daemon for BiHelix RGB service APIs.

This daemon exposes the `rgb-service-api` Axum router. It requires explicit
configuration for service bind address, Bitcoin network, data directory, and
Bitcoin indexer endpoint. Missing required config must fail loudly.

## 启动

Example config: `examples/rgb-service.toml`

```toml
[service]
bind = "127.0.0.1:8787"
network = "regtest"
data_dir = "/tmp/bihelix-rgb-service"
esplora_url = "http://127.0.0.1:3002"

[iroh]
secret_key_hex = ""
```



Daemon logs are written to:

```text
<service.data_dir>/rgb-service.log
```

When `[iroh]` is configured, the daemon derives the iroh `node_id` from
`secret_key_hex` and writes both `node_id` and the current endpoint address to
this log file at startup.

Run:

```bash
cargo run -p rgb-service-daemon -- examples/rgb-service.toml
```

`esplora_url` 是 BTC 链查询后端，用来检查 anchor tx、outpoint、confirmation 和 recovery
相关链上状态。它不是 RGB 数据源，也不代表 service 托管 BTC 私钥。

## Public HTTP API

Current public daemon routes:

```text
POST /v1/iroh-nodes/register    # 注册 BTC 地址对应的 iroh node_id
POST /v1/iroh-nodes/lookup      # 用签名身份根据 BTC 地址查询 iroh node_id
POST /v1/assets/issue          # 发行 RGB20 资产
POST /v1/assets/list           # 查询 account 下资产列表
POST /v1/balance               # 查询资产汇总余额
POST /v1/balance/breakdown     # 查询 allocation / pending 明细
POST /v1/invoices/create       # 创建 RGB 收款 invoice
POST /v1/transfers/prepare     # 准备 RGB 转账，返回带 RGB commitment 的 anchor PSBT
POST /v1/transfers/commit      # BTC 广播后提交 txid，推进 RGB pending 状态
POST /v1/consignments/send     # 构建并传输 RGB consignment
POST /v1/consignments/receive  # 接收外部 RGB consignment
POST /v1/transfers/cancel      # 取消尚未完成的 transfer
POST /v1/pending/list          # 查询 pending operations
POST /v1/recover               # 恢复/推进 pending operations
POST /v1/test/rgb              # 受控测试环境触发 RGB lifecycle 测试
```

所有请求都使用 `SignedRequest<T>`。`prepare` 和 `commit` 还需要
`AssetSpendAuthorization`。public daemon 不暴露 raw fascia，也不提供任意 raw consignment 下载。
consignment 通过 `/v1/consignments/send` 和 `/v1/consignments/receive` 做受控传输。
`/v1/consignments/send` 支持 `service_inbox`、`iroh` 和 `inline` transport。请求 `iroh` 时，daemon 必须配置 `[iroh].secret_key_hex` 并启动真实 iroh endpoint。

详细中文 API 文档见仓库根目录 `README.md` 的 `Public HTTP API 中文说明`。
