# rgb-service-api

Shared API layer for BiHelix RGB services.

This crate defines the stable boundary for RGB asset, balance, direct transfer,
and service-owned LN/RGB transition operations. It also defines the
authorization model used by HTTP services and SDK callers.

Mutating asset operations require signed requests. Operations that move or lock
RGB value additionally require an explicit asset spend authorization from the
user wallet or signer.

## 中文接口边界

`rgb-service-api` 是 RGB Service 的公共调用边界，不是 BTC 钱包，也不是 LN node。

它负责表达：

- RGB 资产发行
- RGB 资产列表
- L1/L2 余额查询
- allocation 明细
- RGB transfer prepare/commit/cancel
- direct send：commit 阶段直接把 consignment 暂存到接收方 account
- 外部 L1 RGB invoice 收发、操作状态与恢复请求
- daemon 内部 pending/recovery 状态的数据结构
- 受控 RGB lifecycle 测试

它不公开表达：

- 普通 L1 pending operation 查询
- 普通 L1 recover 推进
- raw fascia 下载
- 任意原始 consignment 上传接口（外部收发由 daemon 管理证明交付）
- 任意 raw consignment 下载
- RGB stock 直接导入/导出
- BTC 私钥托管
- 通用 BTC 交易广播（外部 RGB finalize 仅广播已校验并授权的确切 carrier）

## Public daemon routes

```text
POST /v1/rna/balance
POST /v1/assets/issue
POST /v1/assets/list
POST /v1/assets/by-utxo
GET  /v1/tokens/list
POST /v1/balance
POST /v1/balance/breakdown
POST /v1/transfers/prepare
POST /v1/transfers/commit
POST /v1/external/receives/create
POST /v1/external/transfers/prepare
POST /v1/external/transfers/finalize
POST /v1/external/operations/get
POST /v1/external/operations/list
POST /v1/external/operations/refresh
POST /v1/external/operations/cancel
POST /v1/ln/channels/open/prepare
POST /v1/ln/channels/funding-ref
POST /v1/ln/commitments/compose
POST /v1/ln/closing/compose
POST /v1/ln/onchain-claims/compose
POST /v1/ln/payments/claim
POST /v1/ln/recover
POST /v1/transfers/cancel
POST /v1/test/rgb
```

`POST /v1/assets/by-utxo` 是只读接口，直接接收 JSON，不要求请求签名，也不扣 RNA。
单个请求包含 `account_id`、`outpoint`、可选 `address` 和 `confirmed`，响应为单个对象；
也可以提交由相同请求对象组成的 JSON 数组，响应为保持输入顺序的对象数组。响应包含
daemon stock 中对应 outpoint 的 `assets` 元数据及 `allocations`。该接口只查询已经由
daemon 接受并验证的 RGB 状态，不导入 consignment，也不根据 BTC dust 猜测 RGB 入账。

普通 L1 RGB pending/recovery 由 daemon 后台 scanner 自动推进，不暴露
`/v1/pending/list` 或 `/v1/recover` 给客户端。

详细请求字段、响应字段、权限和签名说明见仓库根目录 `README.md`。

外部端点使用 `SignedRequest<ExternalRgbRequest>`，签名 purpose 为
`external_rgb`，账户公钥须证明 P2WPKH/BIP86 地址归属。prepare/finalize
另需匹配 invoice、金额和 PSBT 的资产授权。完整实际调用合约及兼容边界见
[外部 RGB API](../../docs/EXTERNAL-RGB-API.zh-CN.md#8-实现接口以本节为调用合约)。
