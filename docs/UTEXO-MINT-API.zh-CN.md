# UTEXO Mint 入金与承兑实施方案

日期：2026-09-17。分支：`feat/utexo-signet-integration`。
状态：用户已要求纳入交付；本文件将公共接口与持久化设计具体化，待确认后实施。当前尚未实现 Mint API，也未完成真实承兑。

## 目标与边界

交付 EVM USDT → Mint → BiHelix RGB 入金，以及 BiHelix RGB → Mint → EVM USDT 承兑退出。沿用现有外部 L1 收发与客户端签名，Mint 订单模块只负责跨链业务编排。UTEXO 负责发行、储备和释放资产；BiHelix 不自行发行替代 USDT。

原 L1 HTTP 互操作继续独立验收。完成 Faucet USDT 转账，不代表该合约可以兑换真实 EVM USDT。Mint 路线必须单独核验 RGB contract ID、EVM token contract、精度和流动性。

## 官方环境核验

2026-09-17，从本机和 `bihelix-aws` 对文档指定地址执行只读请求：

```text
GET https://transfer.gateway.dev.utexo.com/api/v0/networks
HTTP 403
Forbidden
```

目前只能确认该请求受拒，不能据此判断是访问控制、网关配置还是服务变更。没有创建 Mint 订单或发生资金操作。需 UTEXO 提供可访问的联调入口及必要访问条件。

[Getting Started](https://docs.utexo.com/mint/getting-started) 目前列出的已测试路线是 Arbitrum 主网与 UTEXO signet；不能当作两端均为测试币的环境。网络与币种以实时发现结果为准，不能把文档示例 ID 写死为可用路线。

[Mint API](https://docs.utexo.com/product-suite/mint-api-reference) 提供网络/币种查询、估价、预登记、付款确认和历史查询。RGB 出金的付款对象来自预登记响应中的 invoice。接口尚未给出可直接依赖的创建幂等键及完整退款协议，因此创建结果不明时不能盲目重发，失败也不能直接标记已退款。

## 建议实现结构

在 daemon 增加独立 `mint` 模块和默认关闭的 `[service.mint]` 配置，复用 Fjall 与后台任务框架。配置固定上游 HTTPS 允许列表、明确批准的资产/网络映射、查询间隔与请求限额。现有 RGB 核心、余额证明和输入预留仍由外部 L1 模块负责。

新增签名域 `mint`，请求沿用 `SignedRequest`，包含 `account_id` 与带类型的 action；签名覆盖完整参数，账户归属检查沿用外部 API。涉及 EVM 地址所有权的上游签名由客户端提供，不能把 BTC 请求签名误作 EVM 所有权证明。

拟新增公共接口如下，均为 POST：

| 路径 | 主要输入 | 返回 |
| --- | --- | --- |
| `/v1/mint/capabilities` | account_id | 实时路线、资产映射、可用性与核验时间 |
| `/v1/mint/quote` | route_id、direction、amount、destination | quote_id、两端金额、费用及有效条件 |
| `/v1/mint/orders/create` | request_id、quote_id、接收 operation_id 或目标地址、费用/最小到账约束 | order_id、付款要求、当前状态 |
| `/v1/mint/orders/confirm-source` | request_id、order_id、源交易或 external operation_id、上游鉴权材料 | 验证结果与订单状态 |
| `/v1/mint/orders/get` | order_id | 本地与上游状态、两端证据及错误 |
| `/v1/mint/orders/list` | cursor、limit | 本账户订单分页 |
| `/v1/mint/orders/refresh` | order_id | 排队核查已存订单，不新建或重新付款 |

金额使用最小单位十进制字符串并做有界整数解析；不同链的精度单独转换。不使用浮点，不按 ticker 映射，不把上游异常默认解析成零费用。报价存快照；创建响应发生价格变化时重新检查最大源支出与最小到账约束，未通过则不进入付款步骤。

暂不提供自动取消/退款接口：须先拿到 UTEXO 的可验证协议，才能承诺相应行为。返回未知上游状态时保留原值并进入人工核查，不能当作成功。

## 两个方向的处理

入金：取得报价 → 按净到账金额准备 daemon 接收意图 → 预登记 Mint → 核对响应金额、接收 invoice、链 ID 与目标合约 → 钱包审阅并签署 EVM 交易 → 核验源链交易后通知上游 → 追踪 RGB 入账。

这里存在需要联调确认的报价与 invoice 依赖：当前 daemon 要求接收金额精确匹配，而 Mint 可能在登记时更新费用。如果净额变化，必须停止付款并重新确认接收意图；不能直接放宽 RGB 收款校验。

出金：创建订单并验证 Mint invoice 的真实 contract ID、网络、金额、有效期和代理 → 使用既有 `/v1/external/transfers/prepare` 与 `finalize` 完成付款 → 绑定同账户实际 operation → 从已保存交易状态生成付款通知 → 验证 EVM 目标到账。不能只凭客户端传入 txid 或上游 `FINISHED` 宣称承兑成功。

客户端继续持有并使用 BTC/EVM 私钥。daemon 不接收 EVM 私钥，不自动做代币无限授权；返回给钱包的交易要求必须能审阅链、合约、金额和费用。

## 持久化与恢复

拟新增独立 keyspace：`mint_orders_v1`、`mint_idempotency_v1`、`mint_quotes_v1`。记录包含 schema_version、账户、语义请求摘要、报价快照、route/asset 映射、transferId、外部收发 operation_id、两端交易标识、原始状态和核验证据。

状态建议：`quoted` → `registering` → `awaiting_payment` → `source_pending` → `destination_pending` → `completed`；另有 `registration_unknown`、`failed`、`needs_review`。退款单独记录 `refund_pending` 与 `refunded`，后者必须有到账证据。

先保存创建意图，再调用上游。若上游可能已登记但响应丢失，保存 `registration_unknown`，通过可信历史证据找回 transferId；在不能唯一匹配时停止自动创建。进程重启不能产生第二笔付款。同 request_id 同业务参数返回原订单，异参返回 409。

订单不能跨账户绑定 external operation，也不能把同一付款重复绑定多个订单。保存上游通知任务并重试同一 transferId；不因查询超时重新广播。持久化鉴权材料最小化保存，签名 URL 与鉴权字段不进入日志。

最终完成要求：源链付款经核验、Mint 状态一致、目标资产实际到账。EVM 侧核验成功 receipt、指定 token 的 Transfer 日志、接收地址、金额与确认；RGB 侧必须关联 daemon 已结算接收记录。若发生重组或证据冲突则隔离订单并保留调查信息。

## 实施及验收顺序

1. 确认本文件的公共接口与持久化边界，完成类型、上游适配、精确金额转换和禁用配置。
2. 用本地模拟服务验证成功、创建响应丢失、重复请求、错账户/错合约/错金额、报价变化、重启恢复、FAILED 无退款证据等路径。
3. 上游访问恢复后保存真实网络/币种/报价响应作为兼容样例，核对 RGB 鉴权字段与 invoice 规则。
4. 指定资金来源、EVM 接收地址、单笔上限及手续费上限后，执行小额双向承兑。若使用真实 EVM 资产，付款前提供具体交易供用户审阅。
5. 验收 Mint 入金 → BiHelix A → BiHelix B → Mint 出金 → EVM 到账；报告费用、交易、资产映射与恢复结果，区分模拟通过和真实通过。

## 待 UTEXO 确认

- 开发网关 `/networks` 返回 403，是否需要白名单、凭证或替换入口？
- 当前 Mint 的 RGB 合约是否为 Faucet 合约 `rgb:f~9F4X0C-TiLOTvy-pALF29V-2xJ2p0m-hP3_vpW-Alj4G5Y`？若不同，请提供 genesis、schema、精度及测试资金渠道。
- 是否有两端均无真实价值资产的环境？可用 EVM 链、代币合约、最小金额和出金限额是什么？
- RGB 出金的 sender、publicKey、authenticationSignature 确切格式，以及兼容外部钱包的 invoice 示例是什么？
- 报价到登记的净额变化如何处理？接收 invoice 是否必须指定金额？
- 预登记超时如何唯一找回订单？退款/取消流程和实际到账证据是什么？

以上问题为待发送清单，本次没有代表用户联系 UTEXO。
