# UTEXO 互操作实测记录

日期：2026-09-16，Asia/Shanghai。分支：`feat/utexo-signet-integration`。

## 结果与范围

通用 NIA 测试资产完成 UTEXO SDK → BiHelix 核心 → UTEXO SDK 的真实链上往返；官方 RGB USDT 尚未获得，未验收。测试通过 `rgb-service-local` 独立 harness 完成，不等于 daemon 已实现公共外部收发 HTTP API，也没有完成 BiHelix A → B、故障恢复或 Mint 承兑验收。

对照版本：Node 22.23.2、`@utexo/rgb-sdk@1.0.0-beta.8`、`@utexo/rgb-lib@0.3.0-beta.13`。网络为 SDK 的 `utexo` 自定义 signet；Esplora 为 https://esplora-api.utexo.com，proxy 为 https://rgb-proxy.utexo.com/json-rpc。

对照钱包在 AWS `reference/state/reference-a`，BiHelix 成功实验在 `interop-stock-bob`，均位于 `/home/ubuntu/utexo-integration`；与运行中 daemon 的 `service-data` 隔离。各自保留自己的钱包状态和 consignment。私钥、助记词和钱包数据库未进入 Git。迁移前本地对照钱包快照已过时，不得用于花费。

## 成功往返证据

测试资产 BHXTEST，precision=0，发行量 1000；不是 USDT。

- Contract：`rgb:LD5JyIwl-UJQCAdg-YRoXiaa-2Q8yV8V-Ry01N26-bNcbx40`。
- Schema：`rgb:sch:RWhwUfTMpuP2Zfx1~j4nswCANGeJrYOqDcKelaMV4zU#remote-digital-pegasus`。
- SDK → BiHelix 100：`1e4d4212d58e2b9a277a37f931ed6448eb2a24d3a9a8a7bcc545e2a420c91049`，区块 627939，收款 vout=1。
- BiHelix → SDK 40：`33dc625eae2f23c90670f1b425cfd4fe8ecf06d9715ad33195b254b95c2ec500`，区块 627941，收款 vout=1，找零 vout=2。
- BiHelix 完整校验来款证明、接纳并落盘；返还确认后 scanner 返回 `amounts=[60], promoted=1`。
- 返还 proxy ACK=true；SDK 最终 transfer 状态 `Settled`，settled/future/spendable 均为 940。金额守恒：940+60=1000。

公开证明、PSBT、invoice 和哈希清单在 `tests/fixtures/utexo/`；最终 SDK/链上查询快照为 `roundtrip-public-evidence.json`。初次刷新曾返回临时 future=980；再次同步后全部收敛到 940，验收采用最终状态。

## 找到并修复的兼容问题

首次测试把 Taproot 收款放在 OP_RETURN 前，UTEXO 拒绝，错误为：

`first DBC-compatible output of witness transaction ... doesn't match the provided proof type (opret1st)`

该失败交易是 `dcfcfc05dc35a9ce6f22924c63ec361008a9c74391652882038a9b721ab3c35c`，proxy ACK=false、SDK Failed。失败实验使用另一测试合约，状态保留供排查，不继续花费。

已有 BiHelix 共识验证允许这种顺序，因此没有修改既有共识兼容策略。新增外部构造预检：要求唯一 OP_RETURN，且位于所有 Taproot 输出之前；harness 按 OP_RETURN=0、收款=1、找零=2 构造，再签名广播。正确布局已获对端接纳。

AWS 上执行 `cargo test --locked --release -p rgb-service-local --test utexo_fixtures`：4 项通过，覆盖真实 proof 编解码/损坏拒绝、blind/witness invoice、失败与成功载体布局、往返同合约证明链。离线测试不能代替官方 USDT 验收。

## 官方 USDT Faucet 操作与错误

使用官方 SDK sandbox 指向的 Telegram `@Utexo_RLN_bot`。BTC Faucet 已成功发放 50,000 sats；随后调用 `/getnodeinfo` 确认 Bot 宣称的 USDT 资产，但尚未取得其 genesis/consignment，不能仅凭 ticker 认定兼容。

1. 使用官方 SDK 在 `utexo` 网络创建 blind receive invoice，minConfirmations=1、有效期 24 小时、transport 为上述官方 proxy。最初请求数量为 42 个原始单位，资产字段留空以接收 Bot 配置的资产。
2. 向 Bot 发送 `/getasset`，按提示提交 invoice。Bot 提示发送 `1000000 USDT`，随后报错：`Could not complete the request against the RGB LN node. Try again later.`
3. 第二次重试相同 invoice，出现相同提示和错误。
4. 为排除请求数量差异，重新生成请求数量为 1000000 原始单位的 blind invoice，再走 `/getasset`。第三次仍报同一错误。Bot 的显示单位不构成已核实的资产精度。
5. 对新 recipient 查询 proxy `consignment.get`，返回 error code `-400`、`Consignment file not found`；SDK 刷新后资产列表只有自建 BHXTEST，没有官方 USDT。

新 invoice 保存于 [USDT-RECEIVE-INVOICE.txt](USDT-RECEIVE-INVOICE.txt)，到期时间 2026-09-17 12:05:40（Asia/Shanghai）。保留旧接收状态，以免遗漏延迟付款；到期后应重新生成，不直接复用。

结论：目前观察到的是 Bot 对 RGB LN 节点请求失败，且本方未拿到证明或余额。无法从这条通用错误断言是对方余额不足、通道故障、节点不可用还是 invoice 校验问题；需要对方服务端日志。通用 NIA 往返成功说明所测 SDK/proxy/核心链路可用，但不能证明 Faucet 内部链路或 USDT 合约兼容。

另一路只读探测：本地与 AWS 请求 `https://transfer.gateway.dev.utexo.com/api/v0/networks` 均返回 HTTP 403。该现象属于 Mint API 访问受限，与 Bot 错误分别记录；没有创建 Mint 订单或支付真实 EVM 资金。

## 接续条件

请 UTEXO 恢复测试 USDT Faucet，或提供指定测试 USDT 的权威 contract/genesis/schema 并向上述有效 invoice 发放测试资产。取得后核实合约身份/精度，完整验证证明、确认入账，执行同合约返还并核对双方已结算余额与 ACK。然后按计划实现 daemon 外部收发的持久化、幂等、恢复和公开接口；仍须执行 A → B 与异常测试。

供转发给 UTEXO 的排查信息（尚未发送群消息）：官方 Bot 三次 `/getasset` 均在提示发放 1000000 USDT 后返回上述 RGB LN node 错误；其中最后一次 invoice 请求值已为 1000000。BTC Faucet 正常、相同环境通用 NIA 双向交易正常；请检查 Bot 对应请求日志并确认测试资产身份及发放通道。Mint networks 另返回 403，请说明访问条件。
