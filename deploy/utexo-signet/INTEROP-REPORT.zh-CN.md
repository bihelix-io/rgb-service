# UTEXO 互操作实测记录

初次测试：2026-09-16；最新重试：2026-09-17，Asia/Shanghai。最新状态见文末。分支：`feat/utexo-signet-integration`。

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
2. 向 Bot 发送 `/getasset`，按提示提交 invoice。Bot 提示发送 `100000000 USDT`，随后报错：`Could not complete the request against the RGB LN node. Try again later.`
3. 第二次重试相同 invoice，出现相同提示和错误。
4. 当时误读 Bot 显示数量，重新生成请求数量为 1000000 原始单位的 blind invoice（2026-09-17 复核原文，Bot 实际显示 100000000，因此这次未做到数量匹配），再走 `/getasset`。第三次仍报同一错误。Bot 的显示单位不构成已核实的资产精度。
5. 对新 recipient 查询 proxy `consignment.get`，返回 error code `-400`、`Consignment file not found`；SDK 刷新后资产列表只有自建 BHXTEST，没有官方 USDT。

新 invoice 保存于 [USDT-RECEIVE-INVOICE.txt](USDT-RECEIVE-INVOICE.txt)，到期时间 2026-09-17 12:05:40（Asia/Shanghai）。保留旧接收状态，以免遗漏延迟付款；到期后应重新生成，不直接复用。

结论：目前观察到的是 Bot 对 RGB LN 节点请求失败，且本方未拿到证明或余额。无法从这条通用错误断言是对方余额不足、通道故障、节点不可用还是 invoice 校验问题；需要对方服务端日志。通用 NIA 往返成功说明所测 SDK/proxy/核心链路可用，但不能证明 Faucet 内部链路或 USDT 合约兼容。

另一路只读探测：本地与 AWS 请求 `https://transfer.gateway.dev.utexo.com/api/v0/networks` 均返回 HTTP 403。该现象属于 Mint API 访问受限，与 Bot 错误分别记录；没有创建 Mint 订单或支付真实 EVM 资金。

## 接续条件

请 UTEXO 恢复测试 USDT Faucet，或提供指定测试 USDT 的权威 contract/genesis/schema 并向上述有效 invoice 发放测试资产。取得后核实合约身份/精度，完整验证证明、确认入账，执行同合约返还并核对双方已结算余额与 ACK。然后按计划实现 daemon 外部收发的持久化、幂等、恢复和公开接口；仍须执行 A → B 与异常测试。

供转发给 UTEXO 的排查信息（尚未发送群消息）：官方 Bot 三次 `/getasset` 均在提示发放 100000000 USDT 后返回上述 RGB LN node 错误；其中最后一次 invoice 请求值已为 1000000。BTC Faucet 正常、相同环境通用 NIA 双向交易正常；请检查 Bot 对应请求日志并确认测试资产身份及发放通道。Mint networks 另返回 403，请说明访问条件。

## 2026-09-17 重试与申请渠道

北京时间 10:04，再次向同一官方 Bot 提交尚未到期的 1000000 原始单位 invoice。这次 Bot 宣告成功，显示发送 **100000000 USDT**。此前说明把 Bot 显示值误记为 1000000，现按聊天原文更正；显示值尚不等于已核实的到账数量/精度。

- Faucet TX：`067078c9bdb413e252c06ae4cba69e223793afb7a87d9ccd70fe91069943fb58`，确认高度 630486。
- proxy 下载到 71705 字节 consignment，SHA256 `01e8f55af34f277b375b44ffcdf2038720222057bbefdf61d56f71ab7bb09c87`。
- BiHelix 核心对该证明解码及链上完整验证通过，包含 104 bundles；schema 为 `rgb:sch:IpjJhFLz3oywYKQxO3KmFgR0Aa415nlTNrNyEFqMZCE#shoe-colombo-mango`，与自建测试 NIA 不同。
- proxy ACK 查询为 true，但 SDK 连续两次刷新仍为 `WaitingCounterparty`，尚未列出 USDT 余额；不能将 Bot 成功或 ACK 单独视为本地钱包入账/互操作完成。底层 refresh 已确认返回 `UnknownRgbSchema`（详情如下）。

申请渠道核实：

1. 既有 Bihelix-UTEXO 群，9 月 16 日 Gofman 表示可提供 dev bridge access；Matteo 要求集成负责人的邮箱并表示会开通。群里已提供邮箱，Matteo 回复收到。本次仅阅读，没有代用户发送跟进。
2. 官网 [Contact Sales](https://utexo.com/contact-sales) 提供申请表与 `sales@utexo.com`；可用于申请集成支持/测试资源，但页面没有承诺自动发放 RGB USDT。
3. 官网链接的 [Discord](https://discord.com/invite/utexo) 可作为支持入口。
4. [官方 SDK sandbox](https://github.com/UTEXO-Protocol/rgb-sdk-rn-sandbox#virtual-channel--utexo-signet) 提供需 Bearer token 的 Faucet API 配置；所示流程用于测试 BTC，不应当作已证实的 USDT Faucet API。

10:02 本地和 AWS 重试 `GET https://transfer.gateway.dev.utexo.com/api/v0/networks` 均仍为 HTTP 403，body `Forbidden`。权限交付/生效尚未确认；文档称该 GET 无需鉴权，不能将此 403 直接归因为缺少某种指定 API key。

### SDK 接收阻塞已定位

直接采集固定版本 native binding 的 refresh 结果（SDK 高层 `BaseWalletManager.refreshWallet()` 丢弃了该返回值）：

```json
{"6":{"updatedStatus":null,"failure":{"UnknownRgbSchema":{"schema_id":"rgb:sch:IpjJhFLz3oywYKQxO3KmFgR0Aa415nlTNrNyEFqMZCE#shoe-colombo-mango"}}}}
```

因此本次不是 Faucet 发放失败，也不是 BiHelix 共识验证失败；当前 SDK/native 组合不识别 Faucet 发来的 schema，接收仍未结算。暂不能从 proxy 的 `validated=true` / ACK=true 推导本地已入账。需要 UTEXO 确认 Faucet 资产对应的 SDK/native 版本或提供当前 SDK 支持的指定测试 USDT。尚未升级依赖、修改 SDK schema 白名单或开展该 USDT 的返还交易。完整 RGB USDT 双向互操作仍未完成。

可发送的英文补充（未代发）：

> Update: the faucet retry succeeded on Sep 17 at approximately 02:04 UTC. TXID: `067078c9bdb413e252c06ae4cba69e223793afb7a87d9ccd70fe91069943fb58`. We downloaded the consignment and BiHelix core successfully validated it against the chain. However, the native refresh result under `@utexo/rgb-sdk@1.0.0-beta.8` / `@utexo/rgb-lib@0.3.0-beta.13` reports `UnknownRgbSchema` for `rgb:sch:IpjJhFLz3oywYKQxO3KmFgR0Aa415nlTNrNyEFqMZCE#shoe-colombo-mango`. The receive remains `WaitingCounterparty` and no USDT balance is listed. Which SDK/native version supports this faucet asset? Also, the dev bridge `/api/v0/networks` endpoint still returns 403 from both environments; could you confirm how access is delivered and enabled for the email already provided? Correction to our previous report: the bot displays 100000000, not 1000000. The submitted invoice requests 1000000 raw units; we have not yet verified the actual received amount or precision.

## 2026-09-17 SDK 版本与源码核对

直接查询 npm registry、GitHub 发布源码，并静态检查 Linux native 发布包，未变更运行中依赖/钱包数据库。

| 组件 | 发布状态 | 底层依赖/结论 |
| --- | --- | --- |
| `@utexo/rgb-sdk` latest | `1.0.0-beta.8`，2026-04-03 | 精确锁定 rgb-lib beta.13 / sdk-core beta.2；当前正在使用 |
| `@utexo/rgb-sdk` beta | `1.0.0-beta.9`，2026-04-09 | 精确锁定 rgb-lib `0.3.0-beta.16.dev` / sdk-core beta.3；仅切 beta 不足以匹配本次 IFA |
| `@utexo/rgb-lib` latest | `0.3.0-beta.18`，2026-04-14 | Linux 平台包于 04-16 发布；其 `.so` 包含旧 IFA schema `p6H_wt...scale-year-shave`，未发现 Faucet 的 `IpjJh...shoe-colombo-mango` 常量 |
| `@utexo/rgb-sdk-rn` latest | `1.0.0-beta.32`，2026-09-14 | 移动端另一发布线，依赖 sdk-core beta.9；不能直接替换 Node SDK |
| `@utexo/rgb-sdk-core` latest | `1.0.0-beta.9`，2026-09-14 | JS 核心更新不等于 Node native schema 支持更新 |

Node SDK 的 [GitHub 仓库](https://github.com/UTEXO-Protocol/rgb-sdk) 已于 2026-07-28 归档。`npm latest` 不代表当前维护中的整个 UTEXO 栈。

Faucet schema 精确对应 rgb-lib 的 `SCHEMA_ID_IFA`（可增发资产）。已逐个检查 Rust 标签 beta.19 至 beta.34：beta.19 为旧 ID；[beta.20 源码](https://github.com/UTEXO-Protocol/rgb-lib/blob/v0.3.0-beta.20/src/wallet/mod.rs)及本次检查的后续标签含当前 ID。当前 dev commit `66e558cef0a562a6234639d0641caea30f4d4382` 也包含该 ID。此为源码识别能力证据，不是这些版本已完成本钱包接收/返还的运行验收。

SDK beta.9 的 Node binding gitHead `adb1c010cbe1d455759ba9bac474db5449589ed7` 指向 rgb-lib 子模块 `e8d8c7a162bb5ecb3bb6edaefe9637be2d1996ed`，其 IFA 常量仍为旧 ID。Node Linux beta.18 发布包独立核查，不能把 Rust 同名标签与 npm 平台包假定为同一构建。

结论：已定位到“Rust 核心支持更新，已发布 Node SDK/native 滞后”。单纯 `npm update` 或改 SDK beta.9 不能据此解决。接续应在隔离测试环境构建支持该 IFA 的 native/参考客户端，先验证 API 与数据库迁移兼容性，再处理现有接收状态；不能只替换 schema 常量或直接让新版打开唯一钱包副本。

发布元数据来源：[SDK](https://registry.npmjs.org/@utexo/rgb-sdk)、[Node native](https://registry.npmjs.org/@utexo/rgb-lib)、[Linux native](https://registry.npmjs.org/@utexo/rgb-lib-linux-x64)、[RN SDK](https://registry.npmjs.org/@utexo/rgb-sdk-rn)。
