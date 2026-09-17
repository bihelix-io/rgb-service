# BiHelix RGB Service × UTEXO 接入计划

日期：2026-09-16。状态：第一步独立 signet 实例已部署并领取测试 BTC；外部 RGB 互操作尚未验证。
初稿分析基线：`fa9fed1`；本次部署源码基线：`71c88087295027de384fd72910c7b33c785947ff`。实际部署与验收见 [部署记录](../deploy/utexo-signet/DEPLOYMENT.zh-CN.md)。

目标：保留 BiHelix daemon 和 SDK，由 daemon 管理用户 RGB 状态、验证和转账，让已有钱包及应用接收、使用并转出 UTEXO 指定的 RGB USDT；另以 Mint 适配层提供跨链进入和退出入口。

## 0. 从 bihelix-aws 开始的执行顺序

第一个部署里程碑是在 bihelix-aws 建立独立的 UTEXO signet 联调实例，现已完成。已检查远端现有服务、端口、磁盘和实际网络；下方保留原规划，部署的具体配置以部署记录为准。

### 第一步：确认网络并部署独立实例（预计 1–2 人日）

- 确认 UTEXO 使用的链身份、Esplora、proxy、测试 BTC 获取方式及推荐 SDK 版本。可以同时准备服务目录和构建产物。
- 先读取 bihelix-aws 现有 systemd/容器配置，沿用其运维方式。实例逻辑名称建议 `rgb-service-utexo-signet`；部署固定 commit 的构建产物并记录校验值。
- 独立配置、数据库、日志、运行身份和测试账户；不导入主网 stock、主网钱包种子或历史迁移数据。资源使用加限额，避免构建和测试影响现网。
- 初期只监听回环地址，通过 SSH 隧道联调；外部钱包需要访问时再配置独立 HTTPS 路由。RGB proof proxy 与 daemon HTTP API 是两个不同入口。
- 初期使用经核验的 UTEXO 链后端，无须先自行运行 Bitcoin Core 全节点。若自行运行节点，必须使用与 UTEXO 相同的 signet 参数并配套索引器。

配置草案如下；占位地址必须替换为经过验证的后端，端口需先检查空闲：

```toml
[service]
bind = "127.0.0.1:18787"
network = "signet"
data_dir = "/var/lib/rgb-service-utexo-signet"
esplora_url = "<已核验的 UTEXO signet Esplora URL>"

[rna]
issue_fee = 1000
transfer_fee = 100
query_fee = 1

[legacy]
enabled = false
```

daemon 当前接受 `network = "signet"`，但它没有 UTEXO 专用网络配置字段。需核验底层 resolver 对实际链的兼容性；检查双方选定高度的块哈希、已知交易及确认情况，不能只比较地址前缀或最新高度。UTEXO SDK 将 `utexo` 和 `signet` 作为不同配置选项暴露。[WDK 配置](https://docs.utexo.com/sdk/wdk-wallet-rgb)

环境验收：进程稳定运行；通过已存在的 catalog API 检查服务响应；已知测试交易/UTXO 在两侧一致；认证测试请求可用；独立数据库重启后保留；现网服务正常。环境启动成功尚不代表已能外部收发 RGB。

### 第二步：建立 UTEXO 对照钱包和协议样例

建立独立 UTEXO SDK 测试钱包、BiHelix A/B 测试账户，获得对应网络的测试 BTC。先让 UTEXO 两个测试账户成功互转一笔专门标记的测试资产，以确认对端环境可用，再采集 invoice、consignment 和传输时序。测试资产通过不等于正式 USDT 合约通过。此步骤纳入 P0。

### 第三步：实现 daemon 外部接收及转出

按 P1/P2 推进，依次验收 UTEXO → BiHelix、BiHelix → UTEXO、UTEXO → BiHelix A → BiHelix B → UTEXO。错误证明、重复投递、重启恢复和并发输入预留必须一并通过。内部 regtest 保留用于可控挖块、故障注入和回归，signet 用于真实对端联调。

### 第四步：接指定 USDT 测试合约及 BitPocket

拿到 UTEXO 指定合约后，重复第三步，校验 schema、精度和业务规则；BitPocket 接入测试实例演示收发。钱包基础收发可以先交付，不必等 Mint。原 P4 的收发部分提前到这里，Mint 界面随下一步完成。

### 第五步：接 Mint 并验证退出

按 P3 完成跨链进入、内部流通、跨链退出及对账。当前指南仍描述 Arbitrum 主网 ↔ UTEXO signet；先确认可用路线和测试资金方案，再安排涉及真实资金的测试。[Mint 环境说明](https://docs.utexo.com/mint/getting-started)

### 第六步：Lightning 与正式发布

Lightning 按 P5 独立验收；主网发布以生产服务开放和本计划上线条件为准。新增环境准备后，首期总工作量约 15–24 人日，外部资料、资金和服务开放等待不计入。

最近可交付的结果应是：一套可复用的 bihelix-aws 联调服务、一个 UTEXO 对照钱包，以及一次可复现的同链 BTC/UTXO 核验记录。随后进入 RGB 互通开发。

## 1. 范围与已知边界

交付分三层：L1 RGB 互操作 → Mint 双向闭环 → RGB Lightning 互操作。每层独立验收，后两层不阻塞基础 L1 支持。

截至本次查阅，Mint 文档仍称 Bitcoin 主网尚未开放，Mint 的 Lightning 目的网络也未开放；Getting Started 标明已测试路线为 Arbitrum 主网与 UTEXO signet。实际开通范围需在联调时再次核验，不能由示例 network ID 推断。该组合可能涉及真实 EVM 资金，不能按全测试币环境处理。[Mint API](https://docs.utexo.com/product-suite/mint-api-reference)、[Mint Getting Started](https://docs.utexo.com/mint/getting-started)

UTEXO Mint 描述了通过 Arbitrum/USDT0 连接 RGB 的锁定、铸造与退出机制；它的承兑系统与 RGB 转账验证是不同职责。BiHelix 不自行重新发行同名 USDT，也不承担 Mint 的储备及资金释放职能。[Mint 机制](https://docs.utexo.com/product-suite/mint)

计划中新增模块、API 和存储结构均为建议，不是已存在的接口。涉及公共接口、持久化及兼容性策略的部分，在实施前完成设计确认；本次仅提交可评审计划。

## 2. 分工

| 组件 | 职责 |
| --- | --- |
| 用户钱包 / 外部签名器 | BTC/EVM 私钥、交易签名、用户资产花费授权 |
| BiHelix daemon | RGB 合约验证、stock、分配、收发状态、证明保存和恢复 |
| BiHelix RGB 互操作模块 | invoice、外部 consignment 收发、transport 与回执适配 |
| BiHelix Mint 适配模块 | 路线发现、报价、订单关联、状态查询与对账 |
| UTEXO 钱包 | 对端 RGB 状态管理；作为联调基准实现 |
| UTEXO Mint | 跨链发行/退出流程及其资金释放机制 |
| RGB proxy | 证明投递与可能的临时存储，不能作为资产唯一备份 |

每个账户的 RGB 状态只有一个负责写入的系统。联调用 UTEXO SDK 使用独立钱包和独立 UTXO，禁止与 daemon 同时管理同一套花费状态。证明交接不等于转交私钥。

## 3. 当前代码与差距

| 位置 | 已有能力 | 本次需要解决 |
| --- | --- | --- |
| `crates/rgb-service-local/src/lib.rs:1142` | consignment 校验及 accept_transfer | 外部证明进入账户的绑定、校验与持久化流程 |
| `crates/rgb-service-local/src/lib.rs:1198` | consignment 编解码 | 与指定 UTEXO 版本的实际文件格式兼容测试 |
| `crates/rgb-ops/invoice/src/lib.rs` | RgbInvoice、Beneficiary、RgbTransport 类型 | 复用底层类型，对接 HTTP 入口和 transport，避免另写一套解析器 |
| `crates/rgb-service-local/src/lib.rs:1119` | 按 recipient_vout 构造 transfer | 支持对方实际 beneficiary 类型，覆盖 blinded seal |
| `crates/rgb-service-local/src/lib.rs:1402` | RGB PSBT 构造 | 核验 OP_RETURN 承诺、`assetOwner` / `transfer` 与正式 schema 匹配 |
| `crates/rgb-service-daemon/src/main.rs:4232` | commit 后暂存接收方状态 | 目前接收方是本 daemon account；增加远端投递分支 |
| `crates/rgb-service-api/src/axum_service.rs` | 余额、prepare/commit 等 HTTP API | 尚无公开外部 invoice 接收、投递状态查询边界 |
| `crates/rgb-service-daemon/src/main.rs:4322` | LN channel/commitment 等实现 | 不能据此认定与 UTEXO LN 网络已经互通 |

另有一个与本次直接相关的缺口：当前 cancel_transfer 返回 Cancelled，但函数体没有实际清理 reservation/pending。外部转账接入时必须核验并实现对应回滚，避免把响应状态当成资源已释放。README 的 LN 501 说明也与当前实现不一致，随相关功能验收更新。

## 4. P0：冻结互操作基线（预计 2–3 人日）

负责人角色：RGB 后端；UTEXO 技术联系人提供确认。

先生成一份兼容性清单，固定以下内容：

1. 对端 SDK/package 精确版本、仓库 commit、底层 RGB/rgb-lib 版本。公开旧链接可能归档或重定向，不能只依赖包名或“0.11.1”标签。
2. UTEXO 指定测试合约、正式合约的权威来源、contract ID、genesis、schema ID、精度、转移规则及可能的增发/销毁/限制规则。
3. Bitcoin 网络实际身份；特别是 UTEXO 自定义 signet 的 challenge、后端索引器和普通 signet 的差别。daemon 的 Signet 枚举支持不等于已连上该网络。
4. transport 协议版本、上传/下载标识、ACK/NACK、重试、过期语义，以及签名、上传、广播的准确顺序。
5. blinded/witness invoice 样例与 consignment 样例，含错误样例。

用离线 fixture 检查解码、schema 和 invoice；用隔离 regtest 或双方认可的无真实资金环境建立最小对端。UTEXO WDK 文档提供 blind/witness 接收及状态查询，可作为联调行为参考，不把示例 NIA 当正式 USDT。[WDK RGB 文档](https://docs.utexo.com/sdk/wdk-wallet-rgb)

**验收：**得到可重复的 fixture 测试报告和确切版本清单；若 schema/共识不兼容，列出差异再评审升级方案，不直接大范围替换 RGB 核心库。正式合约尚未提供时可继续通用联调，但不能标记“USDT 兼容已通过”。

## 5. P1：L1 外部接收（预计 3–5 人日）

负责人角色：RGB 后端，钱包协作。

实现接收意图：账户、网络、contract ID、金额规则、有效期、beneficiary、transport、确认要求；blinded 接收还需持久化 seal secret 及其受控 UTXO 映射。敏感接收数据纳入备份。

流程：

1. 钱包证明接收 UTXO/地址归属，daemon 创建接收意图和 RGB invoice。
2. 对端支付，daemon 按约定 transport 获取 consignment。
3. 核对接收意图、合约、金额、网络和 beneficiary；执行 RGB 验证及必要链上检查。
4. 将证明和接收状态可靠写入存储，按对端协议规定发送回执；不能把广播前的有效证明误当作已确认资产。
5. 后台确认后推进可用余额，登记 account_utxos。查询仍以已验证 stock 为依据。

实现持久化 inbox、按证明摘要/操作身份去重及进程重启恢复。外部下载限制目标地址、重定向、大小、超时，避免 invoice endpoint 访问 daemon 内网；未经验证的证明不进入可用余额。

**验收：**UTEXO → BiHelix，在双方支持的 blind/witness 模式下余额一致；重复投递只记账一次；错误资产、网络、接收方和过期意图不被静默接受。过期后的已发生支付进入显式恢复处置，不直接丢弃。

## 6. P2：L1 外部转出（预计 4–6 人日）

负责人角色：RGB 后端，钱包前端协作。

新增外部 invoice 转账能力，保留原本机 account direct-send 行为。具体使用新增端点或版本化请求结构在实施前确认，不把 invoice 字符串塞进原 account_id。

1. 解析对端 invoice，校验网络、资产、金额、到期时间及 transport 能力。
2. 对选中的 RGB UTXO 做持久化预留；检查同 UTXO 上其他资产的状态保存和找零。
3. 构造 RGB PSBT，由外部钱包签名。花费授权绑定规范化 invoice/接收意图摘要、金额、合约和 PSBT，避免 prepare 后换接收方。
4. 根据 P0 验证的协议执行证明投递、对端确认和广播；必要时增加广播前 finalize 阶段，不能直接套用目前先广播再 commit 的流程。
5. 记录本地 transfer、proof digest、对端投递身份和最终 txid；验证最终签名交易与准备交易一致，再推进确认状态。

持久化 outbox 支持限速重试。分别跟踪“证明已交付”“对端已接受”“交易已广播”“链上已确认”，避免一个 committed 状态掩盖失败位置。超时不代表失败；已广播后禁止自动解锁原输入或重新支付。

**验收：**UTEXO → BiHelix A → BiHelix B → UTEXO 完整往返，同一 contract ID、金额守恒，找零正确；网络中断和进程重启后不重复广播付款、不重复扣账。

P1/P2 共同测试：并发花费冲突、坏证明、ACK/NACK、丢回执、输入已花费、未确认交易、重组、取消、备份恢复后继续转出。若暂不支持 RBF，显式阻止该转账流程使用 RBF，不能沿用旧 txid/proof。

## 7. P3：Mint 进入和退出（预计 3–5 人日，外部等待另计）

负责人角色：业务后端 + 钱包前端。复用 P1/P2 的 RGB 收发，订单逻辑不进入 RGB 共识代码。

客户端按环境发现网络、币种和可用路线；取得报价后创建订单，关联 UTEXO transferId，完成钱包签名/支付，再通知 Mint 和查询结果。外部 API 详细字段以锁定的文档/接口版本为准。[Mint API](https://docs.utexo.com/product-suite/mint-api-reference)

建议的适配接口为 capabilities、quote、create_order、confirm_source、get_status；它们是内部抽象，先不承诺新的公共 HTTP URL。金额使用整数最小单位和精确十进制转换，保留每笔报价，不能硬编码费率或用浮点数处理金额。

进入：创建 BiHelix 接收 invoice → 申请 Mint 订单 → 用户 EVM 钱包审阅并调用指定合约 → 通知 Mint → daemon 接收并验证证明 → 到账。

退出：指定目标链和地址 → 申请 Mint 订单及其 RGB invoice → BiHelix 外部转账流程支付 → 通知 Mint → 跟踪目标链实际到账。钱包仅支付服务生成并校验过的 invoice；不自行推断或实现 USDT 的 burn 操作。

保存本地订单 ID、幂等键、账户、两端网络/资产映射、金额、invoice 摘要、UTEXO transferId、源 txid、目标 txid、原始状态和重试信息。asset ID 映射必须经验证，不能按 ticker 合并。

建议本地业务状态：quoted → created → awaiting_user_action → source_pending → destination_pending → completed；另设 failed、manual_review，退款独立跟踪到实际到账。上游失败或超时不能自动变成“已退款”，创建订单超时也不能直接重复创建并支付。

接口鉴权说明存在 RGB 特例与通用字段表之间的差异；P0/P3 使用具体请求样本核实。历史查询涉及签名 URL，日志必须脱敏。Mint 状态与两端账目需对账，API 接受请求不等于承兑完成。

**验收：**Mint 入金 → BiHelix A → BiHelix B → Mint 出金 → 目标链到账，记录资产身份、两端金额、费用和证明。真实资金路线由用户/运营指定额度并执行资金操作；自动化先使用 mock/无真实资金环境验证状态机。

## 8. P4：BitPocket / SDK 产品接入（预计 2–3 人日）

负责人角色：钱包前端，后端支持。

发布接收 invoice、支付外部 invoice、余额/状态查询及可选 Mint 订单接口的 SDK 封装。首个钱包集成 BitPocket，再输出其他钱包、DApp、交易所可复用示例。

界面区分 RGB 转账与跨链兑换，展示实际网络、到账金额、费用和处理中状态；未开通路线不显示为可用。主网、测试网资产不得混入一个余额。UTEXO Mint 不可用时，已持有资产的普通 RGB 转账应继续工作。

**验收：**用户不操作原始 consignment 文件即可完成收发及订单查询；错误路径有可定位订单，刷新或重启不会重新发起付款。

## 9. P5：Lightning（P2 之后独立评估，不计入首期承诺）

先确认产品采用 BiHelix 节点直接互联还是 UTEXO LSP，并验证两端准确版本、RGB invoice 扩展、通道资产 ID、承诺交易及 HTLC 语义。若 LSP 使用不同资产表示，必须明确兑换关系，不能当作同一资产直接合并余额。

测试开通道、双向支付、余额/流动性、超时失败、重启、合作关闭和强制关闭后的链上资产恢复。既有 daemon compose 实现不构成上述测试的替代。

Mint 的 Lightning 路线与普通 LN 节点互通分别验收；API 有字段不代表服务已开放。[Mint 当前能力限制](https://docs.utexo.com/product-suite/mint-api-reference)、[UTEXO Lightning 模块](https://docs.utexo.com/sdk/wdk-rgb-lightning)

## 10. 实施前需要确认的设计

| 决策 | 建议 | 确认时机 |
| --- | --- | --- |
| 共识库兼容 | 优先复用当前核心；只有 fixture 证明必要时才升级 | P0 结束 |
| 公共 API | 新增/版本化外部收发语义，保持已有调用兼容 | P1/P2 编码前 |
| 存储 | daemon 单一写入 inbox/outbox/接收秘密；Mint 订单独立逻辑命名空间 | 落盘结构实施前 |
| 广播时序 | 遵循对端验证的收发协议，钱包保持私钥控制 | P0 后 |
| 业务收费 | 普通 RGB 流通、Mint、swap/渠道归因分别定义 | 商业接入上线前 |

## 11. 排期、产物与发布

估算基于核心兼容、已有钱包配合：P0–P4 共 14–22 人日，不含对方等待、主网上线等待和共识升级。单名主力工程师约 3–5 周；P0 结束后依据实际协议差距重新估算。此为工作量估算，不是外部上线日期承诺。

| 里程碑 | 产物 | 通过条件 |
| --- | --- | --- |
| M0 | 版本/合约/网络清单、fixtures、时序说明 | 核心可验证，未知项有明确处理人 |
| M1 | daemon 外部双向 RGB 收发、测试报告 | 完整往返及恢复测试通过 |
| M2 | Mint 适配、订单对账、闭环记录 | 跨链进入后可退出，费用可核对 |
| M3 | BitPocket 演示、SDK 示例、运维手册 | 钱包端可完成流程，故障可恢复 |
| M4 | 独立 Lightning 报告 | 节点互通与退出恢复通过 |

主网上线要求：官方资产身份确认、生产网络/endpoint 实际开放、正式合约验证、真实额度试运行及退出成功、备份恢复通过、费用和限额确认、异常对账与人工处理责任明确。先限定资产和用户范围，逐步开放。

回退方式：分别关闭新建外部转账、Mint 新订单和新资产接入；保留查询、已提交订单的投递/确认/恢复。数据库采用向后可读或有明确迁移方案的设计，不能通过删库或回退二进制丢弃已广播资产状态。

## 12. 给 UTEXO 的技术确认清单（待发送）

1. 请提供推荐 SDK/底层 RGB commit、USDT 测试及正式 contract/genesis/schema、精度与权威发布位置。
2. 请提供外部钱包接收/付款的 blind/witness invoice 与 consignment fixture，以及 proxy 协议和广播/ACK 时序。
3. UTEXO signet 的准确链配置及 indexer/proxy 是什么？是否提供不需要真实 EVM 资金的联调环境？
4. Mint 是否接受 BiHelix 自生成 invoice？RGB 出金的 sender address、鉴权和 verify 字段如何填写？
5. 重复请求、订单创建超时、报价到期、取消/退款和目标链未到账如何查询与恢复？
6. 正式 Mint 的网络、资产、限额、费用和生产 endpoint 何时可用？承兑/冻结规则在哪里公布？
7. Lightning 是否使用与 L1 相同合约？如果不同，兑换和退出由谁执行？推荐的互联节点/LSP 是哪个？

2026-09-17 实施更新：隔离 signet 环境、测试 BTC、通用 NIA 及官方 Faucet 测试 USDT 的核心链上双向往返均已通过。当前 IFA 需要支持新 schema 的参考端，本次固定官方 Rust rgb-lib v0.3.0-beta.20；旧 Node SDK 冻结。USDT 最终为参考端 99.4、BiHelix 0.6，合计 100，双方已结算。P0 官方资产互操作证据已补齐；P1/P2 daemon 公共外部收发及恢复测试仍未完成，不能视为 M1 已交付。Mint networks 最后查询仍为 403，Mint/Lightning/主网未验收。未向群里发送消息，也未创建 Mint 订单或使用真实 EVM 资金。详见 [互操作实测记录](../deploy/utexo-signet/INTEROP-REPORT.zh-CN.md)。
