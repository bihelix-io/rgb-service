# daemon 外部 RGB 收发接口实施方案

日期：2026-09-17。代码基线：`e555c2b`，分支 `feat/utexo-signet-integration`。
状态：L1 接口已实现，AWS signet HTTP 闭环、强制终止恢复与结算后重启验证通过，见 [验收报告](../deploy/utexo-signet/HTTP-INTEROP-REPORT.zh-CN.md)。前七节保留设计依据，实际 HTTP 合约与兼容边界以第 8 节为准。

目标：钱包通过 daemon API 完成外部 RGB 收款、付款、状态查询及中断恢复；不再需要手工运行互操作 harness。使用已经通过官方 Faucet 测试 USDT 双向链上验证的 BiHelix 核心，私钥和交易签名仍由钱包负责。

## 1. 实施决策

1. 增加 `/v1/external/...` 端点；现有 `/v1/transfers/prepare`、`commit` 的本机账户直转请求格式保持兼容，不把 invoice 塞进 `recipient`。
2. 沿用 daemon 现有 Fjall 数据库、账户 stock、account_utxos 及后台扫描。新增独立版本化 inbox/outbox、输入预留和幂等记录，不再引入另一套钱包数据库，不在 daemon 内嵌 UTEXO SDK。
3. 钱包提供 BTC carrier PSBT 并在本地签名。daemon 负责 RGB 分配、承诺、consignment、代理投递和广播恢复；新增流程由 daemon 广播已审阅并授权的确切交易。
4. 两种 invoice beneficiary 都纳入收发验收：witness 与 blinded。已补齐 blinded 核心 seal 构造与秘密保存，并通过 daemon A → B 的真实 blinded 收款验证。
5. 外部 transport 由运维配置允许列表；首个实际联调目标为 UTEXO 官方 proxy。允许列表未配置时关闭外部代理网络访问。
6. 新接口要求账户归属校验。保留现有签名封装，但不能只验证“某个公钥签过这段 JSON”。第一版按现有 `account_id = BTC address` 模型支持公钥可直接证明归属的 P2WPKH 和 BIP86 P2TR 账户；任意 descriptor、多签和脚本账户明确拒绝，后续单独扩展。

部署先更新隔离的 bihelix-aws UTEXO signet 实例。主网和其他运行中实例不随该次联调发布。

## 2. 已核实的代码差距

| 已有代码 | 可复用能力 | 需要补齐 |
| --- | --- | --- |
| `rgb-service-api/src/axum_service.rs` | SignedRequest、权限检查、HTTP 错误映射 | 外部收发路由、状态及请求 DTO |
| `rgb-service-daemon/src/main.rs:ConfiguredAuthVerifier` | ECDSA 请求与资产授权验签 | 公钥到账户、接收地址及输入的归属绑定；新端点独立签名 purpose |
| `main.rs:prepare_transfer/commit_transfer` | PSBT、fascia、发送/接收 staged stock | 对方 invoice、投递、广播前持久化、跨入口输入预留 |
| `main.rs:cancel_transfer/list_pending` | 方法签名 | 当前取消不清理状态，pending 返回空；不能作为新接口实现 |
| `main.rs:spawn_recovery_scanner` | 后台确认与 account_utxos 更新 | inbox/outbox 扫描、网络超时恢复、可观察错误 |
| `rgb-service-local/src/lib.rs` | prepare external PSBT、consignment、验证与接受 | blinded seal、候选交易校验、接收意图和金额验证 |

准备交易时还必须显式检查 RGB 输入总量足够。现有核心使用 `saturating_sub` 计算找零，不能把“成功构造了 PSBT”当作金额守恒已经验证。

## 3. HTTP 合约

所有端点使用现有 `SignedRequest<T>` JSON 封装。每个端点有独立签名 purpose，签名覆盖完整 payload。写操作增加 `request_id`；业务幂等不依赖会随重签改变的请求签名 nonce。

| POST 路径 | purpose | payload 主要字段 | 结果 |
| --- | --- | --- | --- |
| `/v1/external/receives/create` | `external_receive_create` | account_id、request_id、asset_id、amount、expires_at、mode、beneficiary 参数 | operation_id、invoice、recipient_id、status |
| `/v1/external/transfers/prepare` | `external_transfer_prepare` | account_id、request_id、invoice、unsigned_anchor_psbt、recipient_vout（witness）、change_vout、asset_authorization | operation_id、anchor_psbt、txid、付款摘要、status |
| `/v1/external/transfers/finalize` | `external_transfer_finalize` | account_id、request_id、operation_id、signed_anchor_psbt、asset_authorization | operation 状态；不是保证已结算 |
| `/v1/external/operations/get` | `external_operation_get` | account_id、operation_id | 状态、数量、txid、确认、投递结果、重试信息 |
| `/v1/external/operations/list` | `external_operation_list` | account_id、cursor、limit、可选 direction/status | 分页记录；不返回其他账户资料 |
| `/v1/external/operations/refresh` | `external_operation_refresh` | account_id、operation_id | 触发/排队一次核查，返回当前状态 |
| `/v1/external/operations/cancel` | `external_operation_cancel` | account_id、request_id、operation_id | 明确的取消或拒绝原因 |

第一版不开放用户提交任意 URL 的下载接口，也不要求调用方自己上传原始 consignment。daemon 按已保存 invoice/recipient 从允许的代理获取证明。

字段规则：

- `asset_id` 是完整 contract ID，不按 USDT ticker 匹配资产。
- `amount` 为正整数最小单位，沿用现有 Rust u64 API；客户端必须使用能够无损处理整数的序列化方式。资产精度来自已验证合约。
- `expires_at` 使用 Unix 秒；确认要求由服务配置约束，客户端不能降到零确认即记可用余额。
- `mode=witness`：钱包提交收款地址及归属证明；第一版可使用账户自身地址，不允许替他人地址创建可计入本账户的收款。
- `mode=blind`：钱包提交受控 outpoint 及归属证明，daemon 生成随机 seal、保存秘密和映射；invoice 仅包含 concealed beneficiary。
- `anchor_psbt` 沿用现有 API 的二进制 PSBT 十六进制编码。
- `asset_authorization` 必须与同一账户、合约、金额、invoice 和 PSBT 一致；prepare 绑定输入 PSBT，finalize 绑定 daemon 返回的 RGB PSBT 及 operation。外层签名绑定 operation_id，不能只凭客户端给出的 txid 提交。
- `request_id` 按账户和端点隔离；同键同语义请求返回原 operation，同键不同语义返回 HTTP 409。
- get/list 是只读查询，不隐式广播；refresh 只处理已保存且被授权的任务，不能更换交易。

建议响应结构：

```json
{
  "operation_id": "<opaque id>",
  "direction": "send",
  "status": "awaiting_signature",
  "asset_id": "rgb:...",
  "amount": 1000000,
  "txid": "<prepared txid>",
  "delivery_status": "not_started",
  "broadcast_status": "not_started",
  "confirmations": 0,
  "required_confirmations": 1,
  "retryable": false,
  "last_error": null
}
```

状态响应不泄露 blind secret、完整私有历史证明或签名密钥。原始证明可由运维备份恢复；不通过公开 catalog 暴露。

## 4. 状态与处理顺序

### 收款

`awaiting_transfer → validating → waiting_confirmations → settled`

1. 验证账户与 beneficiary 归属，先持久化意图、invoice 和 blind secret，再返回 invoice。
2. 后台按 recipient 拉取证明，限制体积、超时、解码资源；将原始字节、摘要和投递元数据持久化。
3. 绑定 contract、network、beneficiary、txid/vout 与精确金额；完整验证 RGB 历史和 carrier。精确金额不符进入 `needs_review`，不得默默入账或只采信代理 `validated` 字段。
4. 未广播候选交易验证与链上确认分开。如果既有 resolver 不能验证当前候选 carrier，需要增加仅针对该候选 txid 的 resolver 注入并验收，不跳过历史验证。链上出现后必须逐字核对实际 carrier。
5. 只有通过上述验证、证明与恢复记录已可靠落盘后才允许 ACK。网络/索引器暂时失败不发送 NACK；确定性无效才记录拒绝原因及 NACK。
6. 满足确认要求后接受到 stock、登记账户 UTXO 并推进余额。每个落盘阶段可幂等重放，不能只设置 `Settled` 标签。
7. invoice 到期停止新支付预期，但已发现支付继续核查；迟到支付保留记录，进入显式处理状态，不删除秘密或证明。

### 付款

`preparing → awaiting_signature → delivering → waiting_confirmations → settled`

投递、ACK、广播分别保存事实；不使用单一 committed 状态代替这三者。

1. 解析 invoice，拒绝错误网络、缺少合约/金额、过期、未知 transport。核对 witness 输出或 blinded seal，检查 BTC/RGB 输入及所有同 UTXO 资产找零。
2. 在数据库事务中创建意图并预留全部输入，再构造 RGB PSBT；成功结果与 fascia/consignment 必须持久化后才能返回。
3. 钱包审阅 BTC 费用、资产数量及接收方后本地签名，提交 finalize。
4. 验证签名结果对应已保存的同一 unsigned transaction，不允许改输入、输出、sequence、locktime 或 RGB commitment。第一版只接受 txid 不因签名变化的 native SegWit carrier，明确拒绝 RBF。
5. 持久化签名交易、证明和提交意图后才开始上传。上传成功后按经过对端实测的发送模式广播；本次参考端 donation 模式不等待 ACK，不能设计成双方互等确认。
6. 丢失上传响应时查询同 recipient/txid 的状态并核对已有证明，不换 recipient 或创建新付款。广播超时先查询预期 txid，允许重投同一原始交易，禁止重新构建另一笔付款。
7. 发送 fascia 与找零记录幂等落盘，后台确认后结算；在结果不明确时继续预留输入。

取消规则：未创建可签名载体的任务可以释放资源；PSBT 已交给钱包后，它可能已在外部签名或广播。此后取消只能停止尚未开始的自动动作并保留风险记录，不凭超时/客户端声明解锁输入。已投递或已广播的任务不能返回“取消成功且资金可重新花费”；需链上核实输入已通过其他已确认交易失效，才做冲突恢复。收款取消也不得丢弃已经发生的支付。

重组：不凭曾经到达 `settled` 永久缓存可用余额；重新校验受影响 outpoint/确认状态，必要时冻结关联付款并进入 `needs_review`，不静默回滚为另一笔新付款。

## 5. 持久化与并发

在现有 Fjall 内新增逻辑 keyspaces，记录带 `schema_version=1`：

| keyspace | 保存内容 |
| --- | --- |
| external_rgb_operations | 账户、方向、invoice、请求摘要、阶段、txid、数量、时间、错误与重试状态 |
| external_rgb_proofs | 原始 consignment、摘要、fascia、必要的 PSBT/签名交易；按操作引用 |
| external_rgb_receive_secrets | blinded seal secret、受控 outpoint、收款意图关联 |
| external_rgb_reservations | network/outpoint → account/operation；全输入预留 |
| external_rgb_idempotency | account/endpoint/request_id → 语义摘要及 operation_id |

意图、幂等和输入预留在同一数据库事务提交并 SyncAll 后才有外部副作用。网络调用不持有数据库写事务；账户级协调器及操作 claim 防止 HTTP 与后台重复推进。

预留必须覆盖现有 direct-send、legacy 和 LN 花费入口；只在新 API 内加锁不构成双花防护。冲突请求返回 409，并要求查原 operation。旧载体已生成但无法核实的账户暂不允许并发建立外部付款。

stock 写入与业务记录之间存在崩溃窗口，使用持久化阶段和确定性操作身份重放，而不是假定两者天然是同一原子事务。确认扫描、余额查询的 UTXO 清理也要遵守未决预留。

恢复扫描在启动时重新发现持久化任务；后台分批、有限并发、指数退避。代理错误与广播状态不明确保留结构化 last_error/next_retry_at。账户数据损坏与确定性验证失败不会无限自动重试。

兼容：不修改旧 PreparedTransferRecord 的格式，不迁移已有账户 stock；旧二进制不认识新 keyspaces，因此存在活跃外部操作时不能直接降级运行旧二进制。回退应先停止新建、继续用新版本查询及恢复已提交任务。

## 6. 配置与传输

新增可选 `[external_rgb]`，默认 disabled；候选字段：enabled、proxy_allowlist、max_consignment_bytes、request_timeout_secs、required_confirmations、max_inflight_operations、retry_interval_secs。

代理必须使用 HTTPS，禁止重定向到未批准目标；DNS 解析和连接目的地址防止访问内网/环回/链路本地地址。测试中的本地 mock transport 通过依赖注入，不打开生产 HTTP 任意 URL 权限。

复用已核验 UTEXO JSON-RPC 的 consignment.get/post、ack.get/post。固定方法与响应解析，上传/下载按字节限额流式处理；代理 HTTP 200、`result=true`、ACK 和链上验证是不同证据。

daemon 持有自己的 proof 副本，UTEXO 持有自己的副本；proxy 不作为唯一存储。备份同时覆盖数据库、inbox/outbox 和 blind secret，钱包 BTC 私钥仍不进入 daemon。

## 7. 实施与验收顺序

1. API DTO/路由与签名 purpose、账户归属、持久化状态/幂等/预留；单元与 HTTP 测试先验证跨账户拒绝、同键重试及并发冲突。
2. 完成 witness/blind invoice 与核心 seal 适配；代理 mock 下验证错误合约、数量、受益人、网络、坏证明、ACK/NACK 和资源限额。
3. 接收后台任务与确认入账；测试每个外部副作用前后中断并重启，不能重复入账。
4. 付款 prepare/finalize 与代理投递、广播恢复；覆盖签名交易被替换、过期、重复 finalize、丢响应、取消及输入冲突。
5. AWS 使用新账户/独立数据先做 HTTP 验收，再执行官方测试 USDT 的 UTEXO → daemon A → daemon B → UTEXO；对照同合约金额守恒，并覆盖双方支持的 blind/witness 模式。
6. 更新调用示例、部署/恢复手册、实际 endpoint 表和验收报告。只有 HTTP 路径及恢复用例通过，才将 P1/P2 标为完成。

2026-09-17 用户追加要求：Mint 入金与承兑已纳入整体接入交付，作为独立里程碑实施，见 [Mint 实施方案](UTEXO-MINT-API.zh-CN.md)。L1 收发通过不代表 Mint 承兑通过。主网上线、Lightning、BitPocket 页面、多签/任意 descriptor 账户仍须独立确定范围并验收。

## 8. 实现接口（以本节为调用合约）

当前已实现上述七条路由，并完成真实 signet HTTP 往返验收。第 3 节为设计草案，实际 wire format 统一为 `SignedRequest<ExternalRgbRequest>`：payload 包含 `account_id` 和带 `type` 的 `action`。签名 purpose 统一为 `external_rgb`，但签名覆盖完整 action；路由会拒绝不匹配的 type，不能跨端点重放为另一动作。

```json
{
  "payload": {
    "account_id": "<账户 BTC 地址>",
    "action": {
      "type": "receive",
      "request_id": "receive-001",
      "asset_id": "rgb:...",
      "amount": 500000,
      "expires_at": 1789700000,
      "blind_outpoint": null
    }
  },
  "signature": {
    "signer_id": "<账户标识>",
    "public_key": "<压缩公钥>",
    "scheme": "ecdsa",
    "nonce": "<新 nonce>",
    "timestamp_ms": 1789615000000,
    "signature": "<DER 签名十六进制>"
  }
}
```

示例时间仅演示单位；实际调用需使用当前时间。完整 DTO 定义在 `crates/rgb-service-api/src/external.rs`。

| action.type | action 其余字段 |
| --- | --- |
| receive | request_id、asset_id、amount、expires_at、blind_outpoint（null 为 witness） |
| prepare | request_id、invoice、unsigned_anchor_psbt、recipient_vout（blind 为 null）、change_vout、asset_authorization |
| finalize | request_id、operation_id、signed_anchor_psbt、asset_authorization |
| get | operation_id |
| list | after（null 或上一页 cursor）、limit（1–100） |
| refresh | operation_id |
| cancel | request_id、operation_id |

响应统一为 `{"operations":[...],"next_cursor":null}`；单笔接口返回一个元素。refresh 将任务安排到后台扫描，并返回当前记录；get/list 不访问 proxy，也不会广播。

默认关闭，通过 daemon TOML 启用：

```toml
[service.external_rgb]
enabled = true
proxy_allowlist = ["https://rgb-proxy.utexo.com/json-rpc"]
max_consignment_bytes = 2097152
request_timeout_secs = 30
required_confirmations = 1
max_operations = 10000
```

使用现有 `service.recovery_scan_interval_secs` 驱动恢复；外部功能启用时该值为 0 也保留 60 秒扫描，避免任务无人执行。第一版要求 `legacy.enabled=false`，不能与绕过预留的 legacy 钱包写入口同时启用。

实现要点与边界：

- ECDSA 请求公钥须对应账户的 P2WPKH 或 BIP86 P2TR 地址；资产授权公钥独立做同样检查。signer_id 文本不构成所有权证明。账户地址必须是配置网络上的规范小写形式。
- BTC carrier 的全部输入都须属于该账户、已确认且未花费；witness_utxo 与链上输出逐项一致。只允许指定接收输出、账户找零和一个 OP_RETURN；拒绝 RBF、重复输入及多余输出。第一版不处理任意 descriptor、多签、tapret invoice。
- finalize 验证完整原始交易一致性以及最终 ECDSA/Schnorr witness 签名。提交后保存原始签名交易，再由 worker 投递和广播；不会重新选择输入或更改付款数量。
- 收款使用精确金额；不符合 invoice 的证明保留为 needs_review。blind 接收要求账户持有的未分配 RGB UTXO，并预留至接收完成。witness 接收的 proxy recipient ID 不重复使用；后续收款可用新的账户地址或 blind UTXO。
- 新 keyspaces 为 `external_rgb_operations_v1`、`external_rgb_artifacts_v1`、`external_rgb_idempotency_v1`、`external_rgb_reservations_v1`、`external_rgb_accounts_v1`。blind secret 保存在受保护的操作记录中；证明/fascia 与操作元数据分开，同事务保存。
- 资产列表及 balance breakdown 按外部输入预留与账户隔离状态投影，预留资产不计入 available；pending 操作来自持久化记录。待确认找零不会提前计为可用，进行中的付款金额以 operation 为准。
- 外部账户登记后，旧 direct-send/issue/LN 写 API 返回冲突，防止绕过预留；其他账户的旧请求格式保持兼容。已有 legacy prepared/pending 的账户须先处理完旧任务。此为第一版的明确兼容边界，不宣称已把所有 legacy/LN 路径迁入同一预留协议。
- 写操作与单个后台状态推进暂以进程内互斥协调，数据库事务内再次检查输入预留。get/list 不等待慢速证明验证。进程重启恢复以数据库记录为准，不依赖内存锁。
- preparing 且尚未返回 PSBT 的失败任务可以取消并释放预留；已返回 PSBT 的发送任务不允许取消解锁。收款取消仍保留秘密与投递观察，迟到付款进入显式复核。
- 确认丢失进入 needs_review 并禁止该账户继续新付款；第一版采用冻结复核，不自动重建被重组影响的后续 RGB 状态。
- 运营复核、旧账户迁入、多签和任意地址轮换不是隐式自动流程。对状态不明确的操作保留证据，不能通过删记录/回退旧二进制释放资产。

调用工具与钱包签名示例见 `integration/utexo-daemon/README.md`。daemon 不接收 BTC 私钥；测试工具读取独立 test WIF，仅输出签名请求或签名交易。
