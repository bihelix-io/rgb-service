# zust-console

`zust-console` 是 RGB service 栈的命令行 REPL / 管理控制台。它会加载
`.zs` 启动脚本，注册原生 Zust 模块，然后保持 REPL 打开，用于调试和运维。

```bash
cargo run -p zust-console -- crates/zust-console/start.zs
cargo run -p zust-console -- -e 'ln_rgb::status()'
```

运行时配置由启动脚本写入：

```zs
root::add("local/rgb-service", "http://3.1.207.115:8091");
root::add("local/btc-addr", "bc1q5nqave6m673q4g704r4ppzwacur3d67amp3f8c");
root::add("local/signer-node", "417c33530ab6097e5e2538ffd16e833ee4337318015f201452a540c49adb4158");
```

`local/btc-addr` 是默认 L1 BTC/RGB 账户。`local/signer-node` 是 signer App
的 iroh node id。`local/rgb-service` 是 RGB daemon 的 HTTP 地址。

常用 REPL 命令：

```text
:load <path>
:reset
:quit
```

## Signer App 协议

签名请求通过 iroh 发送，消息格式是 Dynamic msgpack：

```text
[path, body]
```

console 目前会调用这些 signer path：

```text
/v1/signer/request-signature
/v1/signer/asset-authorization
/v1/signer/psbt/sign
/v1/signer/address/batch
```

批量地址请求：

```json
{
  "account_id": "bc1q...",
  "network": "bitcoin",
  "purpose": "low_water_refill",
  "count": 20,
  "timestamp_ms": 1780000000000
}
```

批量地址返回：

```json
{
  "account_id": "bc1q...",
  "network": "bitcoin",
  "count": 20,
  "addresses": [
    {
      "address": "bc1...",
      "derivation_path": "m/84'/0'/0'/0/12",
      "index": 12,
      "script_pubkey": "0014..."
    }
  ]
}
```

PSBT 签名请求：

```json
{
  "account_id": "bc1q...",
  "ident": "alice",
  "domain": "bihelix-btc-wallet",
  "psbt": "...",
  "signing_accounts": [
    {
      "kind": "derived",
      "ident": "alice",
      "account_id": "bc1q...",
      "address": "bc1q...",
      "source": "local_address_pool",
      "derivation_path": "m/84'/0'/0'/0/12",
      "index": 12,
      "script_pubkey": "0014...",
      "signer_response": {}
    }
  ],
  "policy": {},
  "expires_at_ms": 1780000300000,
  "timestamp_ms": 1780000000000
}
```

当 `ident == ""` 时，`signing_accounts[0].kind` 是 `"default"`，`address`
就是 `local/btc-addr`。

## 本地状态

Fjall 本地存储位于 `.zust-console/local-store`。这是 console 的本地状态，
不是 RGB stock，也不是 LN channel database。

当前保存的 partition：

| Partition | 用途 |
| --- | --- |
| `ident_btc_address` | `ident -> btc_address` 的收款地址分配映射。 |
| `wallet_btc_address` | 默认钱包地址缓存。 |
| `btc_deposit_records` | 已扫描到的 BTC 入账记录，key 为 `txid:vout`。 |
| `btc_address_pool_available` | 未使用的 signer 派生地址池记录。 |
| `btc_address_pool_used` | 已分配的 signer 派生地址记录，包含 signer 元数据。 |
| `ident_ln_invoice` | 预留的旧版/本地 LN invoice 映射。 |
| `ln_payment_hash_ident` | 预留的旧版/本地 LN payment 映射。 |

LN 热钱包状态单独保存：

```text
.zust-console/ln-node.json
.zust-console/lightning/
.zust-console/lightning/ldk/
```

## 模块概览

当前注册的原生模块：

```text
btc
rgb
ln_rgb
```

所有原生函数都会返回 Dynamic。大多数对象返回值包含 `ok: true`；错误会返回
`{ ok: false, error: "..." }`。

## btc

BTC 相关接口使用 `ident` 字符串选择地址：

```text
""      -> 默认 local/btc-addr
"alice" -> btc::get_deposit_address("alice") 分配的 signer 派生地址
```

当前 BTC 函数：

| 函数 | 说明 |
| --- | --- |
| `btc::get_wallet_address(ident)` | 返回选中的 BTC 地址和账户元数据。 |
| `btc::balance(ident)` | 通过 Esplora 读取选中地址的余额和 UTXO。 |
| `btc::status(ident)` | 返回选中 BTC 地址余额，并附带 `btc::assets(ident)` 结果。 |
| `btc::utxos(ident)` | 通过 Esplora 列出选中地址的 UTXO。 |
| `btc::assets(ident)` | 将选中地址的已跟踪 UTXO 发送到 RGB daemon `/v1/assets/list`。 |
| `btc::get_deposit_address(ident)` | 返回已有分配地址，或从本地 signer 派生地址池取一个地址。`""` 返回默认账户。 |
| `btc::lookup_address_ident(address)` | 从本地记录反查已分配地址对应的 ident。 |
| `btc::scan_deposits(ident)` | 通过 Esplora 扫描默认/ident 地址，并持久化入账记录。 |
| `btc::scan_ident_deposits(ident)` | 旧式 ident 扫描别名；要求 ident 非空。 |
| `btc::address_pool_status()` | 返回地址池可用/已用数量，以及可用地址记录。 |
| `btc::refill_address_pool(count)` | 向 signer App 请求 `count` 个新地址，并放入可用地址池。 |
| `btc::sign_psbt(psbt, ident)` | 将 PSBT 和选中签名账户元数据发送到 `/v1/signer/psbt/sign`。 |
| `btc::broadcast(tx_hex)` | 通过 Esplora `/tx` 广播 raw transaction hex。 |
| `btc::tx_status(txid)` | 读取 Esplora 交易确认状态。 |

示例：

```zs
btc::get_wallet_address("")
btc::get_deposit_address("alice")
btc::balance("alice")
btc::utxos("alice")
btc::scan_deposits("alice")
btc::sign_psbt(psbt, "alice")
```

当前 scanner 是小规模/admin 用法：每次只扫描一个选中的地址。它还不是生产级
block indexer。

## rgb

`rgb` 模块访问 `local/rgb-service`。大多数调用会用 `signed_request(...)`
包装 payload；除非输入里已经带有 `signature` 字段，否则会自动使用 signer App。

| 函数 | 路由 / 说明 |
| --- | --- |
| `rgb::signed(payload)` | 对任意 payload 返回 `{ payload, signature }`。 |
| `rgb::rna_balance()` | POST `/v1/rna/balance`。无参数。 |
| `rgb::request_signature(payload)` | 直接通过 `/v1/signer/request-signature` 请求签名。 |
| `rgb::asset_authorization(asset_id, amount, purpose, recipient, anchor_psbt, expires_at_ms)` | 直接请求 signer asset authorization。默认值用 `""`/`0`。 |
| `rgb::request(route, payload)` | POST 任意 RGB daemon route。 |
| `rgb::issue(ticker, name, precision, supply, allocation_outpoint)` | POST `/v1/assets/issue`。 |
| `rgb::assets()` | 对默认账户 POST `/v1/assets/list`。 |
| `rgb::token_list()` | GET `/v1/tokens/list`；公开 token/contract 列表。 |
| `rgb::balance(asset_id, scope)` | POST `/v1/balance`。`scope` 传 `""` 表示 `all`。 |
| `rgb::balance_breakdown(asset_id)` | POST `/v1/balance/breakdown`。 |
| `rgb::prepare_transfer(asset_id, amount, recipient, unsigned_anchor_psbt, change_vout, recipient_vout, fee_rate_sat_vb)` | 先请求 asset authorization，再 POST `/v1/transfers/prepare`。 |
| `rgb::commit_transfer(asset_id, amount, transfer_id, txid, signed_anchor_psbt)` | 先请求 asset authorization，再 POST `/v1/transfers/commit`。 |
| `rgb::pending()` | POST `/v1/pending/list`。 |
| `rgb::recover(operation_id)` | POST `/v1/recover`。传 `""` 表示恢复所有 recoverable operation。 |
| `rgb::test(scenario)` | POST `/v1/test/rgb`。传 `""` 表示 `full_rgb20_lifecycle`。 |

示例：

```zs
rgb::rna_balance()
rgb::token_list()
rgb::balance("...", "")
rgb::prepare_transfer("...", 100, "receiver-account", unsigned_psbt, 1, 0, 1)
rgb::commit_transfer("...", 100, transfer_id, txid, signed_psbt)
```

RGB consignment 的存储和传输由 `rgb-service-daemon` 负责，不在本地 console RGB
stock 里管理。

## ln_rgb

`ln_rgb` 是唯一的本地热钱包 Lightning 模块。它通过同一个
`LnRgbBtcLnBackend` runtime 控制 BTC LN 节点和 RGB-over-Lightning 流程。

创建/加载热钱包节点：

```zs
let lightning = ln_rgb::node_address({
  network: "bitcoin",
  low_water_sats: 100000,
  data_dir: ".zust-console/lightning",
  ldk_data_dir: ".zust-console/lightning/ldk",
  listen: "0.0.0.0:9736",
  chain_source: {
    kind: "esplora",
    url: "https://blockstream.info/api",
  },
});
root::add("local/lightning", lightning);
ln_rgb::start()
```

当前 LN/RGB-LN 函数：

| 函数 | 说明 |
| --- | --- |
| `ln_rgb::node_address(config)` | 创建/加载本地 LN 热钱包，持久化 mnemonic，返回脱敏后的节点对象。 |
| `ln_rgb::start()` | 从 `local/lightning` 启动 LN runtime。 |
| `ln_rgb::stop()` | 停止 LN runtime。 |
| `ln_rgb::status()` | 返回 runtime 状态、余额、peer/channel 数量。 |
| `ln_rgb::scanner_status()` | 返回 BTC 地址池/scanner 状态。 |
| `ln_rgb::spawn_scanner(interval_ms)` | 启动地址池补充 scanner 线程。 |
| `ln_rgb::events()` | 取出最多 100 条待处理 LN debug event。 |
| `ln_rgb::get_node_id()` | 返回 LN node id。 |
| `ln_rgb::get_addr()` | 返回 LN 热钱包 L1 充值地址。 |
| `ln_rgb::amount()` | 返回可用链上余额 + LN 余额快照，单位 sats。 |
| `ln_rgb::btc_amount()` | 返回可用链上余额快照，单位 sats。 |
| `ln_rgb::ln_amount()` | 返回 Lightning 余额快照，单位 sats。 |
| `ln_rgb::get_peers()` | 返回已连接/已持久化 peer。 |
| `ln_rgb::get_channels()` | 返回 channel 快照。 |
| `ln_rgb::connect(node_id, address, persist)` | 连接 peer。`address` 是 LDK socket address 字符串。 |
| `ln_rgb::open_channel(node_id, address, amount_sats, push_msat)` | 打开 BTC LN channel。`push_msat` 传 `0` 表示不 push。 |
| `ln_rgb::close_channel(channel_id, counterparty_node_id, force, reason)` | 关闭 BTC LN channel。`reason` 传 `""` 表示无 reason。 |
| `ln_rgb::invoice(amount_msat, description, expiry_secs)` | 创建 BOLT11 invoice。默认值用 `""`/`0`。 |
| `ln_rgb::pay(invoice)` | 支付 BOLT11 invoice 字符串。 |
| `ln_rgb::token_list()` | 通过 LN RGB service client 调 RGB daemon token list。 |
| `ln_rgb::get_info()` | 返回 RGB LN runtime 信息、余额、peer/channel 数量。 |
| `ln_rgb::rgb_channel_context(contract_id, amount, outbound)` | 构造指定 contract/amount 的 RGB channel context。 |
| `ln_rgb::open_rgb_channel(node_id, address, capacity_sat, push_msat, user_channel_id, contract_id, amount)` | 打开 RGB-funded channel。`address` 可传 `""`，`user_channel_id` 可传 `0` 使用默认值。 |
| `ln_rgb::send_rgb_payment(recipient_node_id, amount_msat, payment_id, contract_id, amount)` | 发送 RGB spontaneous payment。`payment_id` 传 `""` 时自动生成。 |

示例：

```zs
ln_rgb::status()
ln_rgb::get_addr()
ln_rgb::invoice(1000, "test", 3600)
ln_rgb::connect("...", "1.2.3.4:9735", true)
ln_rgb::get_info()
ln_rgb::open_rgb_channel("...", "1.2.3.4:9735", 100000, 0, 0, "...", 100)
ln_rgb::send_rgb_payment("...", 1000, "", "...", 1)
```

## 当前运行说明

- `btc::get_deposit_address(ident)` 会消费本地 signer 派生地址池；只有地址池低于
  low water 时才会请求 signer App 补充。
- `btc::scan_deposits(ident)` 适合小规模/admin 流程。大型托管钱包需要替换为
  基于 block/indexer 的 scanner。
- `btc::sign_psbt(psbt, ident)` 本地不持有私钥。它会把 PSBT 和地址派生元数据
  发送给 signer App。
- `rgb::*` 不再使用本地 RGB stock，也不再使用 Iroh consignment transport。
  Consignment 管理属于 `rgb-service-daemon`。
- LN 使用本地热钱包。它和 `local/btc-addr` 是分开的，channel 操作需要单独给
  LN 热钱包充值。
