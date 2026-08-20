# rgb-service 1.0.11-prod 赎回修复升级手册

本文档用于将 `rgb-service` 从 `1.0.10-prod` 升级到 `1.0.11-prod`。
`1.0.11-prod` 不改变 wallet-v2 质押导入格式或 Fjall schema，重点修复 BitDrop
BTC 历史质押的 `/redeem/psbt` 和 `/redeem/callback` 路径。

首次从 `1.0.9-prod` 升级或尚未导入 wallet-v2 备份时，仍按
[1.0.10-prod 迁移手册](UPGRADE-1.0.10-prod.zh-CN.md)执行导入，但应直接使用
`1.0.11-prod` 二进制。

## 1. 修复内容

- `/redeem/psbt` 在构建交易前查询最新链状态，只为已确认、CSV 已成熟且仍未花费的
  outpoint 返回 PSBT。
- 手续费按预计最终 P2WSH witness 后的交易 weight 计算，避免旧版本只按 unsigned
  transaction 估算而导致实际费率不足。
- `/redeem/callback` 按 stake outpoint 串行处理，避免同一 daemon 进程内两个回调同时
  广播同一个质押输出。
- callback 广播前核对链上 outspend；如果交易已经广播但 Fjall 尚未更新，会根据真实
  spend txid 自动补写 `status=1` 和 `redeem_spend_txid`。
- 广播返回超时或“交易已存在”等错误时，会短暂重查链状态；只要链 backend 已观察到
  同一个 txid，就按幂等成功处理。
- 同一签名 PSBT 重放返回成功；同一 outpoint 被其他 txid 花费时返回冲突并记录真实
  spend txid，不会再次广播。
- 如果已记录交易从内存池消失，同一签名 PSBT 会重新广播；如果交易被 RBF 替换，只有
  链 backend 实际观察到的 replacement txid 才能更新 Fjall。
- 拒绝已经 finalize、带 `final_script_sig`、缺少正确 witness script、sequence 或签名的
  PSBT。
- wallet-v2 SQL 导入会拒绝重复 `transfer.id`，避免后出现的异常行静默覆盖前一行。
- 如果 Fjall 已原子提交、但进程在最终 JSON 报告落盘前中断，同源重启会验证迁移标记、
  全部记录和索引，并把预检查报告修复为 `already_applied_verified`。

本版本仍只处理 BTC 赎回，不增加 LN 状态迁移，也不依赖 Ledger。

## 2. 升级前检查

1. 记录当前运行版本和 `service.data_dir`。
2. 完整备份 `service.data_dir`，不要只备份单个 Fjall keyspace。
3. 保存已有 wallet-v2 SQL 备份和导入报告。
4. 确认配置的 Esplora 或 Electrum backend 可以查询 tip、funding transaction、UTXO 和
   outspend。
5. 如果 `1.0.10-prod` 已经产生真实赎回，先保存 callback 日志和对应 txid。

## 3. 构建与安装

```bash
cd /srv/rgb-service
git fetch origin --tags
git checkout 1.0.11-prod
cargo build --release -p rgb-service-daemon
```

二进制路径：

```text
target/release/rgb-service
```

停止旧 daemon 后替换二进制，再使用原配置和原 `service.data_dir` 启动。升级过程中不需要
删除迁移标记，也不需要重新导入已经成功导入的 SQL 备份。

如果部署命令仍保留同一份一次性导入参数，启动时只会执行同源幂等校验；建议确认正式
报告正常后移除该参数，减少每次启动的无意义检查。

异常中断后的修复报告会包含：

```text
success = true
phase = "already_applied_verified"
idempotent_replay = true
recovered_from_marker = true
post_import.marker_verified = true
```

## 4. 上线前验证

### 4.1 历史数据

用一个已知 public key 检查历史质押仍可读取：

```bash
curl --get 'https://node.bihelix.io/v3/redeem/list' \
  --data-urlencode 'public_key=COMPRESSED_PUBLIC_KEY'
```

核对 outpoint、sats、CSV height、status 和 `redeem_spend_txid` 与升级前一致。

### 4.2 PSBT 构建

选择一条 `status=0`、链上未花费且已成熟的测试记录：

```bash
curl -X POST 'https://node.bihelix.io/v3/redeem/psbt' \
  -H 'content-type: application/json' \
  -d '{
    "outpoint": "TXID:VOUT",
    "fee_rate": 2,
    "address": "bc1..."
  }'
```

验收：

- 已成熟且未花费：返回一个单输入、单输出 PSBT。
- CSV 未成熟或 funding transaction 未确认：返回 `409`，不生成可提交的 PSBT。
- outpoint 已被花费：返回 `409`；能够获得 spend txid 时同步修复 Fjall 状态。
- `fee_rate=0`：返回 `400`。
- 输出金额等于 funding sats 减去按最终 witness weight 计算的手续费，且不低于 dust。

### 4.3 Callback 幂等性

`/redeem/callback` 会真实广播交易，只能使用受控、已成熟的测试质押。

1. 钱包对 daemon 生成的 PSBT 签名，不要提前 finalize。
2. 首次提交 callback，记录返回状态和广播 txid。
3. 原样重复提交同一个签名 PSBT。
4. 再次查询 `/redeem/list` 并检查链上 outspend。

验收：

- 首次成功返回 `200`。
- 同一签名 PSBT 重复提交仍返回 `200`，不会生成第二个 txid。
- Fjall 中为 `status=1`，`redeem_spend_txid` 与链上 spend txid 完全一致。
- 未被链 backend 接受的另一个交易不能覆盖已记录 txid；如果链上已出现真实 replacement，
  Fjall 会改为该 replacement txid，并对不匹配的 callback 返回 `409`。

### 4.4 发布前真实备份基准

`1.0.11-prod` release binary 使用以下源文件摘要完成了新的 mainnet dry-run：

```text
b3cbd3b28c37841475e199b3ecb339741a2985eec3721d79301751d779ccb527
```

在 Bitcoin 高度 `963236` 的结果：

| 项目 | 数量 | sats |
| --- | ---: | ---: |
| 全部历史质押 | 358 | 164602985 |
| 未赎回且链上未花费 | 116 | 24839119 |
| 已赎回历史记录 | 242 | 139763866 |
| 已成熟可赎回 | 60 | 11592706 |
| 仍处于 CSV 锁定期 | 56 | 13246413 |

`records_checked=116`、`valid_unspent=116`、`spent_on_chain=0`、`invalid=0`。
成熟数量会随区块高度变化，生产导入仍以部署时生成的报告为准。

## 5. 运行时行为

callback 的状态顺序为：

1. 校验 PSBT 结构、stake witness script、CSV sequence 和用户签名字段。
2. 计算候选 spend txid。
3. 查询链上 outspend；同 txid 已存在则补写 Fjall 并返回成功，其他 txid 则以链上实际
   spend 修复 Fjall 后返回冲突。
4. 确认 funding transaction 已确认且下一候选区块满足 CSV。
5. 广播交易。
6. 同步持久化 Fjall；若广播响应不确定，则重查链上 outspend 后决定成功或返回错误。

因此，客户端遇到网络超时可以安全地原样重放同一个签名 PSBT。不要在超时后重新修改
输出地址、金额或手续费再提交；修改后的交易会产生另一个 txid，并在原交易已经被接受时
得到冲突响应。

## 6. 监控与对账

重点监控：

- callback 返回的 `200`、`409` 和 `500` 数量。
- 日志中的 `legacy broadcast ok`、`legacy broadcast failed` 和
  `legacy redeem broadcast recovery query failed`。
- Esplora/Electrum 的超时及索引延迟。
- Fjall `redeem_spend_txid` 与链上 outspend txid 是否一致。

对每次生产赎回至少保存 outpoint、候选 txid、callback 结果和最终链上状态。`500` 表示
daemon 当时既没有确认广播成功，也没有从链 backend 观察到同 txid；客户端应重放原始
签名 PSBT，而不是立即构造不同交易。

## 7. 回滚

尚未发生新赎回时，可以停止 `1.0.11-prod`，恢复完整数据目录备份和旧二进制。

一旦发生真实赎回，不应直接恢复旧 Fjall 快照后继续接收流量，因为快照可能再次把已花费
outpoint 显示为 `status=0`。应先按 Bitcoin 链上 outspend 对账并修复数据库，或继续让
`1.0.11-prod` 作为唯一写入者。

## 8. 完成标准

- daemon 全量测试和 release build 通过。
- 原有导入记录、迁移标记和二级索引不变。
- 已成熟记录可生成手续费正确的 PSBT，锁定或已花费记录不能生成 PSBT。
- 受控 callback 首次广播、同 txid 重放和 Fjall/链上对账均通过。
- 生产客户端在超时时只重放同一个签名 PSBT。
