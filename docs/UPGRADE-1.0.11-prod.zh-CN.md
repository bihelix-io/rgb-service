# rgb-service 1.0.11-prod 完整升级、数据导入与赎回手册

本文档是 `1.0.11-prod` 的独立部署手册。即使生产环境从未部署
`1.0.10-prod`，也应直接按本文完成升级和 wallet-service-v2 历史质押数据导入，
不需要先安装或运行 `1.0.10-prod`。

目标是将 wallet-service-v2 PostgreSQL 备份中的历史 BTC 质押赎回数据一次性导入
`rgb-service-daemon` 的 Fjall 数据库，并由 daemon 提供 BitDrop 使用的
`/redeem/list`、`/redeem/psbt` 和 `/redeem/callback`。

本次只迁移 BTC 质押赎回，不迁移 LN 状态，也不依赖 Ledger。

## 1. 版本能力与范围

`1.0.11-prod` 包含以下完整能力：

- 从 wallet-service-v2 `.sql` 或 `.sql.zip` 备份解析 `redeem`、`transfer` 和
  `rgb_assign`。
- 对全部未赎回 outpoint 执行 Bitcoin 链上确认、金额、脚本、确认高度和未花费检查。
- 将质押记录、public key 二级索引和迁移标记原子写入 Fjall，并执行 `SyncAll`。
- 导入后回读全部记录和索引，校验不可变摘要并生成 JSON 报告。
- 同一备份重复执行时只验证，不重复导入；不同备份会被拒绝。
- 提供 `GET /redeem/list`、`POST /redeem/psbt`、`POST /redeem/callback`。
- `/redeem/psbt` 只为已确认、CSV 已成熟且未花费的 outpoint 构建 PSBT。
- 手续费按预计最终 P2WSH witness 后的 transaction weight 计算。
- callback 按 outpoint 串行处理，并在广播响应不确定或 Fjall 未落盘时根据链上
  outspend 自动恢复。
- 同一签名 PSBT 可以安全重放；RBF replacement 只以链 backend 实际观察到的 txid
  为准。
- 导入器拒绝重复 `transfer.id`；原子提交完成但最终报告未写完时，可以从迁移标记
  修复报告。

以下内容不属于本次迁移：

- `/stake/coloring`、`/stake/callback` 的完整主质押流程。
- wallet-service-v2 的 stock backup/upload、foreign transfer、retry/rescan。
- LN 状态迁移。

## 2. 升级和导入前准备

开始前必须完成：

1. 取得完整、只读保存的 wallet-service-v2 PostgreSQL `.sql` 或 `.sql.zip` 备份。
2. 确认 daemon 配置的 `service.network` 与备份中的 Bitcoin 网络一致。
3. 确认 Esplora 或 Electrum backend 可以查询历史交易、tip、UTXO 和 outspend。
4. 完整备份当前 `service.data_dir`，不要只备份单个 Fjall keyspace。
5. 保存当前生产二进制、配置和服务启动命令。
6. 导入时确保没有另一个 daemon 进程占用同一个 Fjall 数据目录。
7. 导入和验证完成前，不切换 BitDrop 生产赎回流量。

计算并保存源文件摘要：

```bash
shasum -a 256 /secure-backup/wallet-v2.sql.zip
```

已经核验的生产备份摘要为：

```text
b3cbd3b28c37841475e199b3ecb339741a2985eec3721d79301751d779ccb527
```

正式导入必须使用与 dry-run 完全相同的文件。不要重新压缩、修改或替换 ZIP；迁移标记
会绑定源文件 SHA-256。

## 3. 安装 1.0.11-prod

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

确认 tag 对应 commit：

```bash
git rev-parse 1.0.11-prod^{commit}
```

预期：

```text
1a9007692326046a0ad007bb2a96a08d0470ad2d
```

先停止旧服务，不要立即启动生产 listener。

## 4. 配置 daemon

基础配置示例：

```toml
[service]
bind = "127.0.0.1:8787"
network = "mainnet"
data_dir = "/var/lib/rgb-service"
esplora_url = "https://YOUR-ESPLORA/api"

[rna]
issue_fee = 1000
transfer_fee = 100
query_fee = 1

[legacy]
enabled = true
sync_timeout_secs = 30
reveal_address_count = 20
rgb_fee_enabled = false
```

如果使用 Electrum，`service.esplora_url` 填写项目支持的 Electrum URL。若
`node.bihelix.io/v3` 通过反向代理连接 daemon，应确保代理将 `/v3` 前缀映射到 daemon
根路由。

配置中的 `service.data_dir` 必须是准备长期使用的生产 Fjall 数据目录。dry-run 会初始化
该目录，但不会写入质押记录或迁移标记。

## 5. 导入前 dry-run

先运行独立 dry-run：

```bash
target/release/rgb-service \
  import-wallet-v2-stake-redeems \
  /etc/rgb-service/rgb-service.toml \
  /secure-backup/wallet-v2.sql.zip \
  --report /secure-backup/stake-import-dry-run.json \
  --dry-run
```

dry-run 会：

1. 计算源文件 SHA-256。
2. 解析并关联 `redeem`、`transfer`、`rgb_assign`。
3. 校验重复主键、重复 outpoint、状态、txid、public key、CSV height 和 assignment。
4. 检查全部未赎回 outpoint 的 funding transaction、金额、P2WSH 脚本、确认高度和
   outspend。
5. 生成 JSON 报告。
6. 不写入 `stake_redeems`、二级索引或迁移标记。

检查报告：

```bash
jq '{
  success,
  phase,
  errors,
  parsed: .parsed_precheck,
  chain: (.chain_precheck | del(.records))
}' /secure-backup/stake-import-dry-run.json
```

必须全部满足：

- `success == true`
- `phase == "dry_run_complete"`
- `errors == []`
- `records_projected == redeem_rows`
- `active_records + spent_records == records_projected`
- `chain_precheck.records_checked == active_records`
- `chain_precheck.valid_unspent == active_records`
- `chain_precheck.spent_on_chain == 0`
- `chain_precheck.invalid == 0`

任何一项不满足都不要正式导入。先检查备份、网络配置、链 backend 和异常 outpoint。

### 已核验备份基准

`1.0.11-prod` release binary 在 Bitcoin mainnet 高度 `963236` 的 dry-run 结果：

| 项目 | 数量 | sats |
| --- | ---: | ---: |
| 全部历史质押 | 358 | 164602985 |
| 未赎回且链上未花费 | 116 | 24839119 |
| 已赎回历史记录 | 242 | 139763866 |
| 已成熟可赎回 | 60 | 11592706 |
| 仍处于 CSV 锁定期 | 56 | 13246413 |

基准报告为：`records_checked=116`、`valid_unspent=116`、`spent_on_chain=0`、
`invalid=0`。成熟数量会随区块高度变化，正式部署以新生成的报告为准。

## 6. 正式导入并启动

dry-run 验收通过后，推荐使用启动参数让 daemon 在监听 HTTP 端口前执行一次正式导入：

```bash
target/release/rgb-service \
  /etc/rgb-service/rgb-service.toml \
  --import-wallet-v2-stake-redeems /secure-backup/wallet-v2.sql.zip \
  --stake-import-report /secure-backup/stake-import.json
```

启动顺序是：

1. 计算并核对源 SHA-256。
2. 重新解析 SQL 并完成全部关联检查。
3. 重新检查全部未赎回 BTC outpoint。
4. 在一个 Fjall transaction 中写入全部记录、public key 索引和迁移标记。
5. 执行 `SyncAll`。
6. 回读全部记录和索引，验证不可变摘要及迁移标记。
7. 原子写入最终 JSON 报告。
8. 只有全部成功后才启动 HTTP listener。

如果导入或验证失败，daemon 不会开始监听。必须保留报告和日志并处理错误，不能跳过检查
强行启动。

也可以在 daemon 完全停止时执行独立正式导入：

```bash
target/release/rgb-service \
  import-wallet-v2-stake-redeems \
  /etc/rgb-service/rgb-service.toml \
  /secure-backup/wallet-v2.sql.zip \
  --report /secure-backup/stake-import.json
```

独立导入成功后，再使用正常命令启动 daemon。

## 7. 正式导入报告验收

```bash
jq '{success, phase, idempotent_replay, import, post_import, errors}' \
  /secure-backup/stake-import.json
```

首次正式导入必须满足：

- `success == true`
- `phase == "imported_and_verified"`
- `idempotent_replay == false`
- `errors == []`
- `import.inserted + import.already_equal == import.attempted`
- `import.marker_written == true`
- `post_import.records_verified == import.attempted`
- `post_import.indexes_verified == import.attempted`
- `post_import.immutable_digest_matches == true`
- `post_import.marker_verified == true`

报告通过后，保存以下文件：

- 原始 SQL/ZIP 备份。
- 源文件 SHA-256。
- dry-run 报告。
- 正式导入报告。
- 导入期间的 daemon 日志。
- 导入前完整 `service.data_dir` 备份。

## 8. 移除一次性参数并正常重启

正式导入和报告验收完成后，停止刚启动的 daemon，移除两个一次性参数：

```text
--import-wallet-v2-stake-redeems
--stake-import-report
```

然后使用正常命令启动：

```bash
target/release/rgb-service /etc/rgb-service/rgb-service.toml
```

确认 daemon 正常监听，并再次检查：

```bash
curl 'https://node.bihelix.io/v3/health'
```

不要在每次生产启动中永久保留导入参数；迁移只需要执行一次。

## 9. 幂等与异常恢复

迁移标记位于 Fjall `schema_migrations` keyspace：

```text
wallet_v2_stake_redeems_v1
```

重复执行规则：

- 同一源文件 SHA-256：不重新解析、查链或重复写入 Fjall，只回读全部记录及索引并验证。
- 不同源文件 SHA-256：立即拒绝，避免把两份历史状态混入同一数据目录。
- 用户赎回后 `status` 和 `redeem_spend_txid` 可以变化；迁移摘要只绑定不可变质押字段，
  不会因为正常赎回而失效。
- 不要手工删除迁移标记，不要将另一份备份导入同一个 `service.data_dir`。

如果 Fjall 原子提交成功后，进程在最终报告落盘前中断，同源命令再次启动会验证迁移
标记、全部记录和索引，并修复原来的预检查报告。修复报告包含：

```text
success = true
phase = "already_applied_verified"
idempotent_replay = true
recovered_from_marker = true
post_import.marker_verified = true
```

只有报告和 post-import 检查全部通过，才能移除一次性参数。

## 10. 导入后的历史数据检查

选择若干已知 public key 查询：

```bash
curl --get 'https://node.bihelix.io/v3/redeem/list' \
  --data-urlencode 'public_key=COMPRESSED_PUBLIC_KEY'
```

核对以下字段：

- `outpoint`
- `sats`
- `height`
- `public_key`
- `status`
- `spend_txid`
- `assign_map`
- `confirm_height`
- `create_time`

应同时抽查未赎回和已赎回历史记录。已赎回记录的 `spend_txid` 应与 Bitcoin 链上
outspend 一致。

## 11. PSBT 构建验收

选择一条 `status=0`、链上未花费且已经 CSV 成熟的受控记录：

```bash
curl -X POST 'https://node.bihelix.io/v3/redeem/psbt' \
  -H 'content-type: application/json' \
  -d '{
    "outpoint": "TXID:VOUT",
    "fee_rate": 2,
    "address": "bc1..."
  }'
```

验收结果：

- 已成熟且未花费：返回单输入、单输出 PSBT。
- CSV 未成熟或 funding transaction 未确认：返回 `409`，不会生成 PSBT。
- outpoint 已花费：返回 `409`；能够获得真实 spend txid 时同步修复 Fjall。
- `fee_rate=0`：返回 `400`。
- funding 金额或 P2WSH CSV 脚本与导入记录不一致：返回冲突。
- 输出金额等于 funding sats 减去按最终 witness weight 计算的手续费，且不低于 dust。

## 12. Callback 赎回验收

`POST /redeem/callback` 会真实广播 Bitcoin 交易，只能使用受控、已成熟的测试质押。

1. 由 daemon 调用 `/redeem/psbt` 生成 PSBT。
2. 钱包使用质押 private key 签名，必须保留 partial signature，不要提前 finalize。
3. 将签名 PSBT 提交 `/redeem/callback`。
4. 保存返回状态和候选 txid。
5. 原样重复提交同一个签名 PSBT。
6. 查询 `/redeem/list` 并检查链上 outspend。

验收结果：

- 首次成功返回 `200`。
- 同一签名 PSBT 重复提交仍返回 `200`，不会生成第二个 txid。
- Fjall 中 `status=1`，`redeem_spend_txid` 与链 backend 实际 outspend 一致。
- 广播返回超时或“交易已存在”时，daemon 会短暂重查 outspend；观察到同一 txid 即按
  幂等成功处理。
- 交易从内存池消失时，同一签名 PSBT 可以重新广播。
- 未被链 backend 接受的另一个候选交易不能覆盖已记录 txid。
- 如果链上已经出现真实 RBF replacement，Fjall 以真实 replacement txid 为准，对不匹配
  的 callback 返回 `409`。

客户端遇到超时必须重放原始签名 PSBT，不要修改输出地址、金额或手续费后构造另一个
交易。

## 13. 监控和对账

查看日志：

```bash
tail -f '<service.data_dir>/rgb-service.log'
```

重点关注：

- `wallet-v2 stake import complete`
- `startup wallet-v2 stake redeem import`
- `legacy broadcast ok`
- `legacy broadcast failed`
- `legacy redeem broadcast recovery query failed`
- `/redeem/list`、`/redeem/psbt`、`/redeem/callback` 的 HTTP status
- Esplora/Electrum timeout 和索引延迟

每次生产赎回应保存 outpoint、候选 txid、callback 结果和最终链上 outspend，并确认 Fjall
记录一致。

## 14. 回滚

### 尚未发生真实赎回

1. 停止 `1.0.11-prod`。
2. 保存导入报告和 daemon 日志。
3. 恢复升级前完整 `service.data_dir` 备份。
4. 恢复旧二进制和启动命令。
5. 不切换 BitDrop 流量。

不要只删除单个 Fjall keyspace 或迁移标记；必须以完整数据目录备份为回滚单位。

### 已经发生真实赎回

一旦 callback 广播成功，不能直接恢复旧数据库或旧 Fjall 快照后继续接收流量。旧状态
可能再次把已经花费的 outpoint 显示为未赎回。

此时必须：

1. 保留当前 Fjall、callback 日志和签名 PSBT。
2. 从 Bitcoin 链核对全部实际 spend txid。
3. 确保数据库状态与链上 outspend 一致。
4. 对账完成后再决定应用层回滚。

## 15. 最终完成标准

全部满足后才算完成：

- `1.0.11-prod` release binary 已安装。
- dry-run 报告全部验收项通过。
- 正式导入报告为 `imported_and_verified`，记录、索引、摘要和迁移标记全部通过。
- 一次性导入参数已经移除，daemon 正常重启和监听。
- `/redeem/list` 可以读取全部历史质押。
- 已成熟记录可以生成手续费正确的 PSBT；锁定、未确认或已花费记录不能生成 PSBT。
- 一条受控 callback 的首次广播、相同 PSBT 重放和 Fjall/链上对账全部通过。
- wallet-service-v2 不再作为同一批质押记录的并行写入者。
