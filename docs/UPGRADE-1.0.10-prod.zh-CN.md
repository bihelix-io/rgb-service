# rgb-service 1.0.10-prod 升级与 wallet-v2 质押迁移手册

本文档用于将 `rgb-service` 从 `1.0.9-prod` 升级到 `1.0.10-prod`，并将
`wallet-service-v2` PostgreSQL 备份中的历史质押赎回数据一次性迁移到
`rgb-service-daemon` 的 Fjall 数据库。

## 1. 版本范围

本版本增加：

- wallet-service-v2 兼容路由：`GET /stake/address`、`POST /stake/split`、
  `GET /redeem/list`、`POST /redeem/psbt`、`POST /redeem/callback`。
- 从 `.sql` 或 `.sql.zip` 导入历史 `redeem`、`transfer`、`rgb_assign` 数据。
- 对全部未赎回质押执行 BTC 链上预检。
- Fjall 原子写入、public key 二级索引、迁移标记及导入后回读校验。
- 同一备份重复执行时只校验、不重复导入；不同备份会被拒绝。

本次迁移只处理 BTC 质押赎回，不迁移 LN 状态，也不依赖 Ledger。
历史 RGB assignment 仅用于 `/redeem/list` 的资产展示和筛选；赎回交易本身是
BTC P2WSH CSV 输出的签名与花费。

以下流程仍不属于本版本：

- `/stake/coloring`、`/stake/callback` 的完整主质押流程。
- wallet-service-v2 的 stock backup/upload、foreign transfer、retry/rescan。
- LN 状态迁移。

## 2. 迁移前提

迁移前确认：

1. 已取得完整、只读保存的 wallet-service-v2 PostgreSQL `.sql` 或 `.sql.zip` 备份。
2. daemon 配置的 `service.network` 与备份所在 Bitcoin 网络一致。
3. `service.esplora_url` 或 Electrum backend 可访问，且能查询历史交易和 UTXO。
4. 已备份当前 `service.data_dir`。
5. 独立导入命令执行时，没有其他 daemon 占用同一个 Fjall 目录。
6. 导入和验证完成前，不把 BitDrop 生产流量切换到新服务。

建议先记录源文件摘要：

```bash
shasum -a 256 /secure-backup/wallet-v2.sql.zip
```

本次验证使用的备份摘要为：

```text
b3cbd3b28c37841475e199b3ecb339741a2985eec3721d79301751d779ccb527
```

迁移标记会绑定源文件 SHA-256。部署时必须持续使用完成 dry-run 的同一份文件，
不要重新压缩或修改 ZIP。

## 3. 安装 1.0.10-prod

```bash
cd /srv/rgb-service
git fetch origin --tags
git checkout 1.0.10-prod
cargo build --release -p rgb-service-daemon
```

二进制路径：

```text
target/release/rgb-service
```

先不要启动生产监听。

## 4. 配置兼容路由

兼容路由默认关闭。生产配置必须显式启用：

```toml
[legacy]
enabled = true
sync_timeout_secs = 30
reveal_address_count = 20
rgb_fee_enabled = false
```

如果 `node.bihelix.io/v3` 通过反向代理连接 daemon，应确保代理把 `/v3` 前缀正确
映射到 daemon 根路由。

## 5. 导入前 dry-run

dry-run 会打开配置中的 Fjall 数据库用于初始化服务，但不会写入
`stake_redeems`、二级索引或迁移标记。它会完成 SQL 关联检查和全部未赎回
outpoint 的链上检查。

```bash
target/release/rgb-service \
  import-wallet-v2-stake-redeems \
  /etc/rgb-service/rgb-service.toml \
  /secure-backup/wallet-v2.sql.zip \
  --report /secure-backup/stake-import-dry-run.json \
  --dry-run
```

必须同时满足以下条件才能继续：

```bash
jq '{
  success,
  phase,
  errors,
  parsed: .parsed_precheck,
  chain: (.chain_precheck | del(.records))
}' /secure-backup/stake-import-dry-run.json
```

验收标准：

- `success == true`
- `phase == "dry_run_complete"`
- `errors == []`
- `records_projected == redeem_rows`
- `active_records + spent_records == records_projected`
- `chain_precheck.records_checked == active_records`
- `chain_precheck.valid_unspent == active_records`
- `chain_precheck.spent_on_chain == 0`
- `chain_precheck.invalid == 0`

任何一项不满足都不要执行正式导入。先检查备份版本、链 backend、网络配置和异常
outpoint。

### 已验证备份的基准结果

在 Bitcoin mainnet 高度 `962999` 的验证结果为：

| 项目 | 数量 | sats |
| --- | ---: | ---: |
| 全部历史质押 | 358 | 164602985 |
| 未赎回且链上未花费 | 116 | 24839119 |
| 已赎回历史记录 | 242 | 139763866 |
| 当时已成熟可赎回 | 60 | 11592706 |
| 当时仍处于 CSV 锁定期 | 56 | 13246413 |

成熟数量会随区块高度增加而变化；生产迁移时以新生成的 dry-run 报告为准。

## 6. 正式导入

推荐让 daemon 在监听 HTTP 端口前完成一次性导入：

```bash
target/release/rgb-service \
  /etc/rgb-service/rgb-service.toml \
  --import-wallet-v2-stake-redeems /secure-backup/wallet-v2.sql.zip \
  --stake-import-report /secure-backup/stake-import.json
```

daemon 的执行顺序为：

1. 计算源文件 SHA-256。
2. 解析并关联 `redeem`、`transfer`、`rgb_assign`。
3. 检查全部未赎回 BTC outpoint。
4. 在一个 Fjall transaction 中写入全部记录、public key 索引和迁移标记。
5. `SyncAll` 持久化。
6. 回读全部记录和索引，验证不可变摘要及迁移标记。
7. 写入最终 JSON 报告。
8. 成功后才启动 HTTP listener。

也可以在 daemon 停止时执行独立导入：

```bash
target/release/rgb-service \
  import-wallet-v2-stake-redeems \
  /etc/rgb-service/rgb-service.toml \
  /secure-backup/wallet-v2.sql.zip \
  --report /secure-backup/stake-import.json
```

正式导入报告必须满足：

```bash
jq '{success, phase, import, post_import, errors}' \
  /secure-backup/stake-import.json
```

验收标准：

- `success == true`
- `phase == "imported_and_verified"`
- `errors == []`
- `import.inserted + import.already_equal == import.attempted`
- `import.marker_written == true`
- `post_import.records_verified == import.attempted`
- `post_import.indexes_verified == import.attempted`
- `post_import.immutable_digest_matches == true`
- `post_import.marker_verified == true`

看到以上结果后，移除一次性启动参数，再按正常方式启动 daemon。保留 SQL 备份、
dry-run 报告和正式报告。

## 7. 幂等行为

迁移标记位于 Fjall `schema_migrations` keyspace：

```text
wallet_v2_stake_redeems_v1
```

重复执行规则：

- 同一源文件 SHA-256：不再解析、查链或写入，只回读 358 条记录及索引并验证，
  返回 `already-applied`。
- 不同源文件 SHA-256：立即拒绝，避免把两份历史状态混入同一数据目录。
- 用户成功赎回后，记录的 `status` 和 `redeem_spend_txid` 可以更新；迁移摘要只绑定
  不可变的原始质押字段，因此正常赎回不会破坏重放校验。

不要手工删除迁移标记，也不要把另一份备份导入同一个 `service.data_dir`。

## 8. BitDrop 赎回验收

历史数据导入成功并启用兼容路由后，BitDrop 的 BTC 赎回顺序为：

1. `GET /redeem/list` 查询用户历史质押。
2. `POST /redeem/psbt` 由 daemon 从链上 funding transaction 重建 PSBT。
3. BitDrop 钱包使用质押 private key 签名，且不提前 finalize PSBT。
4. `POST /redeem/callback` 由 daemon 校验签名和 CSV 条件、广播交易并幂等更新 Fjall。

只读检查示例：

```bash
curl --get 'https://node.bihelix.io/v3/redeem/list' \
  --data-urlencode 'public_key=COMPRESSED_PUBLIC_KEY'
```

构建 PSBT 示例：

```bash
curl -X POST 'https://node.bihelix.io/v3/redeem/psbt' \
  -H 'content-type: application/json' \
  -d '{
    "outpoint": "TXID:VOUT",
    "fee_rate": 2,
    "address": "bc1..."
  }'
```

不要用生产 UTXO 试调用 `/redeem/callback`；该接口会真实广播交易。选择一条已成熟的
测试质押，由钱包完成签名后再进行端到端验收。仍处于 CSV 锁定期的记录可以展示和
生成 PSBT，但 Bitcoin 网络会在成熟前拒绝花费。

## 9. 监控

上线后检查：

```bash
tail -f '<service.data_dir>/rgb-service.log'
```

重点关注：

- `wallet-v2 stake import complete`
- `startup wallet-v2 stake redeem import ... result=applied`
- `/redeem/list`、`/redeem/psbt`、`/redeem/callback` 的 HTTP status
- chain backend timeout 或 broadcast error
- `already redeemed` 冲突

建议按链上 txid 对账每一次成功 callback，确认 Fjall `status=1` 与真实 spend txid
一致。

## 10. 回滚

### 尚未切流、没有真实赎回

1. 停止 `1.0.10-prod`。
2. 保存失败报告和 daemon 日志。
3. 恢复升级前完整 `service.data_dir` 备份。
4. 恢复原二进制或 `1.0.9-prod`。
5. 保持 BitDrop 仍指向旧服务。

不要只删除 Fjall keyspace 或迁移标记；必须以完整数据目录备份为回滚单位。

### 已经发生真实赎回

一旦 `/redeem/callback` 成功广播交易，就不能简单恢复旧数据库并重新启用
wallet-service-v2。旧 PostgreSQL 可能仍把该 outpoint 标为未赎回，形成双花尝试或
重复展示。

此时应：

1. 保留当前 Fjall 和所有 callback 日志。
2. 从 Bitcoin 链核对全部已广播 spend txid。
3. 把旧数据库中的对应记录标为已赎回，或继续以新 daemon 为唯一写入者。
4. 完成状态对账后再决定应用层回滚。

## 11. 完成标准

满足以下条件后迁移完成：

- 正式报告全部验收项通过。
- daemon 已去掉一次性导入参数并正常重启。
- 同源重放返回 `already-applied`。
- BitDrop 能列出历史质押并为已成熟记录生成可签名 PSBT。
- 一条受控赎回的广播 txid、链上 spend 和 Fjall 状态一致。
- `wallet-service-v2` 不再作为同一批质押记录的并行写入者。
