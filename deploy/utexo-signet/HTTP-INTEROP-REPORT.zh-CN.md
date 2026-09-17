# UTEXO 官方测试 USDT：daemon HTTP 闭环验收

日期：2026-09-17（Asia/Shanghai）。分支：`feat/utexo-signet-integration`。

## 范围与结果

daemon A/B 通过公共 HTTP API 完成收款、准备交易、提交钱包签名、证明交付与结算。实际路线为 UTEXO 官方 Rust 参考钱包 → daemon A → daemon B → UTEXO 参考钱包，包含 witness 和 blinded 两种收款。钱包签名使用独立测试工具，RGB 共识处理、输入预留与恢复由 daemon 完成，未用独立 stock harness 代替 daemon。

这是官方 Faucet 测试 USDT 在 UTEXO 自定义 signet 上的 L1 验收。Mint 承兑、主网与 Lightning 不在该验收结论内。

## 资产与交易

- Contract：`rgb:f~9F4X0C-TiLOTvy-pALF29V-2xJ2p0m-hP3_vpW-Alj4G5Y`，precision=6。
- Schema：`rgb:sch:IpjJhFLz3oywYKQxO3KmFgR0Aa415nlTNrNyEFqMZCE#shoe-colombo-mango`。
- 参考端：UTEXO `rgb-lib` v0.3.0-beta.20，commit `b5c2fb6e53bc8bce2c72e5e9ee50412d2a2f16a4`。
- A：`tb1qtjr0zpkn8s3ens84fmlgm32fna5huz4k2um9lh`。
- B：`tb1qlxm00ugdjddrx44wmztd9l9v96hq2w4t77yw0c`。

| 方向 | USDT | 接收方式 | TXID | 确认高度 | BTC 手续费 |
| --- | ---: | --- | --- | ---: | ---: |
| UTEXO → A | 0.5 | witness | `87e4619446a193f135f3d202da49c45a52d7fda9c3d1e661b0287ee1a37db5bc` | 630661 | 487 sats |
| A → B | 0.2 | blinded | `8abc3493e7c490da90e21375b7cd0908ada55f0afed6820f21da37bf493359bf` | 630691 | 500 sats |
| B → UTEXO | 0.1 | witness | `daea30e262cc5de6639d323e1c5a9039691ea4e87cf0dbb8c73476ea64c2ee60` | 630696 | 500 sats |

B 的 blinded seal 绑定 Faucet BTC UTXO `96f1ccff85074dda66300d1e1cdb5ce0679e49e3b96db4fd8e32225e4180eda8:0`（50,000 sats，高度 630631）。A → B 不创建新的 BTC 收款输出；B 随后花费自己的 seal UTXO，向参考端付 2,000 sats，找零 47,500 sats。

三个 proxy ACK 均为 true，daemon 四个收发 operation 均为 settled；参考端发送 idx=12、接收 idx=13 均为 Settled。最终参考钱包 settled/future/spendable 均为 99,000,000 原始单位（99.0 USDT）。

最终余额：参考端 99.0 + daemon A 0.3 + daemon B 0.1 + 前一轮独立 Carol 0.6 = **100 USDT**。Carol 未参与本次付款，余额沿用前一轮已验证记录。

参考端首次刷新将接收推进到 WaitingConfirmations，临时显示 settled/spendable=98.9、future=99.1；第二次刷新后全部收敛为 99.0。本报告采用最终已结算结果，不使用中间余额或仅凭 ACK 作为到账证据。

## HTTP 与恢复验证

1. 收款：`receives/create` 建立 A 的 witness 和 B 的 blind 接收意图；daemon 自行下载、验证并保存证明，确认后计入 account_utxos。
2. 付款：`transfers/prepare` 返回 RGB PSBT；测试钱包本地审阅并签名后，`transfers/finalize` 先落盘签名交易，后台执行上传与广播。daemon 不持有测试 WIF。
3. 真实进程中断：临时将扫描间隔设为 3600 秒，在 A → B finalize 已持久化、delivery/broadcast 均为 not_started 时执行 `sudo systemctl kill --kill-who=main --signal=SIGKILL rgb-service-utexo-signet.service`。systemd 日志记录 03:48:59 UTC 被信号终止、03:49:04 UTC 自动启动。PID 从 3563172 变为 3564114；恢复扫描间隔为 30 秒。
4. 恢复结果：同一 operation `0a3902e8f18ecf3264d1bc8513d2bf7af0d1d3a977d939b938d3bda0c31f3b37` 和同一 txid 完成上传、广播及结算。没有重新选择输入或创建替代付款。
5. HTTP 幂等：重复 prepare/finalize 返回同一 operation 和 txid；相同 request_id 修改 change_vout 返回 HTTP 409。网络响应丢失后查 txid 而不创建新付款的分支另有模拟 HTTP 回归测试覆盖。
6. 余额预留：B 的返还交易准备后，0.2 USDT 输入通过 `/v1/assets/list` 返回 reserved；结算后 A 的 0.3 与 B 的 0.1 USDT 返回 available/confirmed。
7. 结算后再次重启：PID 3564114 → 3566958；HTTP 比较四个 operation/txid 及完整资产分配结果，重启前后完全一致，余额不重复记账。

第一次强制终止命令使用的 `--kill-whom` 不被服务器 systemd 接受，未终止进程；随后使用上述 `--kill-who` 命令成功。公开证据中的 journal 对应实际成功执行。

## 自动验证

- daemon 本地及 AWS release：46 项通过，包括跨账户鉴权、篡改/未签名请求拒绝、输入原子预留、并发幂等、签名验证、持久化恢复和不确定广播恢复。
- API（axum）：4 项通过。
- RGB 核心单元测试：3 项通过，包括 blinded 金额守恒与超额发送拒绝。
- 真实互操作 fixtures：6 项通过；新增三笔证明分别有 107、108、109 个历史 bundle，并检查 blinded invoice 的 seal 确实出现在终端承诺中。
- `cargo check --locked --workspace --all-targets` 通过；依赖与既有组件仍有编译警告。
- 修改文件格式检查及 `git diff --check` 通过。

## 部署及证据

部署位置为 `bihelix-aws:/home/ubuntu/utexo-integration`，服务 `rgb-service-utexo-signet.service`，API `127.0.0.1:18787`。最终 daemon SHA256：`9fdef04a202ab3a112c11288d412ef427c442f52ad86d0a6cecc00a10f422b69`。部署的 daemon/core/API 源码及 Cargo 文件已逐项比对本地 SHA256。

公开证据见 `tests/fixtures/utexo-daemon/`：三份 consignment、blind invoice、交易数据、哈希清单、重启前后状态及参考端余额。该目录不含私钥、接收秘密或钱包数据库。

私有联调记录在 AWS `http-interop/`。原始 daemon 与数据备份也保存在该目录，但发生付款后不能恢复旧数据库继续运行。保留现有 service-data 与参考钱包，旧 Node 钱包及前一轮 Carol stock 不继续参与本次转账。

第一版边界：默认关闭；配置后仅允许指定 HTTPS proxy；需要 legacy.enabled=false；外部账户阻止旧 issue/direct-send/LN 写路径绕过预留；支持 P2WPKH/BIP86 账户；已返回 PSBT 不自动取消解锁；重组导致账户隔离复核，尚不自动重建状态。详细配置和实际请求见 [外部 RGB API](../../docs/EXTERNAL-RGB-API.zh-CN.md)。
