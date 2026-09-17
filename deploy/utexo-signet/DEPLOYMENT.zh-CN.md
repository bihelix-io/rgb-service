# UTEXO signet 部署记录

初始验收时间：2026-09-16 11:42（Asia/Shanghai）。

## 2026-09-17 外部 HTTP API 更新

隔离实例已更新并启用 `[service.external_rgb]`，legacy 保持关闭，proxy 允许列表为 `https://rgb-proxy.utexo.com/json-rpc`，恢复扫描间隔为 30 秒。API 继续仅监听 `127.0.0.1:18787`。

当前 daemon 二进制 SHA256：`9fdef04a202ab3a112c11288d412ef427c442f52ad86d0a6cecc00a10f422b69`。对应 daemon/core/API 源文件哈希在 `tests/fixtures/utexo-daemon/deployed-source-sha256.json`，与本次提交源码一致；下方 `71c8808` 与旧二进制哈希是初次部署历史。

官方测试 USDT 的三段 HTTP 转账及真实 SIGKILL 恢复已通过。结算后再次重启，四个 operation/txid 和 A/B 余额保持一致；最终服务 active。完整证据见 [HTTP 验收报告](HTTP-INTEROP-REPORT.zh-CN.md)。

部署前备份位于 AWS `http-interop/rgb-service.before-external`、`rgb-service.before-external.toml` 与 `service-data.before-external.tgz`。当前数据库已有新付款和 blind 接收秘密，不能恢复该旧数据快照继续花费。私钥和原始数据库未进入 Git。

## 初次部署记录

- 分支：`feat/utexo-signet-integration`。
- daemon 源码：`71c88087295027de384fd72910c7b33c785947ff`；本分支新增部署配置、探针及测试签名工具，未修改 daemon 业务逻辑。
- 主机：`bihelix-aws`；目录：`/home/ubuntu/utexo-integration`。
- systemd：`rgb-service-utexo-signet.service`，已启用开机启动。
- API：`127.0.0.1:18787`，仅回环监听；运行用户 ubuntu。
- 数据：`service-data/`；测试私钥：`signer/alice.wif`，权限 0600，目录 0700。私钥未进入 Git，也未提供给 daemon。
- Esplora：`https://esplora-api.utexo.com`。
- 二进制 SHA256：`f67eebd6ac87d9b145cfae0340ac207236ef4b486845c9a1fdfea0998ea5f41d`。
- 构建：`cargo build --locked --release -p rgb-service-daemon --bin rgb-service --example signet_test_signer`，独立 target，2 个编译任务。

可通过 `ssh -L 18787:127.0.0.1:18787 bihelix-aws` 建立本地访问隧道。

## Faucet 已到账

- 来源：官方 SDK sandbox 文档列出的 Telegram `@Utexo_RLN_bot`。
- 地址：`tb1qd3aemqrz7a3z4cqqt2emmkecwme83s5p43lku9`。
- 金额：50,000 sats（0.0005 测试 BTC）。
- TXID：`0cfd1d29d89e8b2c87d19582c31519efe6e0823920011dcfe2491c79ba4751ba`，vout 0。
- UTEXO Esplora 已确认：区块高度 627876。
- Bot 帮助文本仍写 shared regtest；实际发款已通过上述 UTEXO Esplora 的 UTXO 与区块查询核实。

UTEXO 高度 100 哈希为 `0000027606cb73bbf383cb8666bd402dd0de1e154c39688db7b72a6cb4b56a17`，与服务器现有普通 signet Bitcoin Core 不同。因此此实例未使用原有 Bitcoin Core。

## 已验证

- catalog API HTTP 200，初始合约及资产列表为空。
- 专用测试密钥签名的 RNA 查询 HTTP 200。
- 无签名请求 HTTP 422；错误签名请求 HTTP 401。
- 为本实例测试账户计入 100,000 DAEMON_RNA 服务额度，使用唯一幂等键；这不是 BTC 或 RGB 资产。
- 重启前后签名查询返回相同额度，持久化检查通过。
- systemd active/running，NRestarts=0；监听地址为回环。
- 原有 Docker 容器均仍运行，未重启。
- 服务器保留 `smoke-results.json`、`chain-results.json`、`alice-public.json` 和 `build.log`。

## 后续互操作实测

已建立官方 SDK 对照钱包，并完成通用 NIA 测试资产的链上双向往返。新增工具支持受限测试签名；这些操作使用独立 stock，运行中 daemon 二进制与业务 API 未因此更新。2026-09-17 已用隔离 Rust 参考端完成官方 Faucet 测试 USDT 的核心链上往返（1 USDT 来款、0.4 USDT 返还，最终参考端 99.4 / BiHelix 0.6）。活动参考钱包为 `reference-rust-signet-state`，USDT stock 为 `interop-stock-usdt-carol`；旧 Node 钱包已冻结。daemon 外部 HTTP API、Mint 与 Lightning 未验收。完整版本、交易、故障及接续条件见 [互操作实测记录](INTEROP-REPORT.zh-CN.md)。
