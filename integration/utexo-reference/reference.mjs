// Isolated UTEXO signet reference wallet. Never prints seed/key material.
import fs from 'node:fs';
import path from 'node:path';
import { generateKeys, WalletManager, configureLogging, LogLevel } from '@utexo/rgb-sdk';
process.umask(0o077);
configureLogging(LogLevel.NONE);
const [mode, name = 'reference-a', ...extra] = process.argv.slice(2);
const modes = ['init', 'status', 'receive', 'witness', 'utxos', 'issue-test', 'send'];
if (!modes.includes(mode) || !/^[a-z0-9-]+$/.test(name)) throw Error('Invalid mode or wallet name');
const dir = path.resolve('state', name);
fs.mkdirSync(dir, { recursive: true, mode: 0o700 });
const lockPath = path.join(dir, 'runner.lock');
const lock = fs.openSync(lockPath, 'wx', 0o600);
let wallet;
try {
  const keyPath = path.join(dir, 'keys.json');
  const resultPath = path.join(dir, `${mode}-public.json`);
  if (['issue-test', 'send', 'receive', 'witness'].includes(mode) && fs.existsSync(resultPath)) {
    throw Error(`Existing ${mode} result: inspect it before explicitly archiving it for another operation`);
  }
  if (mode === 'init' && fs.existsSync(resultPath)) {
    console.log(fs.readFileSync(resultPath, 'utf8'));
  } else {
    if (mode === 'init' && !fs.existsSync(keyPath)) {
      fs.writeFileSync(keyPath, JSON.stringify(await generateKeys('utexo')), { flag: 'wx', mode: 0o600 });
    }
    const keys = JSON.parse(fs.readFileSync(keyPath));
    wallet = new WalletManager({
      xpubVan: keys.accountXpubVanilla, xpubCol: keys.accountXpubColored,
      masterFingerprint: keys.masterFingerprint, mnemonic: keys.mnemonic,
      network: 'utexo', dataDir: path.join(dir, 'rgb'),
      indexerUrl: 'https://esplora-api.utexo.com',
      transportEndpoint: 'rpcs://rgb-proxy.utexo.com/json-rpc',
    });
    let result;
    if (mode === 'init') result = { address: await wallet.getAddress(), network: 'utexo', name };
    if (mode === 'status') {
      await wallet.syncWallet();
      await wallet.refreshWallet();
      const assets = await wallet.listAssets();
      const transfers = await wallet.listTransfers();
      for (const asset of Object.values(assets).flat()) transfers.push(...await wallet.listTransfers(asset.assetId));
      result = { btc: await wallet.getBtcBalance(), assets, transfers: [...new Map(transfers.map(t => [t.idx, t])).values()], utxos: await wallet.listUnspents() };
    }
    if (mode === 'receive' || mode === 'witness') {
      const amount = Number(extra[1] || 1000000);
      if (!Number.isSafeInteger(amount) || amount <= 0) throw Error('Positive integer amount required');
      const params = { amount, assetId: extra[0] || undefined, minConfirmations: 1, durationSeconds: 86400 };
      result = mode === 'receive' ? await wallet.blindReceive(params) : await wallet.witnessReceive(params);
    }
    if (mode === 'utxos') {
      const num = Number(extra[0] || 8), size = Number(extra[1] || 1000);
      if (!Number.isSafeInteger(num) || num <= 0 || !Number.isSafeInteger(size) || size < 546) throw Error('Invalid UTXO request');
      result = await wallet.createUtxos({ num, size, feeRate: 2, upTo: extra[2] !== 'false' });
    }
    if (mode === 'issue-test') result = await wallet.issueAssetNia({ ticker: 'BHXTEST', name: 'BiHelix Interop Test Only', precision: 0, amounts: [1000] });
    if (mode === 'send') {
      const [invoice, assetId, amountText, witnessSatsText] = extra;
      const witnessSats = Number(witnessSatsText || 2000);
      if (!Number.isSafeInteger(witnessSats) || witnessSats < 546) throw Error("Valid witness sats required");
      const amount = Number(amountText);
      if (!invoice || !assetId || !Number.isSafeInteger(amount) || amount <= 0) throw Error('invoice, assetId, positive integer amount required');
      // Ambiguous network failures require inspection, never an automatic retry.
      const pending = path.join(dir, 'send-pending.json');
      fs.writeFileSync(pending, JSON.stringify({ invoice, assetId, amount, witnessSats }), { flag: 'wx' });
      result = await wallet.send({ invoice, assetId, amount, donation: true, feeRate: 2, witnessData: { amountSat: witnessSats }, minConfirmations: 1 });
    }
    fs.writeFileSync(resultPath, JSON.stringify(result, null, 2));
    console.log(JSON.stringify(result, null, 2));
  }
} finally {
  try { if (wallet) await wallet.dispose(); }
  finally { fs.closeSync(lock); fs.unlinkSync(lockPath); }
}
