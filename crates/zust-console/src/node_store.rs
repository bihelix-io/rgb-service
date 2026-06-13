use std::{
    collections::HashMap,
    fs,
    path::{Path, PathBuf},
    sync::{Arc, Mutex, OnceLock, RwLock},
};

use anyhow::{anyhow, bail, Context, Result};
use fjall::{
    KeyspaceCreateOptions, PersistMode, Readable, SingleWriterTxDatabase, SingleWriterTxKeyspace,
};
use rgbstd::persistence::fjall::FjallBinStore;

const LOCAL_STORE_DIR: &str = "local-store";
const RGB_STOCK_PARTITION: &str = "rgb_stock";
const RGB_PENDING_OPS_PARTITION: &str = "rgb_pending_ops";
const IDENT_BTC_ADDRESS_PARTITION: &str = "ident_btc_address";
const WALLET_BTC_ADDRESS_PARTITION: &str = "wallet_btc_address";
const DEFAULT_WALLET_BTC_ADDRESS_KEY: &str = "default";
const IDENT_LN_INVOICE_PARTITION: &str = "ident_ln_invoice";
const LN_PAYMENT_HASH_IDENT_PARTITION: &str = "ln_payment_hash_ident";

static NODE_STORES: OnceLock<Mutex<HashMap<PathBuf, Arc<LocalNodeStoreInner>>>> = OnceLock::new();

#[derive(Clone)]
pub struct LocalNodeStore {
    inner: Arc<LocalNodeStoreInner>,
}

struct LocalNodeStoreInner {
    path: PathBuf,
    db: SingleWriterTxDatabase,
    rgb_stock_lock: RwLock<()>,
}

impl LocalNodeStore {
    pub fn open(data_dir: &Path) -> Result<Self> {
        Self::open_store_dir(&default_store_dir(data_dir))
    }

    pub fn open_store_dir(store_dir: &Path) -> Result<Self> {
        fs::create_dir_all(store_dir)
            .with_context(|| format!("create local Fjall store {}", store_dir.display()))?;
        let path = fs::canonicalize(store_dir)
            .with_context(|| format!("canonicalize local Fjall store {}", store_dir.display()))?;
        let mut stores = NODE_STORES
            .get_or_init(|| Mutex::new(HashMap::new()))
            .lock()
            .map_err(|err| anyhow!("local Fjall store registry lock poisoned: {err}"))?;
        if let Some(existing) = stores.get(&path) {
            return Ok(Self {
                inner: Arc::clone(existing),
            });
        }
        let db = SingleWriterTxDatabase::builder(&path)
            .open()
            .with_context(|| format!("open local Fjall store {}", path.display()))?;
        let inner = Arc::new(LocalNodeStoreInner {
            path: path.clone(),
            db,
            rgb_stock_lock: RwLock::new(()),
        });
        stores.insert(path, Arc::clone(&inner));
        Ok(Self { inner })
    }

    pub fn path(&self) -> &Path {
        &self.inner.path
    }

    pub fn rgb_stock_store(&self) -> Result<FjallBinStore> {
        self.rgb_stock_store_for_partition(RGB_STOCK_PARTITION)
    }

    pub fn rgb_stock_has_data(&self) -> Result<bool> {
        self.rgb_stock_partition_has_data(RGB_STOCK_PARTITION)
    }

    pub fn with_rgb_stock_write_lock<T>(&self, f: impl FnOnce() -> Result<T>) -> Result<T> {
        let _guard = self
            .inner
            .rgb_stock_lock
            .write()
            .map_err(|err| anyhow!("local RGB stock write lock poisoned: {err}"))?;
        f()
    }

    pub fn persist(&self) -> Result<()> {
        self.inner
            .db
            .persist(PersistMode::SyncAll)
            .context("persist local Fjall store")
    }

    pub fn get_ident_btc_address(&self, ident: &str) -> Result<Option<String>> {
        self.get_string(IDENT_BTC_ADDRESS_PARTITION, ident)
    }

    pub fn list_ident_btc_addresses(&self) -> Result<Vec<(String, String)>> {
        self.list_strings(IDENT_BTC_ADDRESS_PARTITION)
    }

    pub fn lookup_ident_by_btc_address(&self, address: &str) -> Result<Option<String>> {
        Ok(self
            .list_ident_btc_addresses()?
            .into_iter()
            .find_map(|(ident, candidate)| (candidate == address).then_some(ident)))
    }

    pub fn put_ident_btc_address(&self, ident: &str, address: &str) -> Result<()> {
        self.put_string(IDENT_BTC_ADDRESS_PARTITION, ident, address)
    }

    pub fn get_wallet_btc_address(&self) -> Result<Option<String>> {
        self.get_string(WALLET_BTC_ADDRESS_PARTITION, DEFAULT_WALLET_BTC_ADDRESS_KEY)
    }

    pub fn list_wallet_btc_addresses(&self) -> Result<Vec<(String, String)>> {
        self.list_strings(WALLET_BTC_ADDRESS_PARTITION)
    }

    pub fn put_wallet_btc_address(&self, address: &str) -> Result<()> {
        self.put_string(
            WALLET_BTC_ADDRESS_PARTITION,
            DEFAULT_WALLET_BTC_ADDRESS_KEY,
            address,
        )
    }

    pub fn get_ident_ln_invoice(&self, ident: &str) -> Result<Option<String>> {
        self.get_string(IDENT_LN_INVOICE_PARTITION, ident)
    }

    pub fn put_ident_ln_invoice(&self, ident: &str, invoice: &str) -> Result<()> {
        self.put_string(IDENT_LN_INVOICE_PARTITION, ident, invoice)
    }

    pub fn get_ln_payment_hash_ident(&self, payment_hash: &str) -> Result<Option<String>> {
        self.get_string(LN_PAYMENT_HASH_IDENT_PARTITION, payment_hash)
    }

    pub fn put_ln_payment_hash_ident(&self, payment_hash: &str, ident: &str) -> Result<()> {
        self.put_string(LN_PAYMENT_HASH_IDENT_PARTITION, payment_hash, ident)
    }

    pub fn get_rgb_pending_op(&self, txid: impl std::fmt::Display) -> Result<Option<Vec<u8>>> {
        self.get_bytes(RGB_PENDING_OPS_PARTITION, &txid.to_string())
    }

    pub fn put_rgb_pending_op(&self, txid: impl std::fmt::Display, op: &[u8]) -> Result<()> {
        self.put_bytes(RGB_PENDING_OPS_PARTITION, &txid.to_string(), op)
    }

    pub fn remove_rgb_pending_op(&self, txid: impl std::fmt::Display) -> Result<()> {
        let keyspace = self.keyspace(RGB_PENDING_OPS_PARTITION)?;
        let mut tx = self.inner.db.write_tx();
        tx.remove(&keyspace, txid.to_string().as_bytes());
        tx.commit()
            .with_context(|| format!("remove RGB pending op {txid}"))?;
        self.persist()
    }

    fn rgb_stock_store_for_partition(&self, partition: &str) -> Result<FjallBinStore> {
        Ok(
            FjallBinStore::with_database(self.inner.path.clone(), self.inner.db.clone(), partition)
                .with_context(|| format!("open RGB stock partition `{partition}`"))?,
        )
    }

    fn keyspace(&self, partition: &str) -> Result<SingleWriterTxKeyspace> {
        self.inner
            .db
            .keyspace(partition, KeyspaceCreateOptions::default)
            .with_context(|| format!("open local Fjall partition `{partition}`"))
    }

    fn get_string(&self, partition: &str, key: &str) -> Result<Option<String>> {
        let Some(bytes) = self.get_bytes(partition, key)? else {
            return Ok(None);
        };
        Ok(Some(String::from_utf8(bytes).with_context(|| {
            format!("decode local Fjall key `{key}` from `{partition}` as UTF-8")
        })?))
    }

    fn put_string(&self, partition: &str, key: &str, value: &str) -> Result<()> {
        self.put_bytes(partition, key, value.as_bytes())
    }

    fn list_strings(&self, partition: &str) -> Result<Vec<(String, String)>> {
        let keyspace = self.keyspace(partition)?;
        self.inner
            .db
            .read_tx()
            .iter(&keyspace)
            .map(|item| {
                let (key, value) = item
                    .into_inner()
                    .with_context(|| format!("iterate local Fjall partition `{partition}`"))?;
                let key = String::from_utf8(key.as_ref().to_vec())
                    .with_context(|| format!("decode local Fjall key from `{partition}`"))?;
                let value = String::from_utf8(value.as_ref().to_vec()).with_context(|| {
                    format!("decode local Fjall value for key `{key}` from `{partition}`")
                })?;
                Ok((key, value))
            })
            .collect()
    }

    fn get_bytes(&self, partition: &str, key: &str) -> Result<Option<Vec<u8>>> {
        let keyspace = self.keyspace(partition)?;
        Ok(keyspace
            .get(key.as_bytes())
            .with_context(|| format!("read local Fjall key `{key}` from `{partition}`"))?
            .map(|bytes| bytes.as_ref().to_vec()))
    }

    fn put_bytes(&self, partition: &str, key: &str, value: &[u8]) -> Result<()> {
        let keyspace = self.keyspace(partition)?;
        let mut tx = self.inner.db.write_tx();
        tx.insert(&keyspace, key.as_bytes(), value);
        tx.commit()
            .with_context(|| format!("commit local Fjall key `{key}` into `{partition}`"))?;
        self.persist()
    }

    fn rgb_stock_partition_has_data(&self, partition: &str) -> Result<bool> {
        self.rgb_stock_store_for_partition(partition)?
            .has_data()
            .map_err(|err| anyhow!("check RGB stock partition `{partition}`: {err:?}"))
    }
}

pub fn default_store_dir(data_dir: &Path) -> PathBuf {
    data_dir.join(LOCAL_STORE_DIR)
}

pub fn ensure_ident(ident: Option<String>) -> Result<Option<String>> {
    match ident {
        Some(ident) if ident.trim().is_empty() => bail!("ident must not be empty"),
        Some(ident) => Ok(Some(ident)),
        None => Ok(None),
    }
}
