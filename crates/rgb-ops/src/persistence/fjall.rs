// RGB ops library for working with smart contracts on Bitcoin & Lightning
//
// SPDX-License-Identifier: Apache-2.0

use std::fmt;
use std::io;
use std::path::PathBuf;

use amplify::confinement::{Confined, U32 as U32MAX};
use fjall::{KeyspaceCreateOptions, PersistMode, SingleWriterTxDatabase, SingleWriterTxKeyspace};
use nonasync::persistence::{PersistenceError, PersistenceProvider};
use strict_encoding::{StrictDeserialize, StrictSerialize};

use crate::persistence::{MemIndex, MemStash, MemState};

const KEYSPACE_RGB_BLOBS: &str = "rgb_blobs";
const KEY_STASH: &[u8] = b"stash";
const KEY_STATE: &[u8] = b"state";
const KEY_INDEX: &[u8] = b"index";

/// Fjall-backed binary persistence provider for RGB [`MemStash`], [`MemState`]
/// and [`MemIndex`].
///
/// This keeps the same coarse-grained blob semantics as [`super::fs::FsBinStore`]:
/// RGB data remains represented by the in-memory providers while persistence is
/// handled by a local embedded key-value store.
///
/// The provider is thread-safe at the Fjall engine level. RGB stock snapshot
/// writes are serialized by [`Stock::store`](crate::persistence::Stock::store).
#[derive(Clone)]
pub struct FjallBinStore {
    pub path: PathBuf,
    db: SingleWriterTxDatabase,
    blobs: SingleWriterTxKeyspace,
}

impl fmt::Debug for FjallBinStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FjallBinStore")
            .field("path", &self.path)
            .field("keyspace", &KEYSPACE_RGB_BLOBS)
            .finish_non_exhaustive()
    }
}

impl FjallBinStore {
    pub fn new(path: PathBuf) -> fjall::Result<Self> {
        let db = SingleWriterTxDatabase::builder(&path).open()?;
        Self::with_database(path, db, KEYSPACE_RGB_BLOBS)
    }

    pub fn with_database(
        path: PathBuf,
        db: SingleWriterTxDatabase,
        keyspace_name: &str,
    ) -> fjall::Result<Self> {
        let blobs = db.keyspace(keyspace_name, KeyspaceCreateOptions::default)?;
        Ok(Self {
            path,
            db,
            blobs,
        })
    }

    fn load_blob<T: StrictDeserialize>(&self, key: &'static [u8]) -> Result<T, PersistenceError> {
        let bytes = self
            .blobs
            .get(key)
            .map_err(PersistenceError::with)?
            .ok_or_else(|| {
                PersistenceError::with(io::Error::new(
                    io::ErrorKind::NotFound,
                    format!("RGB Fjall blob is missing: {}", String::from_utf8_lossy(key)),
                ))
            })?;
        let confined = Confined::try_from(bytes.as_ref().to_vec()).map_err(PersistenceError::with)?;
        T::from_strict_serialized::<U32MAX>(confined).map_err(PersistenceError::with)
    }

    fn store_blob<T: StrictSerialize>(
        &self,
        key: &'static [u8],
        object: &T,
    ) -> Result<(), PersistenceError> {
        let bytes = object
            .to_strict_serialized::<U32MAX>()
            .map_err(PersistenceError::with)?
            .release();
        let mut tx = self.db.write_tx();
        tx.insert(&self.blobs, key, bytes);
        tx.commit().map_err(PersistenceError::with)?;
        self.db
            .persist(PersistMode::SyncAll)
            .map_err(PersistenceError::with)
    }

    pub fn has_data(&self) -> Result<bool, PersistenceError> {
        let stash = self
            .blobs
            .get(KEY_STASH)
            .map_err(PersistenceError::with)?;
        let state = self
            .blobs
            .get(KEY_STATE)
            .map_err(PersistenceError::with)?;
        Ok(stash.as_ref().is_some_and(|bytes| bytes.len() > 128)
            && state.as_ref().is_some_and(|bytes| bytes.len() > 128))
    }
}

impl PersistenceProvider<MemStash> for FjallBinStore {
    fn load(&self) -> Result<MemStash, PersistenceError> { self.load_blob(KEY_STASH) }

    fn store(&self, object: &MemStash) -> Result<(), PersistenceError> {
        self.store_blob(KEY_STASH, object)
    }
}

impl PersistenceProvider<MemState> for FjallBinStore {
    fn load(&self) -> Result<MemState, PersistenceError> { self.load_blob(KEY_STATE) }

    fn store(&self, object: &MemState) -> Result<(), PersistenceError> {
        self.store_blob(KEY_STATE, object)
    }
}

impl PersistenceProvider<MemIndex> for FjallBinStore {
    fn load(&self) -> Result<MemIndex, PersistenceError> { self.load_blob(KEY_INDEX) }

    fn store(&self, object: &MemIndex) -> Result<(), PersistenceError> {
        self.store_blob(KEY_INDEX, object)
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::time::{SystemTime, UNIX_EPOCH};

    use crate::persistence::{MemIndex, MemStash, MemState, Stock};

    use super::FjallBinStore;

    #[test]
    fn persists_empty_stock_roundtrip() {
        let suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!("rgb-fjall-store-{suffix}"));
        let store = FjallBinStore::new(path.clone()).expect("open fjall store");

        let mut stock = Stock::in_memory();
        stock
            .make_persistent(store.clone(), true)
            .expect("persist stock");
        stock.store().expect("store stock");

        Stock::<MemStash, MemState, MemIndex>::load(store, true).expect("load stock");

        let _ = fs::remove_dir_all(path);
    }
}
