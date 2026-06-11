use std::collections::HashMap;

use backon::{BlockingRetryable, ExponentialBuilder};
use rgbstd::{
    Txid,
    bitcoin::{Transaction as Tx, consensus},
    containers::{Consignment, PubWitness},
    validation::{ResolveWitness, WitnessResolverError, WitnessStatus},
    vm::{WitnessOrd, WitnessPos},
};

// TODO: maybe remove this resolver.
#[derive(Default, Debug)]
pub struct LnResolver {
    // Local known on-chain txs.
    // txid => (tx, (height, timestamp))
    local_txs: HashMap<Txid, (Tx, WitnessPos)>,

    // Channel state tx
    active_tx: Option<Tx>,
    archived_txs: HashMap<Txid, Tx>,
}

impl LnResolver {
    pub fn new() -> Self { Self::default() }

    // Timestamp must be greater than or equal to 1231006505
    pub fn add_onchain_tx(&mut self, consensus_serialized_tx: &[u8], height: u32, timestamp: i64) {
        let height = std::num::NonZeroU32::new(height).unwrap();
        let tx: Tx = consensus::deserialize(consensus_serialized_tx).unwrap();
        let witness_pos = WitnessPos::bitcoin(height, timestamp).unwrap();
        self.local_txs
            .insert(tx.compute_txid(), (tx, witness_pos));
    }

    pub fn replace_active(&mut self, consensus_serialized_tx: &[u8]) {
        let tx: Tx = consensus::deserialize(consensus_serialized_tx).unwrap();

        if let Some(old) = self.active_tx.replace(tx) {
            let old_txid = old.compute_txid();
            self.archived_txs.insert(old_txid, old);
        }
    }

    pub fn get_consensus_serialized_active_tx(&self) -> Option<Vec<u8>> {
        self.active_tx
            .as_ref()
            .map(consensus::serialize)
    }
}

impl ResolveWitness for LnResolver {
    fn resolve_witness(&self, witness_id: Txid) -> Result<WitnessStatus, WitnessResolverError> {
        if let Some((tx, witness_pos)) = self.local_txs.get(&witness_id) {
            return Ok(WitnessStatus::Resolved(
                tx.clone(),
                WitnessOrd::Mined(*witness_pos),
            ));
        }

        if let Some(ref tx) = self.active_tx {
            if tx.compute_txid() == witness_id {
                return Ok(WitnessStatus::Resolved(tx.clone(), WitnessOrd::Tentative));
            }
        }

        if let Some(tx) = self.archived_txs.get(&witness_id) {
            return Ok(WitnessStatus::Resolved(tx.clone(), WitnessOrd::Archived));
        }

        Ok(WitnessStatus::Unresolved)
    }

    fn check_chain_net(&self, _chain_net: rgbstd::ChainNet) -> Result<(), WitnessResolverError> {
        Ok(())
    }
}

#[derive(Default, Debug)]
pub struct LocalResolver {
    terminal_txes: HashMap<Txid, Tx>,
}

impl LocalResolver {
    pub fn new() -> Self { Self::default() }

    pub fn add_witness(&mut self, witness: Tx) {
        self.terminal_txes.insert(witness.compute_txid(), witness);
    }

    pub fn add_terminals<const TYPE: bool>(&mut self, consignment: &Consignment<TYPE>) {
        self.terminal_txes.extend(
            consignment
                .bundles
                .iter()
                .filter_map(|bw| match bw.pub_witness.clone() {
                    PubWitness::Tx(tx) => Some((tx.compute_txid(), tx)),
                    _ => None,
                }),
        );
    }
}

impl ResolveWitness for LocalResolver {
    fn resolve_witness(&self, witness_id: Txid) -> Result<WitnessStatus, WitnessResolverError> {
        match self.terminal_txes.get(&witness_id) {
            Some(tx) => Ok(WitnessStatus::Resolved(tx.clone(), WitnessOrd::Tentative)),
            None => Ok(WitnessStatus::Unresolved),
        }
    }

    fn check_chain_net(&self, _chain_net: rgbstd::ChainNet) -> Result<(), WitnessResolverError> {
        Ok(())
    }
}

#[derive(Default, Debug)]
pub struct WithLocalResolver<T: ResolveWitness> {
    inner: T,
    terminal_txes: HashMap<Txid, Tx>,
}

impl<T: ResolveWitness> WithLocalResolver<T> {
    pub fn new(inner: T) -> Self {
        Self {
            inner,
            terminal_txes: HashMap::new(),
        }
    }

    pub fn add_witness(&mut self, witness: Tx) {
        self.terminal_txes.insert(witness.compute_txid(), witness);
    }

    pub fn add_terminals<const TYPE: bool>(&mut self, consignment: &Consignment<TYPE>) {
        self.terminal_txes.extend(
            consignment
                .bundles
                .iter()
                .filter_map(|bw| match bw.pub_witness.clone() {
                    PubWitness::Tx(tx) => Some((tx.compute_txid(), tx)),
                    _ => None,
                }),
        );
    }

    pub fn add_pending_tx_from_consignment<const TYPE: bool>(
        &mut self,
        txid: Txid,
        consignment: &Consignment<TYPE>,
    ) {
        self.terminal_txes.extend(
            consignment
                .bundles
                .iter()
                .find_map(|bw| match bw.pub_witness.clone() {
                    PubWitness::Tx(tx) if tx.compute_txid() == txid => {
                        Some((tx.compute_txid(), tx))
                    }
                    _ => None,
                }),
        );
    }
}

impl<T: ResolveWitness> ResolveWitness for WithLocalResolver<T> {
    fn resolve_witness(&self, witness_id: Txid) -> Result<WitnessStatus, WitnessResolverError> {
        match self.terminal_txes.get(&witness_id) {
            Some(tx) => Ok(WitnessStatus::Resolved(tx.clone(), WitnessOrd::Tentative)),
            None => self.inner.resolve_witness(witness_id),
        }
    }

    fn check_chain_net(&self, chain_net: rgbstd::ChainNet) -> Result<(), WitnessResolverError> {
        self.inner.check_chain_net(chain_net)
    }
}

#[derive(Debug)]
pub enum GlobalResolver {
    Online(OnlineResolver),
    Local(LocalResolver),
}

impl GlobalResolver {
    pub fn new_online(esplora_url: &str) -> Self {
        Self::Online(OnlineResolver::new(esplora_url))
    }

    pub fn new_local(local_resolver: LocalResolver) -> Self { Self::Local(local_resolver) }
}

impl ResolveWitness for GlobalResolver {
    fn resolve_witness(&self, witness_id: Txid) -> Result<WitnessStatus, WitnessResolverError> {
        match self {
            Self::Online(resolver) => resolver.resolve_witness(witness_id),
            Self::Local(resolver) => resolver.resolve_witness(witness_id),
        }
    }

    fn check_chain_net(&self, chain_net: rgbstd::ChainNet) -> Result<(), WitnessResolverError> {
        match self {
            Self::Online(resolver) => resolver.check_chain_net(chain_net),
            Self::Local(resolver) => resolver.check_chain_net(chain_net),
        }
    }
}

#[derive(Debug)]
pub struct OnlineResolver {
    // TODO
    #[allow(unused)]
    esplora_url: String,
    client: esplora_client::BlockingClient,
}

impl OnlineResolver {
    pub fn new(esplora_url: &str) -> Self {
        let builder = esplora_client::Builder::new(esplora_url);

        Self {
            esplora_url: esplora_url.to_string(),
            client: builder.build_blocking(),
        }
    }
}

impl ResolveWitness for OnlineResolver {
    fn resolve_witness(&self, witness_id: Txid) -> Result<WitnessStatus, WitnessResolverError> {
        let txid = witness_id.to_string().parse().unwrap();

        let op = || {
            let tx_opt = self
                .client
                .get_tx(&txid)
                .map_err(|e| WitnessResolverError::ResolverIssue(Some(witness_id), e.to_string()))?;
            let Some(tx) = tx_opt else {
                return Ok(WitnessStatus::Unresolved);
            };
            let status = self
                .client
                .get_tx_status(&txid)
                .map_err(|e| WitnessResolverError::ResolverIssue(Some(witness_id), e.to_string()))?;
            let ord = match status.block_height.and_then(|h| status.block_time.map(|t| (h, t))) {
                Some((h, t)) => {
                    let h = std::num::NonZeroU32::new(h)
                        .ok_or(WitnessResolverError::InvalidResolverData)?;
                    let pos = WitnessPos::bitcoin(h, t as i64)
                        .ok_or(WitnessResolverError::InvalidResolverData)?;
                    WitnessOrd::Mined(pos)
                }
                None => WitnessOrd::Tentative,
            };
            Ok(WitnessStatus::Resolved(tx, ord))
        };

        op.retry(default_backoff()).call()
    }

    fn check_chain_net(&self, _chain_net: rgbstd::ChainNet) -> Result<(), WitnessResolverError> {
        Ok(())
    }
}

/// Unchecked fascia resolver
pub struct FasciaResolver;

impl ResolveWitness for FasciaResolver {
    fn resolve_witness(&self, _witness_id: Txid) -> Result<WitnessStatus, WitnessResolverError> {
        Ok(WitnessStatus::Unresolved)
    }

    fn check_chain_net(&self, _chain_net: rgbstd::ChainNet) -> Result<(), WitnessResolverError> {
        Ok(())
    }
}

impl rgbstd::validation::WitnessOrdProvider for FasciaResolver {
    fn witness_ord(&self, _witness_id: Txid) -> Result<WitnessOrd, WitnessResolverError> {
        Ok(WitnessOrd::Tentative)
    }
}

fn default_backoff() -> ExponentialBuilder { ExponentialBuilder::default() }
