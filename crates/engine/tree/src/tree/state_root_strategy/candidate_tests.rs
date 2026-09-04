use super::*;
use alloy_primitives::{Address, U256};
use reth_chainspec::ChainSpec;
use reth_db_common::init::init_genesis;
use reth_ethereum_primitives::EthPrimitives;
use reth_provider::{test_utils::create_test_provider_factory_with_chain_spec, HeaderProvider};
use revm::state::{AccountInfo, AccountStatus, EvmState, EvmStorageSlot, TransactionId};
use std::time::Duration;

fn submit(handle: &mut StateRootHandle, balance: u64) {
    let mut state = EvmState::default();
    let mut account = revm::state::Account::default();
    account.info = AccountInfo { balance: U256::from(balance), ..Default::default() };
    account.status = AccountStatus::Touched;
    account.storage.insert(
        U256::from(1),
        EvmStorageSlot::new_changed(U256::ZERO, U256::from(balance * 10), TransactionId::ZERO),
    );
    state.insert(Address::with_last_byte(42), account);
    let mut hook = handle.take_execution_hook();
    hook.on_state(state);
    drop(hook);
}

#[test]
fn idle_launchers_do_not_starve_validation() {
    static RUNTIME: std::sync::LazyLock<reth_tasks::Runtime> =
        std::sync::LazyLock::new(reth_tasks::Runtime::test);
    let runtime = RUNTIME.clone();
    let chain = Arc::new(ChainSpec::default());
    let factory = create_test_provider_factory_with_chain_spec(chain);
    let hash = init_genesis(&factory).unwrap();
    let parent = factory.sealed_header(0).unwrap().unwrap();
    let manager = OverlayManager::<EthPrimitives>::default();
    let overlay = OverlayStateProviderFactory::new(factory, manager.overlay_builder(hash));
    let strategy = DefaultStateRootStrategy::default();
    let launch = || {
        strategy.candidate_payload_builder_launcher::<EthPrimitives, _>(
            &runtime,
            hash,
            parent.header(),
            overlay.clone(),
            &TreeConfig::default(),
            PayloadBuilderLease::new(()),
        )
    };
    let first = launch();
    let mut first_job = first.start();
    submit(&mut first_job, 1);
    let first_rx = first_job.take_state_root_rx();
    assert!(first_rx.recv_timeout(Duration::from_secs(3)).unwrap().is_ok());
    drop(first_job);
    let second = launch();
    let mut second_job = second.start();
    submit(&mut second_job, 2);
    let second_rx = second_job.take_state_root_rx();
    assert!(second_rx.recv_timeout(Duration::from_secs(3)).unwrap().is_ok());
    drop(second_job);

    let mut validation = strategy.spawn_state_root(
        &runtime,
        Some(&manager),
        ProofWorkers::Fresh(overlay),
        StateRootTaskOptions {
            parent_header: parent,
            preserved_sparse_trie: None,
            transaction_count: Some(31),
            config: &TreeConfig::default(),
            pending_sparse_trie_prune_blocks: None,
        },
    );
    submit(&mut validation, 3);
    let rx = validation.take_state_root_rx();
    rx.recv_timeout(Duration::from_secs(3)).expect("idle launchers blocked validation").unwrap();
    drop((first, second, validation));
}

#[derive(Clone)]
struct ObservedFactory<F> {
    inner: F,
    opened: crossbeam_channel::Sender<()>,
    release: Option<crossbeam_channel::Receiver<()>>,
}
impl<F: DatabaseProviderFactory> DatabaseProviderFactory for ObservedFactory<F> {
    type DB = F::DB;
    type Provider = F::Provider;
    type ProviderRW = F::ProviderRW;
    fn database_provider_ro(&self) -> ProviderResult<Self::Provider> {
        self.opened.send(()).unwrap();
        if let Some(release) = &self.release {
            release.recv().unwrap();
            Err(ProviderError::other(std::io::Error::other("injected database-open failure")))
        } else {
            self.inner.database_provider_ro()
        }
    }
    fn database_provider_rw(&self) -> ProviderResult<Self::ProviderRW> {
        self.inner.database_provider_rw()
    }
}

#[test]
fn provider_open_failure_reaches_candidate() {
    static RUNTIME: std::sync::LazyLock<reth_tasks::Runtime> =
        std::sync::LazyLock::new(reth_tasks::Runtime::test);
    let runtime = RUNTIME.clone();
    let chain = Arc::new(ChainSpec::default());
    let factory = create_test_provider_factory_with_chain_spec(chain);
    let hash = init_genesis(&factory).unwrap();
    let parent = factory.sealed_header(0).unwrap().unwrap();
    let manager = OverlayManager::<EthPrimitives>::default();
    let (opened_tx, opened_rx) = crossbeam_channel::unbounded();
    let (release_tx, release_rx) = crossbeam_channel::unbounded();
    let overlay = OverlayStateProviderFactory::new(
        ObservedFactory { inner: factory, opened: opened_tx, release: Some(release_rx) },
        manager.overlay_builder(hash),
    );
    let launcher = DefaultStateRootStrategy::default()
        .candidate_payload_builder_launcher::<EthPrimitives, _>(
            &runtime,
            hash,
            parent.header(),
            overlay,
            &TreeConfig::default(),
            PayloadBuilderLease::new(()),
        );
    let mut job = launcher.start();
    submit(&mut job, 1);
    let rx = job.take_state_root_rx();
    opened_rx.recv_timeout(Duration::from_secs(3)).unwrap();
    assert!(rx.try_recv().is_err());
    release_tx.send(()).unwrap();
    let error =
        rx.recv_timeout(Duration::from_secs(3)).expect("provider error was lost").unwrap_err();
    assert!(matches!(error, StateRootTaskError::Provider(_)));
    let mut next = launcher.start();
    submit(&mut next, 2);
    assert!(next.take_state_root_rx().recv_timeout(Duration::from_secs(3)).unwrap().is_err());
    assert!(opened_rx.is_empty(), "a failed parent view must report its cached error");
}

#[test]
fn concurrent_candidates_match_serial_roots_and_cancel_independently() {
    static RUNTIME: std::sync::LazyLock<reth_tasks::Runtime> =
        std::sync::LazyLock::new(reth_tasks::Runtime::test);
    let runtime = RUNTIME.clone();
    let chain = Arc::new(ChainSpec::default());
    let factory = create_test_provider_factory_with_chain_spec(chain);
    let hash = init_genesis(&factory).unwrap();
    let parent = factory.sealed_header(0).unwrap().unwrap();
    let manager = OverlayManager::<EthPrimitives>::default();
    let (opened_tx, opened_rx) = crossbeam_channel::unbounded();
    let overlay = OverlayStateProviderFactory::new(
        ObservedFactory { inner: factory, opened: opened_tx, release: None },
        manager.overlay_builder(hash),
    );
    struct Lease(std::sync::mpsc::Sender<()>);
    impl Drop for Lease {
        fn drop(&mut self) {
            let _ = self.0.send(());
        }
    }
    let (lease_tx, lease_rx) = std::sync::mpsc::channel();
    let launcher = DefaultStateRootStrategy::default()
        .candidate_payload_builder_launcher::<EthPrimitives, _>(
            &runtime,
            hash,
            parent.header(),
            overlay,
            &TreeConfig::default(),
            PayloadBuilderLease::new(Lease(lease_tx)),
        );
    let mut canceled = launcher.start();
    let unfinished_hook = canceled.take_execution_hook();
    let canceled_rx = canceled.take_state_root_rx();
    let jobs: Vec<_> = (1..=8)
        .map(|balance| {
            let mut job = launcher.start();
            submit(&mut job, balance);
            let rx = job.take_state_root_rx();
            (balance, job, rx)
        })
        .collect();
    drop(canceled);
    assert!(matches!(
        canceled_rx.recv_timeout(Duration::from_secs(3)).unwrap(),
        Err(StateRootTaskError::Canceled)
    ));
    drop(unfinished_hook);
    for (balance, _job, rx) in jobs {
        let outcome = rx.recv_timeout(Duration::from_secs(3)).unwrap().unwrap();
        let expected = reth_trie::test_utils::state_root([(
            Address::with_last_byte(42),
            (
                reth_primitives_traits::Account {
                    balance: U256::from(balance),
                    ..Default::default()
                },
                [(B256::from(U256::from(1)), U256::from(balance * 10))],
            ),
        )]);
        assert_eq!(outcome.state_root, expected);
        assert_eq!(Arc::strong_count(&outcome.hashed_state), 1);
        assert_eq!(Arc::strong_count(&outcome.trie_updates), 1);
    }
    assert_eq!(opened_rx.len(), runtime.cpu_pool().current_num_threads());
    let mut last = launcher.start();
    let hook = last.take_execution_hook();
    let result = last.take_state_root_rx();
    drop(launcher);
    assert!(lease_rx.try_recv().is_err(), "an active job must retain its pause");
    drop(last);
    assert!(matches!(
        result.recv_timeout(Duration::from_secs(3)).unwrap(),
        Err(StateRootTaskError::Canceled)
    ));
    drop(hook);
    lease_rx.recv_timeout(Duration::from_secs(3)).expect("canceled job leaked its pause");
}

#[test]
fn populated_parent_matches_serial_root() {
    static RUNTIME: std::sync::LazyLock<reth_tasks::Runtime> =
        std::sync::LazyLock::new(reth_tasks::Runtime::test);
    let mut genesis = ChainSpec::<alloy_consensus::Header>::default().genesis;
    let mut updates = EvmState::default();
    let mut expected = Vec::new();
    for i in 1u64..=128 {
        let address = Address::from_word(B256::from(U256::from(i)));
        let parent = genesis.alloc.entry(address).or_default();
        parent.balance = U256::from(100);
        parent.storage = Some(
            (1u64..=16)
                .map(|slot| (B256::from(U256::from(slot)), B256::from(U256::from(slot))))
                .collect(),
        );
        let mut account = revm::state::Account::default();
        account.info = AccountInfo { balance: U256::from(101), ..Default::default() };
        account.status = AccountStatus::Touched;
        for slot in 1u64..=8 {
            account.storage.insert(
                U256::from(slot),
                EvmStorageSlot::new_changed(
                    U256::from(slot),
                    U256::from(slot + 1),
                    TransactionId::ZERO,
                ),
            );
        }
        updates.insert(address, account);
        let storage: Vec<_> = (1u64..=16)
            .map(|slot| {
                (B256::from(U256::from(slot)), U256::from(if slot <= 8 { slot + 1 } else { slot }))
            })
            .collect();
        expected.push((
            address,
            (
                reth_primitives_traits::Account { balance: U256::from(101), ..Default::default() },
                storage,
            ),
        ));
    }
    let chain = Arc::new(ChainSpec::from_genesis(genesis));
    let factory = create_test_provider_factory_with_chain_spec(chain);
    let hash = init_genesis(&factory).unwrap();
    let parent = factory.sealed_header(0).unwrap().unwrap();
    let manager = OverlayManager::<EthPrimitives>::default();
    let launcher = DefaultStateRootStrategy::default()
        .candidate_payload_builder_launcher::<EthPrimitives, _>(
            &RUNTIME,
            hash,
            parent.header(),
            OverlayStateProviderFactory::new(factory, manager.overlay_builder(hash)),
            &TreeConfig::default(),
            PayloadBuilderLease::new(()),
        );
    let mut job = launcher.start();
    let mut hook = job.take_execution_hook();
    hook.on_state(updates);
    drop(hook);
    let outcome = job.take_state_root_rx().recv_timeout(Duration::from_secs(5)).unwrap().unwrap();
    assert_eq!(outcome.state_root, reth_trie::test_utils::state_root(expected));
}
