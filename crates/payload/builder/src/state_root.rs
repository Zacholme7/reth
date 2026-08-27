//! Independent payload state-root resources.

use alloy_primitives::B256;
#[cfg(any(test, feature = "test-utils"))]
use reth_trie_parallel::state_root_task::noop_state_root_streams;
use reth_trie_parallel::{
    error::StateRootTaskError,
    state_root_task::{StateRootHintStream, StateRootTaskCancelGuard, StateRootUpdateHook},
};
use std::sync::Arc;
use tokio::sync::oneshot;

pub use reth_trie_parallel::state_root_task::StateRootComputeOutcome;

/// Cloneable launcher for independent state-root jobs.
#[derive(Clone)]
pub struct PayloadStateRootJobLauncher {
    start: Arc<dyn Fn() -> Result<PayloadStateRootJob, StateRootTaskError> + Send + Sync>,
}

impl std::fmt::Debug for PayloadStateRootJobLauncher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PayloadStateRootJobLauncher").finish_non_exhaustive()
    }
}

impl PayloadStateRootJobLauncher {
    /// Creates a launcher from an engine-owned synchronous constructor.
    #[doc(hidden)]
    pub fn new(
        start: impl Fn() -> Result<PayloadStateRootJob, StateRootTaskError> + Send + Sync + 'static,
    ) -> Self {
        Self { start: Arc::new(start) }
    }

    /// Starts one independent state-root job against the launcher's frozen parent view.
    pub fn start(&self) -> Result<PayloadStateRootJob, StateRootTaskError> {
        (self.start)()
    }
}

/// One running independent state-root job.
#[derive(Debug)]
pub struct PayloadStateRootJob {
    hook: Option<StateRootUpdateHook>,
    hint: Option<StateRootHintStream>,
    result: oneshot::Receiver<Result<(B256, StateRootComputeOutcome), StateRootTaskError>>,
    _cancel_guard: StateRootTaskCancelGuard,
}

impl PayloadStateRootJob {
    /// Creates the high-level payload job from low-level state-root task capabilities.
    #[doc(hidden)]
    pub const fn new(
        hook: StateRootUpdateHook,
        hint: StateRootHintStream,
        result: oneshot::Receiver<Result<(B256, StateRootComputeOutcome), StateRootTaskError>>,
        cancel_guard: StateRootTaskCancelGuard,
    ) -> Self {
        Self { hook: Some(hook), hint: Some(hint), result, _cancel_guard: cancel_guard }
    }

    /// Creates a ready state-root job for payload-builder tests.
    #[cfg(any(test, feature = "test-utils"))]
    pub fn prepared_for_test(parent_hash: B256, outcome: StateRootComputeOutcome) -> Self {
        let (updates, hint) = noop_state_root_streams();
        let (result_tx, result) = oneshot::channel();
        result_tx.send(Ok((parent_hash, outcome))).expect("new test result receiver is open");
        let (cancel_guard, _cancel_rx) = StateRootTaskCancelGuard::channel();
        Self::new(updates.into_state_hook(), hint, result, cancel_guard)
    }

    /// Takes the single authoritative execution hook.
    ///
    /// # Panics
    ///
    /// If the hook was already taken.
    pub const fn take_state_hook(&mut self) -> StateRootUpdateHook {
        self.hook.take().expect("candidate state root hook already taken")
    }

    /// Takes the single best-effort state-access hint stream.
    ///
    /// # Panics
    ///
    /// If the stream was already taken.
    pub const fn take_hint_stream(&mut self) -> StateRootHintStream {
        self.hint.take().expect("candidate state root hint stream already taken")
    }

    /// Awaits the state-root result after execution relinquishes its hook.
    ///
    /// # Panics
    ///
    /// If the authoritative hook was not taken by the caller.
    pub async fn finish(self) -> Result<(B256, StateRootComputeOutcome), StateRootTaskError> {
        assert!(self.hook.is_none(), "candidate state root hook was not taken");
        self.result.await.map_err(|_| {
            StateRootTaskError::Other("candidate state root task dropped".to_string())
        })?
    }
}
