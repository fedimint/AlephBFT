use futures::{future::poll_fn, task::AtomicWaker};
use std::{
    fmt,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    task::Poll,
};

/// A dynamically controlled gate for local unit creation.
///
/// A gate starts open. Clones refer to the same state, so a caller can put one
/// clone in [`crate::Config`] and retain another clone to pause or resume unit
/// creation. The gate controls only locally created units and does not stop
/// processing units received from other nodes.
///
/// The creator checks the gate immediately before constructing each
/// `PreUnit`. While it remains closed, the local node does not
/// acquire data for, sign, back up, or broadcast a new unit, but it continues
/// processing parent notifications and remains responsive to termination.
///
/// Create a distinct gate for each concurrently live AlephBFT session. Sharing
/// one gate between live sessions is unsupported because the gate stores only
/// one waiting task's waker.
///
/// `close` is not an acknowledgement barrier: a unit that already passed the
/// gate may complete. Internally, waiting is cancellation-safe and `open`
/// wakes the creator promptly without consuming gate state.
#[derive(Clone)]
pub struct UnitCreationGate {
    /// State shared by the consensus task and its controller.
    inner: Arc<Inner>,
}

struct Inner {
    /// Whether unit creation may proceed.
    open: AtomicBool,
    /// Wakes the creator after the gate opens.
    waker: AtomicWaker,
    /// Records that a test has observed a pending gate wait.
    #[cfg(test)]
    waiter_registered: AtomicBool,
}

impl UnitCreationGate {
    /// Creates an open unit-creation gate.
    pub fn new() -> Self {
        Self::default()
    }

    /// Prevents subsequent local unit creation.
    ///
    /// A unit whose gate wait already completed may still be created. This API
    /// does not provide a strict pause barrier.
    pub fn close(&self) {
        self.inner.open.store(false, Ordering::Release);
    }

    /// Allows local unit creation and wakes a creator waiting at the gate.
    pub fn open(&self) {
        self.inner.open.store(true, Ordering::Release);
        self.inner.waker.wake();
    }

    /// Returns whether the gate is currently open.
    pub fn is_open(&self) -> bool {
        self.inner.open.load(Ordering::Acquire)
    }

    /// Waits until the gate is open.
    ///
    /// This wait is cancellation-safe: dropping it does not consume a wakeup
    /// or modify gate state. Closing and reopening the gate wakes a waiting
    /// creator promptly.
    pub(crate) async fn wait_until_open(&self) {
        poll_fn(|cx| {
            if self.is_open() {
                return Poll::Ready(());
            }

            self.inner.waker.register(cx.waker());
            #[cfg(test)]
            self.inner.waiter_registered.store(true, Ordering::Release);
            if self.is_open() {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        })
        .await
    }
}

impl Default for UnitCreationGate {
    fn default() -> Self {
        Self {
            inner: Arc::new(Inner {
                open: AtomicBool::new(true),
                waker: AtomicWaker::new(),
                #[cfg(test)]
                waiter_registered: AtomicBool::new(false),
            }),
        }
    }
}

impl fmt::Debug for UnitCreationGate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("UnitCreationGate")
            .field("open", &self.is_open())
            .finish()
    }
}

#[cfg(test)]
impl UnitCreationGate {
    pub(crate) fn has_registered_waiter(&self) -> bool {
        self.inner.waiter_registered.load(Ordering::Acquire)
    }
}

#[cfg(test)]
mod tests;
