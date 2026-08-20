use super::UnitCreationGate;
use futures::{pin_mut, FutureExt};
use std::{task::Poll, time::Duration};

#[tokio::test]
async fn defaults_to_open() {
    UnitCreationGate::default()
        .wait_until_open()
        .now_or_never()
        .expect("the default gate should not block");
}

#[tokio::test]
async fn reopening_wakes_a_waiter() {
    let gate = UnitCreationGate::new();
    gate.close();
    let waiter_gate = gate.clone();
    let waiter = tokio::spawn(async move {
        waiter_gate.wait_until_open().await;
    });
    tokio::time::timeout(Duration::from_secs(1), async {
        while !gate.has_registered_waiter() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("waiter should register while the gate is closed");
    assert!(!waiter.is_finished());

    gate.open();
    tokio::time::timeout(Duration::from_secs(1), waiter)
        .await
        .expect("opening should wake the registered wait")
        .unwrap();
}

#[tokio::test]
async fn wait_is_cancellation_safe() {
    let gate = UnitCreationGate::new();
    gate.close();
    {
        let cancelled_wait = gate.wait_until_open();
        pin_mut!(cancelled_wait);
        assert!(matches!(futures::poll!(&mut cancelled_wait), Poll::Pending));
    }

    let replacement_wait = gate.wait_until_open();
    pin_mut!(replacement_wait);
    assert!(matches!(
        futures::poll!(&mut replacement_wait),
        Poll::Pending
    ));
    gate.open();
    tokio::time::timeout(Duration::from_secs(1), replacement_wait)
        .await
        .expect("cancelling a prior wait should not consume reopening");
}
