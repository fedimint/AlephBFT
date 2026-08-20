use super::{
    create_unit_for_config, keep_processing_units_until_for_config,
    wait_until_unit_creation_is_open, Config, Creator, GATED_PATH_ENTRIES,
};
use crate::{
    testing::{gen_config, gen_delay_config},
    units::preunit_to_unit,
    NodeCount, NodeIndex, UnitCreationGate,
};
use aleph_bft_mock::Hasher64;
use futures::channel::mpsc;
use futures_timer::Delay;
use std::time::Duration;

#[tokio::test]
async fn gate_free_config_selects_original_creator_path() {
    let config: Config = gen_config(NodeIndex(0), NodeCount(1), gen_delay_config()).into();
    assert!(config.unit_creation_gate.is_none());

    GATED_PATH_ENTRIES.set(0);
    let mut creator = Creator::<Hasher64>::new(NodeIndex(0), NodeCount(1));
    let (_parents_tx, mut parents_rx) = mpsc::unbounded();
    assert!(
        create_unit_for_config(0, &mut creator, &mut parents_rx, None)
            .await
            .is_ok()
    );
    assert!(keep_processing_units_until_for_config(
        &mut creator,
        &mut parents_rx,
        Delay::new(Duration::ZERO),
        None,
    )
    .await
    .is_ok());
    assert_eq!(
        GATED_PATH_ENTRIES.get(),
        0,
        "gate-free dispatch must not enter helpers containing gate selects or cooperative yields"
    );

    let config: Config = gen_config(NodeIndex(0), NodeCount(1), gen_delay_config())
        .with_unit_creation_gate(UnitCreationGate::new())
        .into();
    assert!(config.unit_creation_gate.is_some());

    let mut creator = Creator::<Hasher64>::new(NodeIndex(0), NodeCount(1));
    assert!(create_unit_for_config(
        0,
        &mut creator,
        &mut parents_rx,
        config.unit_creation_gate.as_ref(),
    )
    .await
    .is_ok());
    assert_eq!(
        GATED_PATH_ENTRIES.get(),
        1,
        "explicit gate dispatch must enter the gated helper"
    );
}

#[tokio::test]
async fn closed_gate_keeps_processing_parent_notifications() {
    let gate = UnitCreationGate::new();
    gate.close();
    let mut source = Creator::<Hasher64>::new(NodeIndex(0), NodeCount(1));
    let mut latest_unit = None;
    for round in 0..=3 {
        let (preunit, _) = source.create_unit(round).unwrap();
        let unit = preunit_to_unit(preunit, 0);
        source.add_unit(&unit);
        latest_unit = Some(unit);
    }

    let (parents_tx, mut parents_rx) = mpsc::unbounded();
    parents_tx.unbounded_send(latest_unit.unwrap()).unwrap();
    let mut creator = Creator::<Hasher64>::new(NodeIndex(0), NodeCount(1));

    assert!(
        tokio::time::timeout(
            Duration::from_millis(50),
            wait_until_unit_creation_is_open(&mut creator, &mut parents_rx, &gate),
        )
        .await
        .is_err(),
        "the closed gate should keep waiting"
    );
    assert_eq!(creator.current_round(), 3);
}
