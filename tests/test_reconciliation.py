"""Tests for the three batches, the prefix guard rail and the caps."""

import pytest

from integrations.reconciliation import (
    DeletionsWithoutPrefixError,
    IdentifierOutsidePrefixError,
    assert_identifiers_in_prefix,
    reconcile,
)
from integrations.state import compute_regulation_digest
from tests.sync_fixtures import build_regulation

PREFIX = "PARIS-EUDO-"


def digests(*specs) -> dict:
    """`digests(("PARIS-EUDO-1", 30), "PARIS-EUDO-2")` — identifier, optional speed."""
    produced = {}
    for spec in specs:
        identifier, speed = spec if isinstance(spec, tuple) else (spec, 30)
        produced[identifier] = compute_regulation_digest(
            build_regulation(identifier, max_speed=speed)
        )
    return produced


def test_the_three_operations_are_computed_at_once():
    produced = digests(f"{PREFIX}new", (f"{PREFIX}changed", 30), f"{PREFIX}same")
    snapshot = digests((f"{PREFIX}changed", 50), f"{PREFIX}same")

    plan = reconcile(
        produced,
        [f"{PREFIX}changed", f"{PREFIX}same", f"{PREFIX}gone"],
        snapshot,
        identifier_prefix=PREFIX,
        update_mode="changed",
        delete_missing=True,
    )

    assert plan.creations.identifiers == (f"{PREFIX}new",)
    assert plan.updates.identifiers == (f"{PREFIX}changed",)
    assert plan.deletions.identifiers == (f"{PREFIX}gone",)
    assert plan.unchanged == (f"{PREFIX}same",)


def test_a_regulation_outside_our_prefix_is_never_deleted():
    # The organization also receives regulations from channels outside this repository.
    plan = reconcile(
        digests(f"{PREFIX}kept"),
        [f"{PREFIX}kept", "LYON_12345", "2023T15201"],
        {},
        identifier_prefix=PREFIX,
        delete_missing=True,
    )

    assert plan.deletions.identifiers == ()
    assert plan.remote_total == 3
    assert plan.remote_in_prefix == 1


def test_deleting_without_a_prefix_is_refused_not_merely_disabled():
    with pytest.raises(DeletionsWithoutPrefixError):
        reconcile(digests("A-1"), ["A-1", "A-2"], {}, delete_missing=True)


def test_an_organization_that_did_not_opt_in_deletes_nothing():
    plan = reconcile(digests("A-1"), ["A-1", "A-2"], None)

    assert plan.deletions.identifiers == ()
    assert plan.creations.identifiers == ()


def test_a_produced_identifier_outside_the_prefix_stops_everything():
    with pytest.raises(IdentifierOutsidePrefixError):
        reconcile(
            digests(f"{PREFIX}ok", "2023T15201"),
            [],
            {},
            identifier_prefix=PREFIX,
        )


def test_a_doubled_prefix_is_caught_by_the_guard_rail():
    # `PARIS-EUDO-PARIS-EUDO-x` does start with the prefix, but a forgotten one does not.
    assert_identifiers_in_prefix([f"{PREFIX}x"], PREFIX)
    with pytest.raises(IdentifierOutsidePrefixError):
        assert_identifiers_in_prefix([f"{PREFIX}x", "x"], PREFIX)


def test_without_a_snapshot_nothing_is_updated():
    produced = digests((f"{PREFIX}a", 30))

    plan = reconcile(
        produced,
        [f"{PREFIX}a"],
        None,
        identifier_prefix=PREFIX,
        update_mode="changed",
    )

    assert plan.updates.identifiers == ()
    assert plan.unchanged == (f"{PREFIX}a",)
    assert plan.snapshot_present is False


def test_a_regulation_missing_from_the_snapshot_is_not_updated():
    plan = reconcile(
        digests((f"{PREFIX}a", 30), (f"{PREFIX}b", 30)),
        [f"{PREFIX}a", f"{PREFIX}b"],
        digests((f"{PREFIX}a", 50)),
        identifier_prefix=PREFIX,
        update_mode="changed",
    )

    # `b` has no fingerprint yet: no update, the snapshot is rebuilt from today.
    assert plan.updates.identifiers == (f"{PREFIX}a",)


def test_update_existing_replaces_everything_already_in_dialog():
    plan = reconcile(
        digests(f"{PREFIX}a", f"{PREFIX}b"),
        [f"{PREFIX}a"],
        digests(f"{PREFIX}a"),
        identifier_prefix=PREFIX,
        update_mode="all",
    )

    assert plan.updates.identifiers == (f"{PREFIX}a",)
    assert plan.creations.identifiers == (f"{PREFIX}b",)


def test_a_batch_over_its_cap_is_held_whole():
    produced = digests(*[f"{PREFIX}{i}" for i in range(5)])

    plan = reconcile(produced, [], {}, identifier_prefix=PREFIX, max_creations=3)

    assert plan.creations.held is True
    assert plan.creations.size == 5
    # Held means nothing at all is applied, not "apply the first three".
    assert plan.creations.applicable == ()
    assert plan.held == {"create": 5}


def test_a_batch_at_its_cap_is_applied():
    plan = reconcile(
        digests(*[f"{PREFIX}{i}" for i in range(3)]),
        [],
        {},
        identifier_prefix=PREFIX,
        max_creations=3,
    )

    assert plan.creations.held is False
    assert len(plan.creations.applicable) == 3


def test_a_held_deletion_batch_is_detected_again_the_next_day():
    produced = digests(f"{PREFIX}kept")
    remote = [f"{PREFIX}kept"] + [f"{PREFIX}gone{i}" for i in range(4)]

    today = reconcile(
        produced, remote, {}, identifier_prefix=PREFIX, delete_missing=True, max_deletions=3
    )
    # Nothing was applied, so DiaLog still holds the same identifiers tomorrow.
    tomorrow = reconcile(
        produced, remote, {}, identifier_prefix=PREFIX, delete_missing=True, max_deletions=3
    )

    assert today.deletions.held and tomorrow.deletions.held
    assert today.deletions.identifiers == tomorrow.deletions.identifiers


def test_force_deletions_releases_the_deletion_batch_only():
    produced = digests(*[f"{PREFIX}{i}" for i in range(5)])
    remote = [f"{PREFIX}gone{i}" for i in range(4)]

    plan = reconcile(
        produced,
        remote,
        {},
        identifier_prefix=PREFIX,
        delete_missing=True,
        max_creations=3,
        max_deletions=3,
        force_deletions=True,
    )

    assert plan.deletions.held is False
    assert len(plan.deletions.applicable) == 4
    assert plan.creations.held is True


def test_an_unchanged_regulation_produces_no_operation():
    produced = digests(f"{PREFIX}a")

    plan = reconcile(
        produced,
        [f"{PREFIX}a"],
        dict(produced),
        identifier_prefix=PREFIX,
        update_mode="changed",
        delete_missing=True,
    )

    assert plan.planned == {"create": 0, "update": 0, "delete": 0}
