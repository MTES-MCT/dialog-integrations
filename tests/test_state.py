"""Tests for the digest and the snapshot store."""

import gzip
import json

from integrations.state import (
    SnapshotStore,
    compute_regulation_digest,
    fingerprint,
    state_dir,
)
from tests.sync_fixtures import build_regulation, geometry


def test_the_same_regulation_always_hashes_the_same():
    first = compute_regulation_digest(build_regulation("A-1"))
    second = compute_regulation_digest(build_regulation("A-1"))

    assert fingerprint(first) == fingerprint(second)


def test_a_changed_speed_changes_the_fingerprint():
    before = compute_regulation_digest(build_regulation("A-1", max_speed=50))
    after = compute_regulation_digest(build_regulation("A-1", max_speed=30))

    assert fingerprint(before) != fingerprint(after)


def test_a_changed_title_changes_the_fingerprint():
    before = compute_regulation_digest(build_regulation("A-1", title="Travaux"))
    after = compute_regulation_digest(build_regulation("A-1", title="Travaux (report)"))

    assert fingerprint(before) != fingerprint(after)


def test_the_identifier_is_not_part_of_the_digest():
    # The identifier is the key of the snapshot, not part of what changed.
    assert compute_regulation_digest(build_regulation("A-1")) == compute_regulation_digest(
        build_regulation("A-2")
    )


def test_a_moved_geometry_changes_the_fingerprint():
    before = compute_regulation_digest(build_regulation("A-1"))
    after = compute_regulation_digest(
        build_regulation("A-1", geometry_json=geometry([[-4.486, 48.39], [-4.480, 48.395]]))
    )

    assert fingerprint(before) != fingerprint(after)


def test_the_same_geometry_written_differently_is_not_a_change():
    # Key order and spacing come from the serializer, not from the data.
    reordered = json.dumps(
        {"coordinates": [[-4.486, 48.39], [-4.484, 48.392]], "type": "LineString"},
        indent=2,
    )
    before = compute_regulation_digest(build_regulation("A-1"))
    after = compute_regulation_digest(build_regulation("A-1", geometry_json=reordered))

    assert fingerprint(before) == fingerprint(after)


def test_the_geometry_is_reduced_to_type_points_bbox_and_hash():
    digest = compute_regulation_digest(build_regulation("A-1"))
    reduced = digest["measures"][0]["locations"][0]["rawGeoJSON"]["geometry"]

    assert reduced["type"] == "LineString"
    assert reduced["points"] == 2
    assert reduced["bbox"] == [-4.486, 48.39, -4.484, 48.392]
    assert "coordinates" not in reduced


def test_an_unparsable_geometry_still_hashes():
    digest = compute_regulation_digest(build_regulation("A-1", geometry_json="not json"))
    reduced = digest["measures"][0]["locations"][0]["rawGeoJSON"]["geometry"]

    assert set(reduced) == {"hash"}


def test_a_missing_snapshot_reads_as_none(tmp_path):
    store = SnapshotStore("co_test", "source", base_dir=tmp_path)

    assert not store.exists()
    assert store.load() is None


def test_a_snapshot_survives_a_round_trip(tmp_path):
    store = SnapshotStore("co_test", "source", base_dir=tmp_path)
    digests = {"A-1": compute_regulation_digest(build_regulation("A-1"))}

    store.save(digests)

    assert store.path == tmp_path / "co_test" / "source.json.gz"
    assert store.load() == digests


def test_a_corrupted_snapshot_reads_as_missing(tmp_path):
    store = SnapshotStore("co_test", "source", base_dir=tmp_path)
    store.path.parent.mkdir(parents=True)
    with gzip.open(store.path, "wt", encoding="utf-8") as handle:
        handle.write("{not json")

    # Guessing its content could cost a mass rewrite; a missing snapshot only costs
    # a day of updates.
    assert store.load() is None


def test_the_state_directory_can_be_moved_with_an_environment_variable(monkeypatch, tmp_path):
    monkeypatch.setenv("DIALOG_STATE_DIR", str(tmp_path / "elsewhere"))

    assert state_dir() == tmp_path / "elsewhere"
    assert SnapshotStore("co_test", "source").path.parent == tmp_path / "elsewhere" / "co_test"
