"""Tests for the field-by-field diff and the French synchronization report."""

from integrations.diff_report import SourceFunnel, diff_digests, format_diff, render_report
from integrations.reconciliation import reconcile
from integrations.state import compute_regulation_digest
from tests.sync_fixtures import build_regulation

PREFIX = "PARIS-EUDO-"


def digest(identifier: str, **kwargs) -> dict:
    return compute_regulation_digest(build_regulation(identifier, **kwargs))


def report_for(plan, produced, snapshot=None, dry_run=True, funnels=None, **kwargs) -> str:
    return render_report(
        organization="co_paris",
        environment="dev",
        dry_run=dry_run,
        plan=plan,
        funnels=funnels if funnels is not None else [],
        produced=produced,
        snapshot=snapshot,
        **kwargs,
    )


def test_a_changed_speed_shows_as_one_field():
    differences = diff_digests(digest("A-1", max_speed=50), digest("A-1", max_speed=30))

    assert differences == [("measures[0].maxSpeed", 50, 30)]


def test_an_added_measure_shows_as_a_missing_value():
    old = digest("A-1")
    new = digest("A-1")
    new["measures"].append(new["measures"][0])

    paths = [path for path, _, _ in diff_digests(old, new)]

    assert paths == ["measures[1]"]


def test_the_diff_is_rendered_with_the_identifier():
    lines = format_diff("A-1", digest("A-1", title="Travaux"), digest("A-1", title="Report"))

    assert lines[0].startswith("  A-1")
    assert "title" in lines[1]
    assert "'Travaux'" in lines[1] and "'Report'" in lines[1]


def test_the_report_shows_the_funnel_and_the_three_batches():
    produced = {f"{PREFIX}a": digest(f"{PREFIX}a")}
    plan = reconcile(produced, [], {}, identifier_prefix=PREFIX, delete_missing=True)
    funnel = SourceFunnel(
        name="eudonet",
        raw_rows=262,
        clean_rows=216,
        regulations=50,
        measures=52,
        metrics={"arrêtés du périmètre": 5297},
    )

    report = report_for(plan, produced, funnels=[funnel])

    assert "262 lignes brutes" in report
    assert "216 lignes nettoyées" in report
    assert "50 arrêtés, 52 mesures" in report
    assert "arrêtés du périmètre : 5297" in report
    assert "à créer : 1" in report
    assert "à mettre à jour : 0" in report
    assert "à supprimer : 0" in report
    assert "aucune écriture" in report


def test_a_missing_snapshot_is_spelled_out():
    produced = {f"{PREFIX}a": digest(f"{PREFIX}a")}
    plan = reconcile(
        produced, [f"{PREFIX}a"], None, identifier_prefix=PREFIX, update_mode="changed"
    )

    report = report_for(plan, produced, snapshot=None)

    assert "Instantané : absent" in report
    assert "reconstruit" in report


def test_an_organization_that_never_updates_says_the_snapshot_is_unused():
    produced = {"B-1": digest("B-1")}

    report = report_for(reconcile(produced, [], None), produced)

    assert "Instantané : non utilisé" in report


def test_short_lists_are_printed_whole_and_long_ones_are_truncated():
    few = {f"{PREFIX}{i}": digest(f"{PREFIX}{i}") for i in range(3)}
    many = {f"{PREFIX}{i}": digest(f"{PREFIX}{i}") for i in range(25)}

    short_report = report_for(reconcile(few, [], {}, identifier_prefix=PREFIX), few)
    long_report = report_for(reconcile(many, [], {}, identifier_prefix=PREFIX), many)

    assert short_report.count(f"  - {PREFIX}") == 3
    assert "20 premiers sur 25" in long_report
    assert long_report.count(f"  - {PREFIX}") == 20


def test_a_held_batch_is_flagged_with_the_command_that_releases_it():
    produced = {f"{PREFIX}a": digest(f"{PREFIX}a")}
    plan = reconcile(
        produced,
        [f"{PREFIX}a"] + [f"{PREFIX}gone{i}" for i in range(4)],
        {},
        identifier_prefix=PREFIX,
        delete_missing=True,
        max_deletions=3,
    )

    report = report_for(plan, produced)

    assert "RETENU" in report
    assert "à revoir manuellement" in report
    assert "--force-deletions" in report


def test_an_organization_without_a_prefix_reads_as_creations_only():
    produced = {"B-1": digest("B-1")}
    plan = reconcile(produced, [], None)

    report = report_for(plan, produced)

    assert "Préfixe d'identifiant : aucun" in report
    assert "sous le préfixe" not in report
    assert "à supprimer : 0" in report
    assert "--force-deletions" not in report


def test_each_update_is_detailed_field_by_field():
    produced = {f"{PREFIX}a": digest(f"{PREFIX}a", max_speed=30)}
    snapshot = {f"{PREFIX}a": digest(f"{PREFIX}a", max_speed=50)}
    plan = reconcile(
        produced, [f"{PREFIX}a"], snapshot, identifier_prefix=PREFIX, update_mode="changed"
    )

    report = report_for(plan, produced, snapshot=snapshot)

    assert "Différences détectées" in report
    assert "measures[0].maxSpeed : 50 → 30" in report
