"""The markdown summary of one run, as the GitHub run page shows it."""

from types import SimpleNamespace

from integrations.sync.reconciliation import IntegrationOutcome
from notifications.summary import MAX_LINES_PER_LEVEL, render_summary


def _record(level: str, name: str, message: str) -> dict:
    return {"level": SimpleNamespace(name=level), "name": name, "message": message}


def _outcome(**changes) -> IntegrationOutcome:
    outcome = IntegrationOutcome(organization="co_test", report="Entonnoir\n  fake : 3 → 2")
    outcome.datasets = [
        {
            "label": "temporaire",
            "sources": ["fake"],
            "regulations": 2,
            "measures": 2,
            "restrictions": 3,
            "retained": 2,
        }
    ]
    for key, value in changes.items():
        setattr(outcome, key, value)
    return outcome


def test_a_run_shows_its_counts_datasets_report_and_alerts():
    markdown = render_summary(
        "co_test",
        "dialog.beta.gouv.fr",
        _outcome(created=2),
        [
            _record("WARNING", "integrations.co_test.fake", "Dropping 1 rows without geometry"),
            _record("ERROR", "integrations.api", "Failed to create: X-1 - got status 400"),
        ],
    )

    assert markdown.startswith("## co_test — dialog.beta.gouv.fr\n")
    assert "✅ 2 créés, 0 mis à jour, 0 supprimé" in markdown
    assert "| temporaire | 2 | 2 | 3 | 2 | 66,7 % |" in markdown
    assert "```text\nEntonnoir\n  fake : 3 → 2\n```" in markdown
    assert (
        "### Erreurs (1)\n\n- `integrations.api` : Failed to create: X-1 - got status 400"
        in markdown
    )
    assert "### Alertes (1)\n\n- `integrations.co_test.fake` : Dropping 1 rows" in markdown


def test_a_dry_run_says_what_it_would_have_done():
    markdown = render_summary(
        "co_test", "h", _outcome(dry_run=True, planned={"create": 2, "delete": 1}), []
    )

    assert "🧪 Simulation, rien n'a été écrit. Prévu : 2 créations, 1 suppressions" in markdown
    assert "✅" not in markdown


def test_a_crash_is_reported_with_the_alerts_gathered_before_it():
    markdown = render_summary(
        "co_test",
        "h",
        None,
        [_record("WARNING", "settings", "No environment file found")],
        failure="UnexpectedStatus(500)",
    )

    assert "❌ **Échec de l'intégration** : `UnexpectedStatus(500)`" in markdown
    assert "### Alertes (1)" in markdown
    assert "Rapport de synchronisation" not in markdown


def test_long_alert_lists_are_cut():
    records = [_record("WARNING", "m", f"alert {i}") for i in range(MAX_LINES_PER_LEVEL + 5)]

    markdown = render_summary("co_test", "h", _outcome(), records)

    assert f"### Alertes ({MAX_LINES_PER_LEVEL + 5})" in markdown
    assert "- … et 5 autres" in markdown
