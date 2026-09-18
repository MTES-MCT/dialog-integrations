"""The integration report: how the results of every organization read in Tchap."""

import pytest

from notifications.notifier import Notifier

# Deliberately out of order: GitHub matrix job outputs have no guaranteed order.
RESULTS = {
    "result_dp_sarthe": '{"success":false}',
    "result_co_brest": '{"success":true}',
}


@pytest.fixture
def notifier():
    return Notifier()


def test_every_organization_is_listed_in_a_stable_order(notifier):
    body, formatted_body = notifier.format_message(RESULTS)

    assert "✅ co_brest : Importé avec succès" in body
    assert "❌ dp_sarthe : Erreur lors de l'import" in body
    assert body.index("co_brest") < body.index("dp_sarthe")
    assert formatted_body.index("co_brest") < formatted_body.index("dp_sarthe")


def test_keys_other_than_results_are_ignored(notifier):
    body, _ = notifier.format_message({**RESULTS, "run_url": "https://github.com/…"})

    assert "run_url" not in body


def test_unreadable_result_counts_as_a_failure(notifier):
    body, _ = notifier.format_message({"result_co_rennes": "not json"})

    assert "❌ co_rennes : Erreur lors de l'import" in body


def test_empty_report_is_flagged_as_an_anomaly(notifier):
    body, formatted_body = notifier.format_message({})

    assert "Aucun résultat d'intégration reçu." in body
    assert "Aucun résultat d'intégration reçu." in formatted_body


def test_html_is_escaped(notifier):
    _, formatted_body = notifier.format_message({"result_<script>": '{"success":true}'})

    assert "<script>" not in formatted_body
    assert "&lt;script&gt;" in formatted_body


# --- Synchronization counters ------------------------------------------------------

SYNCHRONIZED = {
    "result_co_paris": (
        '{"success":true,"created":12,"updated":3,"deleted":1,'
        '"held":{"delete":60},"source":{"arrêtés du périmètre":5297,"mesures":11230}}'
    )
}


def test_a_result_without_counters_reads_exactly_as_before(notifier):
    body, formatted_body = notifier.format_message(RESULTS)

    assert "✅ co_brest : Importé avec succès" in body
    assert " - " not in body
    assert "<ul>" in formatted_body and "<li><ul>" not in formatted_body


def test_the_counters_are_shown_per_organization(notifier):
    body, formatted_body = notifier.format_message(SYNCHRONIZED)

    assert "✅ co_paris : Importé avec succès - 12 créés, 3 mis à jour, 1 supprimé" in body
    assert "12 créés, 3 mis à jour, 1 supprimé" in formatted_body


def test_a_run_that_changed_nothing_says_so(notifier):
    body, _ = notifier.format_message(
        {"result_co_brest": '{"success":true,"created":0,"updated":0,"deleted":0}'}
    )

    assert "aucun changement" in body


def test_a_held_batch_is_reported_for_review(notifier):
    body, formatted_body = notifier.format_message(SYNCHRONIZED)

    assert "lot retenu (plafond dépassé) : 60 suppressions - à revoir manuellement" in body
    assert "60 suppressions" in formatted_body


def test_source_volumes_are_reported_when_given(notifier):
    body, _ = notifier.format_message(SYNCHRONIZED)

    assert "Volumétries source : arrêtés du périmètre : 5297, mesures : 11230" in body


def test_failed_writes_are_counted_next_to_the_successes(notifier):
    body, _ = notifier.format_message(
        {"result_co_rennes": '{"success":true,"created":5,"updated":0,"deleted":0,"errors":2}'}
    )

    assert "5 créés, 0 mis à jour, 0 supprimé, 2 en échec" in body


def test_the_corpus_size_is_shown_under_the_organization(notifier):
    body, formatted_body = notifier.format_message(
        {
            "result_co_lyon": (
                '{"success":true,"created":4,"updated":0,"deleted":0,'
                '"integrated":{"rows":374,"regulations":197,"measures":197}}'
            )
        }
    )

    # Raw row counts mean something different in every organization: not shown.
    assert "197 arrêtés, 197 mesures" in body
    assert "374" not in body
    assert "197 arrêtés, 197 mesures" in formatted_body


def test_the_corpus_size_without_a_row_count(notifier):
    body, _ = notifier.format_message(
        {
            "result_co_brest": (
                '{"success":true,"created":0,"updated":0,"deleted":0,'
                '"integrated":{"regulations":1577,"measures":3085}}'
            )
        }
    )

    assert "1577 arrêtés, 3085 mesures" in body


def test_each_dataset_has_its_own_line_with_the_share_retained(notifier):
    body, _ = notifier.format_message(
        {
            "result_dp_sarthe": (
                '{"success":true,"created":0,"updated":0,"deleted":0,'
                '"integrated":{"regulations":877,"measures":877,"rows":878},'
                '"datasets":['
                '{"label":"permanent","sources":["limitations_vitesse","restrictions_gabarits"],'
                '"regulations":867,"measures":867,"restrictions":868,"retained":867},'
                '{"label":"temporaire","sources":["chantiers_routiers"],'
                '"regulations":10,"measures":10,"restrictions":12,"retained":10}]}'
            )
        }
    )

    assert "permanent : 867 arrêtés, 867 mesures, 99,9 % du jeu retenu" in body
    assert "temporaire : 10 arrêtés, 10 mesures, 83,3 % du jeu retenu" in body
    # The organization's total gives way to its datasets.
    assert "877 arrêtés" not in body


def test_a_dataset_without_a_count_has_no_rate(notifier):
    body, _ = notifier.format_message(
        {
            "result_co_paris": (
                '{"success":true,"datasets":[{"label":"eudonet","sources":["eudonet"],'
                '"regulations":32,"measures":51,"restrictions":null,"retained":51}]}'
            )
        }
    )

    assert "eudonet : 32 arrêtés, 51 mesures\n" in body + "\n"
    assert "%" not in body


STAGING = "dialog-staging-pr2096.osc-fr1.scalingo.io"


def test_production_comes_first_and_stagings_below(notifier):
    body, formatted_body = notifier.format_message(
        {
            "result_co_lyon": f'{{"success":true,"target":"{STAGING}"}}',
            "result_co_brest": '{"success":true,"target":"dialog.beta.gouv.fr"}',
            "result_dp_sarthe": '{"success":true}',
            "result_co_rennes": f'{{"success":false,"target":"{STAGING}"}}',
        }
    )

    assert body.index("Production") < body.index("co_brest") < body.index("dp_sarthe")
    assert body.index("dp_sarthe") < body.index(f"Staging — {STAGING}")
    assert body.index("Staging") < body.index("co_lyon") < body.index("co_rennes")
    # A single staging host is named once, in the heading.
    assert "co_lyon [" not in body
    assert "<strong>Production</strong>" in formatted_body
    assert formatted_body.index("Production") < formatted_body.index("Staging")


def test_several_staging_hosts_are_named_per_organization(notifier):
    body, _ = notifier.format_message(
        {
            "result_co_lyon": '{"success":true,"target":"staging-a.example.org"}',
            "result_co_rennes": '{"success":true,"target":"staging-b.example.org"}',
        }
    )

    assert "Production" not in body
    assert "Staging — staging-a.example.org, staging-b.example.org" in body
    assert "co_lyon [staging-a.example.org]" in body
    assert "co_rennes [staging-b.example.org]" in body


def test_closures_are_counted_and_refusals_are_not(notifier):
    body, _ = notifier.format_message(
        {
            "result_co_lyon": (
                '{"success":true,"created":0,"updated":0,"deleted":0,"closed":2,"refused":3}'
            )
        }
    )

    assert "0 créé, 0 mis à jour, 0 supprimé, 2 clos" in body
    assert "refus" not in body


def test_a_closing_organization_that_changed_nothing_says_so(notifier):
    body, _ = notifier.format_message(
        {"result_co_lyon": '{"success":true,"created":0,"updated":0,"deleted":0,"closed":0}'}
    )

    assert "aucun changement" in body


# --- Target ------------------------------------------------------------------------


def test_a_run_against_production_is_not_flagged(notifier):
    body, _ = notifier.format_message(
        {"result_co_brest": '{"success":true,"target":"dialog.beta.gouv.fr"}'}
    )

    assert "✅ co_brest : Importé avec succès" in body
    assert "[" not in body


def test_a_run_against_another_host_is_filed_under_its_staging(notifier):
    body, formatted_body = notifier.format_message(
        {"result_co_paris": '{"success":true,"target":"dialog-staging-pr2096.osc-fr1.scalingo.io"}'}
    )

    assert (
        "Staging — dialog-staging-pr2096.osc-fr1.scalingo.io\n✅ co_paris : Importé avec succès"
        in body
    )
    assert "Production" not in body
    assert "<strong>Staging — dialog-staging-pr2096.osc-fr1.scalingo.io</strong>" in formatted_body
