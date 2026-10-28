import importlib

from loguru import logger

import cli


def test_a_crash_never_prints_the_values_of_its_variables(capfd):
    importlib.reload(cli)
    credentials = {"SubscriberPassword": "not-a-real-secret"}
    try:
        raise ConnectionError(f"lost the connection, {len(credentials)} field(s) sent")
    except ConnectionError:
        logger.exception("Integration failed")
    printed = capfd.readouterr().err
    assert "ConnectionError" in printed
    assert "not-a-real-secret" not in printed


def test_the_results_left_by_the_jobs_are_read_and_a_missing_one_is_a_failure(tmp_path):
    (tmp_path / "result_co_brest.json").write_text('{"success":true}', encoding="utf-8")
    (tmp_path / "result_co_lyon.json").write_text("", encoding="utf-8")

    results = cli.read_results(tmp_path, ["co_brest", "co_lyon", "co_paris"])

    assert results == {
        "result_co_brest": '{"success":true}',
        "result_co_lyon": '{"success": false}',
        "result_co_paris": '{"success": false}',
    }


def test_a_failed_source_or_too_many_refusals_fail_the_command(monkeypatch):
    from typer.testing import CliRunner

    from sync.outcome import IntegrationOutcome

    def run(outcome):
        integration = type(
            "_Integration",
            (),
            {
                "organization_settings": type("S", (), {"base_url": "https://h"})(),
                "integrate_regulations": lambda self, **kwargs: outcome,
            },
        )()
        monkeypatch.setattr(
            cli.BaseIntegration, "from_organization", classmethod(lambda c, o, env: integration)
        )
        return CliRunner().invoke(cli.app, ["integrate", "co_brest", "--json"])

    healthy = run(IntegrationOutcome("co_brest", rejections=2, produced=100))
    failed = run(IntegrationOutcome("co_brest", frozen_sources={"permanent_lineaire": "failed"}))
    refused = run(IntegrationOutcome("co_brest", rejections=30, produced=100))

    assert healthy.exit_code == 0
    assert failed.exit_code == 1 and '"success": false' in failed.stdout
    assert refused.exit_code == 1
