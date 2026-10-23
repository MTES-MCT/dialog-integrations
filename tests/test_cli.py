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
