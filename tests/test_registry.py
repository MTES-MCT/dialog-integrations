"""`integrations.toml`: what the nightly workflow runs, read before the project is installed."""

import json

import pytest

import registry


def _write(tmp_path, text):
    path = tmp_path / "integrations.toml"
    path.write_text(text, encoding="utf-8")
    return path


def test_the_matrix_lists_every_organization_with_its_target(tmp_path):
    path = _write(
        tmp_path,
        """
[[integration]]
organization = "co_brest"
target = "prod"

[[integration]]
organization = "co_lyon"
target = "staging"
paused = ["chaussees_trottoirs"]
""",
    )
    entries = registry.load(path)

    assert registry.matrix(entries) == {
        "include": [
            {"organization": "co_brest", "target": "prod"},
            {"organization": "co_lyon", "target": "staging"},
        ]
    }
    assert registry.entry("co_lyon", path).paused == ("chaussees_trottoirs",)  # type: ignore[union-attr]
    assert registry.select(entries, "co_lyon") == [entries[1]]


@pytest.mark.parametrize(
    "text",
    [
        '[[integration]]\norganization = "co_brest"\ntarget = "production"\n',
        '[[integration]]\ntarget = "prod"\n',
        '[[integration]]\norganization = "a"\ntarget = "prod"\n'
        '[[integration]]\norganization = "a"\ntarget = "staging"\n',
        '[[integration]]\norganization = "a"\ntarget = "prod"\npaused = "x"\n',
    ],
    ids=["unknown target", "no organization", "listed twice", "paused not a list"],
)
def test_a_registry_the_workflow_cannot_run_is_refused(tmp_path, text):
    with pytest.raises(registry.RegistryError):
        registry.load(_write(tmp_path, text))


def test_asking_for_an_organization_the_registry_lacks_fails(capsys):
    assert registry.main(["matrix", "co_nowhere"]) == 1
    assert registry.main(["matrix"]) == 0
    assert json.loads(capsys.readouterr().out)["include"]
