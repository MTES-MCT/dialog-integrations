import re

_LOWERCASE_INSIDE_NAME = {
    "de",
    "du",
    "des",
    "la",
    "le",
    "les",
    "et",
    "au",
    "aux",
    "a",
    "en",
    "sur",
    "sous",
}
_ELIDED = re.compile(r"^([dl])'(.+)$", re.IGNORECASE)
_FILED = re.compile(r"^\s*(?P<name>[^()]+?)\s*\((?P<kind>[^()]*)\)\s*$")


def from_referential(filed: str) -> str:
    """`SAINT AURELIEN (rue)` as `Rue Saint Aurelien`, `ANCIENNE ECOLE (rue de l')` as
    `Rue de l'Ancienne Ecole`; a name not filed that way is left as it is."""
    match = _FILED.match(filed.replace("’", "'"))
    if match is None:
        return filed
    kind = match["kind"].strip()
    name = " ".join(_word(word, first=i == 0) for i, word in enumerate(match["name"].split()))
    if not kind:
        return name
    joined = f"{kind}{name}" if kind.endswith("'") else f"{kind} {name}"
    return joined[0].upper() + joined[1:]


def _word(word: str, first: bool) -> str:
    lower = word.lower()
    if not first and lower in _LOWERCASE_INSIDE_NAME:
        return lower
    elided = _ELIDED.match(lower)
    if elided is not None:
        article = elided[1] if not first else elided[1].upper()
        return f"{article}'{_capitalized(elided[2])}"
    return _capitalized(lower)


def _capitalized(word: str) -> str:
    return "-".join(part[:1].upper() + part[1:] for part in word.split("-"))
