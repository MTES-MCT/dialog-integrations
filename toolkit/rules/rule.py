"""A business rule, written once over canonical column names."""

from collections.abc import Callable
from dataclasses import dataclass
from enum import Enum

import polars as pl


class Nature(Enum):
    """Why a row fails the rule; the value is shown in the report."""

    # Right, but not ours to publish.
    OUT_OF_SCOPE = "hors périmètre"
    # DiaLog cannot express it yet.
    DIALOG_LIMIT = "limite de DiaLog"
    # Broken at the producer.
    DATA_DEFECT = "défaut de la donnée"
    # Plausible, but worth a look.
    SUSPICIOUS = "à vérifier"


class Outcome(Enum):
    """What becomes of the rows that fail the rule; the value is shown in the report."""

    DROP = "écarté"
    FLAG = "signalé"


@dataclass(frozen=True, eq=False)
class Rule:
    code: str
    label: str
    nature: Nature
    outcome: Outcome
    # True on the rows the rule validates; false or null fails them, so a legitimately
    # missing value must be said valid here. A function when the expression reads the
    # clock, so that it is built at each run, not at import.
    valid: pl.Expr | Callable[[], pl.Expr]

    def expression(self) -> pl.Expr:
        return self.valid() if callable(self.valid) else self.valid

    @property
    def reads(self) -> set[str]:
        return set(self.expression().meta.root_names())
