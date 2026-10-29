"""A business rule, written once over canonical column names."""

from collections.abc import Callable
from dataclasses import dataclass
from enum import Enum

import polars as pl


class Nature(Enum):
    """Why the rule applies; the value is shown in the report."""

    # Right, but not ours to publish.
    OUT_OF_SCOPE = "hors périmètre"
    # DiaLog cannot express it yet.
    DIALOG_LIMIT = "limite de DiaLog"
    # Broken at the producer.
    DATA_DEFECT = "défaut de la donnée"
    # Plausible, but worth a look.
    SUSPICIOUS = "à vérifier"


class Outcome(Enum):
    """What becomes of the rows the rule applies to; the value is shown in the report."""

    DROP = "écarté"
    FLAG = "signalé"


@dataclass(frozen=True, eq=False)
class Rule:
    code: str
    label: str
    nature: Nature
    outcome: Outcome
    # True on the rows the rule applies to; null on a row it cannot read. A function when
    # the expression reads the clock, so that it is built at each run, not at import.
    applies: pl.Expr | Callable[[], pl.Expr]

    def expression(self) -> pl.Expr:
        return self.applies() if callable(self.applies) else self.applies

    @property
    def reads(self) -> set[str]:
        return set(self.expression().meta.root_names())
