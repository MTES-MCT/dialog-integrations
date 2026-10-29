"""A business rule, written once over canonical column names."""

from dataclasses import dataclass
from enum import Enum

import polars as pl


class Nature(Enum):
    """What becomes of a row the rule applies to; the value is shown in the report."""

    DECISION = "écarté"
    # The regulation is left as DiaLog has it: a broken row must not close what was right.
    DEFECT = "défaut"
    SIGNAL = "signalé"


@dataclass(frozen=True, eq=False)
class Rule:
    code: str
    label: str
    nature: Nature
    # True on the rows the rule applies to; null on a row it cannot read.
    applies: pl.Expr
    reference: str | None = None

    @property
    def reads(self) -> set[str]:
        return set(self.applies.meta.root_names())
