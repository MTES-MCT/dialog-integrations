"""Reading a work site's free-text description, all or nothing (R-78).

DiaLog can express its hours and a resident exemption, not a direction, a weekend
reopening or 15-minute closures: a half-read description would publish a stronger
restriction than stated. The residual threshold sits between the longest legitimate
residue measured, `entreet` (7 non-space characters), and the shortest illegitimate one,
`SensNord/Sud` (12).
"""

import re
from dataclasses import dataclass

from toolkit.transforms.time_slots import _CLOCK, _SLOT, parse_time_slots

# `Sauf riverain` / `Sauf riverains`, the one exemption the layer spells out.
RESIDENT_EXEMPTION = re.compile(r"sauf\s+riverains?", re.IGNORECASE)

# Non-space characters the description may keep once hours and the resident exemption
# are removed: connectors such as `de … à`, `entre … et`, `-`, `/`, a trailing comma.
RESIDUAL_MAX_CHARS = 8


@dataclass(frozen=True)
class DescriptionReading:
    slots: list[tuple[str, str]]
    n_clocks: int  # clock readings found, paired or not
    n_pairs: int  # `start – end` pairs matched, before deduplication
    exempts_residents: bool
    residual: str  # what is left once hours and the exemption are removed, spaces dropped

    @property
    def hours_fully_paired(self) -> bool:
        """Every clock reading belongs to a pair: no lone `réouverture à 18h`."""
        return self.n_clocks == 2 * self.n_pairs

    @property
    def fully_read(self) -> bool:
        return len(self.residual) <= RESIDUAL_MAX_CHARS


def read_description(text: str | None) -> DescriptionReading:
    text = text or ""
    residual = RESIDENT_EXEMPTION.sub("", _CLOCK_RE.sub("", text))
    return DescriptionReading(
        slots=parse_time_slots(text),
        n_clocks=len(_CLOCK_RE.findall(text)),
        n_pairs=len(_SLOT.findall(text)),
        exempts_residents=RESIDENT_EXEMPTION.search(text) is not None,
        residual=re.sub(r"\s+", "", residual),
    )


_CLOCK_RE = re.compile(_CLOCK)
