"""Reading daily time slots out of a free-text description, such as Lyon's
`descripchantierinternet` (`de 08h00 à 18h00`, `7h-17h`, `entre 6h et 17h`…).

Narrow on purpose: a slot is a pair of clock readings joined by an explicit separator,
never a lone hour; anything unparsed is counted and reported, not guessed.
"""

import re
from datetime import date, datetime, time
from zoneinfo import ZoneInfo

PARIS = ZoneInfo("Europe/Paris")

# `7h`, `7h30`, `08h00`, `21:00`.
_CLOCK = r"(\d{1,2})\s*[hH:]\s*(\d{2})?"

# `/` also separates successive slots (`11h-15h / 19h-00h`): leftmost-first matching lets
# each pair consume its own closing hour first.
_SLOT = re.compile(_CLOCK + r"\s*(?:jusqu'à|à|au|a|et|-|–|—|/)\s*" + _CLOCK)


def parse_time_slots(text: str | None) -> list[tuple[str, str]]:
    """Every daily slot the text states, as `("HH:MM", "HH:MM")` pairs.

    A slot whose end precedes its start crosses midnight: that is how night work is
    written. A window repeated in two sentences is kept once.
    """
    if not text:
        return []

    slots: list[tuple[str, str]] = []
    for match in _SLOT.finditer(text):
        start_hour, start_minute, end_hour, end_minute = match.groups()
        start = _clock(start_hour, start_minute)
        end = _clock(end_hour, end_minute)
        if start is None or end is None or start == end:
            continue
        if (start, end) not in slots:
            slots.append((start, end))
    return slots


def _clock(hour: str, minute: str | None) -> str | None:
    """`("7", None)` becomes `"07:00"`; anything off the clock face becomes None."""
    hours, minutes = int(hour), int(minute or 0)
    if hours > 23 or minutes > 59:
        return None
    return f"{hours:02d}:{minutes:02d}"


def to_iso_slots(day: date, slots: list[tuple[str, str]]) -> list[dict[str, str]]:
    """Anchor clock readings on a calendar day, in French local time.

    DiaLog reads the written offset: anchoring on the period's own start day makes a
    summer slot say `+02:00` and a winter one `+01:00`.
    """
    return [{"start_time": _iso(day, start), "end_time": _iso(day, end)} for start, end in slots]


def _iso(day: date, clock: str) -> str:
    hours, minutes = (int(part) for part in clock.split(":"))
    return datetime.combine(day, time(hours, minutes), tzinfo=PARIS).isoformat()
