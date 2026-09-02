"""Reading opening hours out of the layer's free-text description.

The Métropole publishes the *existence* of a daily window in `typeperturbation`
("de jour", "de nuit") and its *hours* nowhere in particular: they land in
`descripchantierinternet`, a free-text field that also carries directions of travel
and general remarks. Eight spellings were observed for the same information —
`de 08h00 à 18h00`, `7h-17h`, `7h30 - 16h`, `entre 6h et 17h`, `de 21:00 à 05:00`.

Parsing free text is fragile by nature, so the rule is narrow on purpose: a slot is
recognised only as a *pair* of clock readings joined by an explicit separator. A lone
hour is never a slot, and anything unparsed is counted and reported rather than
guessed at.
"""

import re
from datetime import date, datetime, time
from zoneinfo import ZoneInfo

PARIS = ZoneInfo("Europe/Paris")

# One clock reading: `7h`, `7h30`, `08h00`, `21:00`. The minutes are optional.
_CLOCK = r"(\d{1,2})\s*[hH:]\s*(\d{2})?"

# Two readings joined by a separator. `/` is included because the layer uses it both
# as a range separator and between successive slots (`11h-15h / 19h-00h / 00h-1h`);
# leftmost-first matching resolves that correctly, since the first pair consumes its
# own closing hour before the next pair is considered.
_SLOT = re.compile(_CLOCK + r"\s*(?:jusqu'à|à|au|a|et|-|–|—|/)\s*" + _CLOCK)


def parse_time_slots(text: str | None) -> list[tuple[str, str]]:
    """Every daily slot the text states, as `("HH:MM", "HH:MM")` pairs.

    A slot whose end precedes its start crosses midnight and is kept as such — that
    is how the source writes night work. Duplicates are dropped: a description that
    repeats the same window in two sentences still describes one window.
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

    DiaLog keeps only the clock of a slot and reads it in Europe/Paris, but it reads
    the offset that is written: anchoring on the period's own start day is what makes
    a summer slot say `+02:00` and a winter one `+01:00` instead of shifting by an
    hour.
    """
    return [{"start_time": _iso(day, start), "end_time": _iso(day, end)} for start, end in slots]


def _iso(day: date, clock: str) -> str:
    hours, minutes = (int(part) for part in clock.split(":"))
    return datetime.combine(day, time(hours, minutes), tzinfo=PARIS).isoformat()
