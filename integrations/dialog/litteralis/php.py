"""The PHP semantics the back end's Litteralis import relies on, reproduced exactly.

This port must produce byte-identical results to the PHP code it replaces, bugs
included, so that both can be compared. Python's defaults differ from PHP's in ways
that matter on French free text and on GeoJSON:

- `trim()` strips six ASCII characters only; `str.strip()` also strips the non-breaking
  space that French text is full of;
- `!$value` is true for `""` and `"0"`, not only for empty strings;
- `==` and `in_array()` compare two numeric strings as numbers (`"1e1" == "10"`);
- `json_encode()` escapes slashes, writes `3.0` as `3` and `1e-5` as `1.0e-5`;
- `DateTimeImmutable::createFromFormat()` accepts one-digit months, rolls over
  out-of-range values (`2023-02-30` is `2023-03-02`) and rejects milliseconds.

Every behaviour below was checked against PHP 8.4.
"""

import re
from datetime import date, datetime, timedelta, timezone, tzinfo
from decimal import Decimal
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

PHP_TRIM_CHARACTERS = " \t\n\r\0\x0b"


class PhpFatalError(Exception):
    """What PHP would have thrown out of the import, aborting it for the organization.

    `php_class` names the PHP throwable: `ErrorException` for a warning turned into an
    exception (the back end runs its import with Symfony's debug error handler), and
    `TypeError` or `Error` for the engine's own errors.
    """

    def __init__(self, php_class: str, message: str):
        super().__init__(f"{php_class}: {message}")
        self.php_class = php_class
        self.message = message


def trim(value: str) -> str:
    """PHP `trim()`: six ASCII characters, never the non-breaking space."""
    return value.strip(PHP_TRIM_CHARACTERS)


def is_falsy(value: Any) -> bool:
    """PHP `!$value`, which is also `empty($value)` on a defined variable."""
    if value is None or value is False:
        return True
    if isinstance(value, bool):
        return False
    if isinstance(value, (int, float)):
        return value == 0
    if isinstance(value, str):
        return value in ("", "0")
    if isinstance(value, (list, dict)):
        return len(value) == 0
    return False


def to_string(value: Any) -> str:
    """PHP string conversion, as `sprintf('%s')` and `(string)` apply it."""
    if value is None or value is False:
        return ""
    if value is True:
        return "1"
    if isinstance(value, str):
        return value
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        return _float_repr(value, exponent_marker="E")
    raise PhpFatalError("ErrorException", "Warning: Array to string conversion")


def require_string(value: Any, where: str) -> str:
    """A value passed to a parameter typed `string` under `strict_types=1`."""
    if not isinstance(value, str):
        raise PhpFatalError(
            "TypeError", f"{where}: must be of type string, {_php_type(value)} given"
        )
    return value


def key(array: dict, name: str) -> Any:
    """`$array['name']` without `??`: a missing key is a warning, hence fatal here."""
    if name not in array:
        raise PhpFatalError("ErrorException", f'Warning: Undefined array key "{name}"')
    return array[name]


def _php_type(value: Any) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "bool"
    if isinstance(value, int):
        return "int"
    if isinstance(value, float):
        return "float"
    if isinstance(value, (list, dict)):
        return "array"
    return type(value).__name__


# --- Loose comparison (`==`, `in_array`, `array_search`) ------------------------------

NUMERIC_STRING = re.compile(
    r"^[ \t\n\r\v\f]*[+-]?(\d+(\.\d*)?|\.\d+)([eE][+-]?\d+)?[ \t\n\r\v\f]*$", re.ASCII
)


def loose_equals(left: str, right: str) -> bool:
    """PHP 8 `==` between two strings: numerically when both are numeric strings."""
    if NUMERIC_STRING.match(left) and NUMERIC_STRING.match(right):
        return to_number(left) == to_number(right)
    return left == right


def to_number(text: str) -> int | float:
    stripped = text.strip(" \t\n\r\v\f")
    if re.fullmatch(r"[+-]?\d+", stripped):
        return int(stripped)
    return float(stripped)


def in_array(needle: str, haystack: list[str]) -> bool:
    return any(loose_equals(needle, item) for item in haystack)


def array_search(needle: str, haystack: list[str]) -> int | None:
    """The first index holding `needle`, None where PHP returns `false`."""
    for index, item in enumerate(haystack):
        if loose_equals(needle, item):
            return index
    return None


# --- json_encode with default flags ----------------------------------------------------


def json_encode(value: Any) -> str:
    """PHP `json_encode($value)` with no flags, after `json_decode($text, true)`.

    A JSON object decoded by PHP becomes an array, and an array whose keys are exactly
    `0, 1, …` (or none at all) is written back as a list: `{}` comes back as `[]`.
    """
    return "".join(_encode(value))


def _encode(value: Any):
    if value is None:
        yield "null"
    elif value is True:
        yield "true"
    elif value is False:
        yield "false"
    elif isinstance(value, int):
        yield str(value)
    elif isinstance(value, float):
        yield _float_repr(value, exponent_marker="e")
    elif isinstance(value, str):
        yield _encode_string(value)
    elif isinstance(value, list):
        yield "["
        for index, item in enumerate(value):
            if index:
                yield ","
            yield from _encode(item)
        yield "]"
    elif isinstance(value, dict):
        if all(str(k) == str(i) for i, k in enumerate(value.keys())):
            yield from _encode(list(value.values()))
            return
        yield "{"
        for index, (name, item) in enumerate(value.items()):
            if index:
                yield ","
            yield _encode_string(str(name))
            yield ":"
            yield from _encode(item)
        yield "}"
    else:
        raise TypeError(f"Cannot encode {type(value).__name__}")


_SHORT_ESCAPES = {
    '"': '\\"',
    "\\": "\\\\",
    "/": "\\/",
    "\b": "\\b",
    "\f": "\\f",
    "\n": "\\n",
    "\r": "\\r",
    "\t": "\\t",
}


def _encode_string(text: str) -> str:
    """A JSON string as PHP writes it: `/` escaped, every non-ASCII character as
    `\\uXXXX` (surrogate pairs above U+FFFF), control characters below 0x20 escaped,
    DEL (0x7f) left as is."""
    parts = ['"']
    for char in text:
        code = ord(char)
        if char in _SHORT_ESCAPES:
            parts.append(_SHORT_ESCAPES[char])
        elif code < 0x20:
            parts.append(f"\\u{code:04x}")
        elif code < 0x80:
            parts.append(char)
        elif code <= 0xFFFF:
            parts.append(f"\\u{code:04x}")
        else:
            code -= 0x10000
            parts.append(f"\\u{0xD800 + (code >> 10):04x}\\u{0xDC00 + (code & 0x3FF):04x}")
    parts.append('"')
    return "".join(parts)


def _float_repr(number: float, exponent_marker: str) -> str:
    """The shortest round-trip digits, laid out as PHP does with serialize_precision -1.

    Fixed notation while the decimal exponent is in [-4, 17), without a trailing `.0`;
    exponent notation otherwise, with at least one decimal (`1.0e+17`). `json_encode`
    writes the exponent in lower case, string conversion in upper case.
    """
    if number != number or number in (float("inf"), float("-inf")):
        raise ValueError("PHP cannot encode NaN or infinity")
    if number == 0:
        return "-0" if str(number).startswith("-") else "0"

    sign, digits, exponent = Decimal(repr(number)).normalize().as_tuple()
    assert isinstance(exponent, int)
    mantissa = "".join(str(d) for d in digits)
    decimal_exponent = len(mantissa) + exponent - 1
    prefix = "-" if sign else ""

    if -4 <= decimal_exponent < 17:
        text = format(Decimal(repr(abs(number))), "f")
        if "." in text:
            text = text.rstrip("0").rstrip(".")
        return prefix + text

    fraction = mantissa[1:] or "0"
    exponent_sign = "+" if decimal_exponent >= 0 else "-"
    return (
        f"{prefix}{mantissa[0]}.{fraction}{exponent_marker}{exponent_sign}{abs(decimal_exponent)}"
    )


# --- DateTimeImmutable::createFromFormat ----------------------------------------------

UTC = timezone.utc

# `Y-m-d\TH:i:sO`, PHP's DateTimeInterface::ISO8601. Years take 1 to 4 digits, months,
# days and hours 1 or 2, minutes and seconds exactly 2; the zone may be preceded by
# blanks. Nothing may follow it: a fractional second is "trailing data".
_ISO8601 = re.compile(
    r"^(\d{1,4})-(\d{1,2})-(\d{1,2})T(\d{1,2}):(\d{2}):(\d{2})[ \t]*(.+)$", re.ASCII
)
_OFFSET = re.compile(r"^([+-])(\d{1,2})(?::?(\d{2}))?$", re.ASCII)
_ABBREVIATIONS = {"z": 0, "utc": 0, "gmt": 0, "cet": 60, "cest": 120}


def create_from_iso8601(text: str) -> datetime | None:
    """`DateTimeImmutable::createFromFormat(DATE_ISO8601, $text)`, or None for `false`.

    Out-of-range fields roll over as PHP does: month 13 is January of the next year,
    day 0 the last day of the previous month, 24:60:60 one hour, one minute later.
    """
    match = _ISO8601.match(text)
    if not match:
        return None
    year, month, day, hour, minute, second = (int(g) for g in match.groups()[:6])
    zone = _parse_zone(match.group(7))
    if zone is None:
        return None

    naive = _rolled_over(year, month, day) + timedelta(hours=hour, minutes=minute, seconds=second)
    return naive.replace(tzinfo=zone)


def _parse_zone(text: str) -> tzinfo | None:
    offset = _OFFSET.match(text)
    if offset:
        sign, hours, minutes = offset.groups()
        delta = timedelta(hours=int(hours), minutes=int(minutes or 0))
        return timezone(-delta if sign == "-" else delta)
    if text.lower() in _ABBREVIATIONS:
        return timezone(timedelta(minutes=_ABBREVIATIONS[text.lower()]))
    if "/" in text:
        try:
            return ZoneInfo(text)
        except (ZoneInfoNotFoundError, ValueError):
            return None
    return None


def _rolled_over(year: int, month: int, day: int) -> datetime:
    """Midnight of a date whose month and day may be out of range, rolled over."""
    year += (month - 1) // 12
    month = (month - 1) % 12 + 1
    return datetime(year, month, 1) + timedelta(days=day - 1)


def create_from_clock(hour: str, minute: str, zone: tzinfo, today: date) -> datetime | None:
    """`DateTimeImmutable::createFromFormat('H:i', "$hour:$minute", $zone)`.

    Without `!` in the format, PHP takes the missing date from the current day in
    `zone`: the offset, hence the UTC time once converted, depends on the day the
    import runs. The hour takes 1 or 2 digits and may exceed 23; the minute takes
    exactly 2 digits, otherwise PHP returns false.
    """
    if not re.fullmatch(r"\d{1,2}", hour, re.ASCII) or not re.fullmatch(r"\d{2}", minute, re.ASCII):
        return None
    naive = datetime(today.year, today.month, today.day) + timedelta(
        hours=int(hour), minutes=int(minute)
    )
    return naive.replace(tzinfo=zone)


def format_iso8601(moment: datetime) -> str:
    """`$moment->format(DATE_ISO8601)`: `2026-10-12T14:00:00+0000`."""
    offset = moment.utcoffset() or timedelta(0)
    minutes = int(offset.total_seconds() // 60)
    sign = "-" if minutes < 0 else "+"
    hours, minutes = divmod(abs(minutes), 60)
    return (
        f"{moment.year:04d}"
        + moment.strftime("-%m-%dT%H:%M:%S")
        + f"{sign}{hours:02d}{minutes:02d}"
    )
