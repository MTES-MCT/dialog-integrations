"""Paris / Eudonet: from a structured location description to a DiaLog named street.

Eudonet carries no geometry. A location is a road name (`2710`), an arrondissement (`2708`)
and, depending on `2705` "Porte sur", either a house number, two bounds, or nothing. That
is exactly DiaLog's `namedStreet` (`roadType = lane`): DiaLog geocodes it itself.

Every `location_*` column produced here is passed verbatim to `SaveNamedStreetDTO`, so
nothing else may be emitted under that prefix (no label, no geometry).

What is dropped, and counted (plan §5.3, R-02): zones, axes (the ring road by kilometre
point), points without a number, sections without both bounds, withdrawn locations, and
locations limited to one direction (R-32 freeze).

Crossing-road labels from the 2017 data migration are upper case with no accent nor
apostrophe ("RUE D AUTEUIL"). DiaLog's geocoder does not recognise them; the official name
does (probed on 2026-09-16, 6 cases out of 6). `street_names.json` is the list of official
names (`l_longmin`) of the Paris open data street referential "Dénominations des emprises
des voies actuelles" (ODbL), taken on 2026-09-08. The migration is frozen since 2017, so a
frozen list covers it; only a street renamed since would be missed, and it is then sent
as is. Rebuild: `sorted({f["properties"]["l_longmin"] for f in paris_voie.geojson})`.
"""

import json
import re
import unicodedata
from dataclasses import dataclass
from functools import cache
from pathlib import Path

import polars as pl
from loguru import logger

from api.dia_log_client.models import DirectionEnum, RoadTypeEnum

# `2705` "Porte sur" labels.
SCOPE_POINT = "Un point"
SCOPE_SECTION = "Une section"
SCOPE_WHOLE_STREET = "La totalité de la voie"
SCOPE_ZONE = "Une zone"
SCOPE_AXIS = "Un axe"

# `2768` "Statut Localisation": a modifying order withdraws a location from an older one.
WITHDRAWN_STATUSES = ("Retiré", "Supprimé")

# `namedStreet.fromPointType` / `toPointType`, as DiaLog's form names them.
POINT_TYPE_HOUSE_NUMBER = "houseNumber"
POINT_TYPE_INTERSECTION = "intersection"

CITY_LABEL = "Paris"

# `2708` catalogue: "1er arrondissement" … "20ème arrondissement" → INSEE 75101 … 75120.
CITY_CODE_BY_DISTRICT = {
    **{"1er arrondissement": "75101"},
    **{f"{n}ème arrondissement": f"751{n:02d}" for n in range(2, 21)},
}

# `2711` "Sens" values that limit a location to one direction. R-32 freeze: no
# one-direction measure is published until DiaLog carries the side of the road, so these
# locations are dropped rather than widened to both directions. Every other value, the two
# "circulation générale" ones included (relative to the traffic, not to the segment),
# says nothing about the segment: BOTH.
ONE_DIRECTION_LABELS = ("du début vers la fin du segment", "de la fin vers le début du segment")

STREET_NAMES_FILE = Path(__file__).with_name("street_names.json")

# "4", "12 bis", "n°39 à 43", "12B", "9t ", "39-43", "1B PASSAGE TURQUETIL". A one-letter
# suffix only counts when nothing else follows it ("4 avenue" is a plain 4).
_NUMBER = re.compile(r"(\d+)\s*(bis|ter|quater|[a-zA-Z])?(?![\w])", re.IGNORECASE)
_LEADING_NUMBER = re.compile(r"^\s*(\d+)\s*(bis|ter|quater|[a-zA-Z])?\s+\S", re.IGNORECASE)
_HAS_SUFFIX = re.compile(
    r"\d\s*(bis|ter|quater|[a-zA-Z])\b", re.IGNORECASE
)  # Polars: no look-ahead

LOCATION_COLUMNS = {
    "location_road_type": pl.Utf8,
    "location_city_code": pl.Utf8,
    "location_city_label": pl.Utf8,
    "location_road_name": pl.Utf8,
    "location_from_point_type": pl.Utf8,
    "location_from_house_number": pl.Utf8,
    "location_from_road_name": pl.Utf8,
    "location_to_point_type": pl.Utf8,
    "location_to_house_number": pl.Utf8,
    "location_to_road_name": pl.Utf8,
    "location_direction": pl.Utf8,
    # Not a DTO field: the reason a row is dropped, null when it is kept.
    "location_rejection": pl.Utf8,
}

_INPUT_COLUMNS = [
    "l_file_id",
    "l_scope",
    "l_road_name",
    "l_district",
    "l_direction",
    "l_status",
    "l_from_house_number",
    "l_from_road_name",
    "l_from_address_label",
    "l_to_house_number",
    "l_to_road_name",
    "l_to_address_label",
    "l_point_house_number",
    "l_point_address_label",
]


@dataclass(frozen=True)
class Bound:
    """One end of a section: a house number on the road itself, or a crossing road."""

    point_type: str
    house_number: str | None = None
    road_name: str | None = None


def parse_house_number(text: str | None) -> str | None:
    """ "12", "12 bis", "n°39" → the first number, with its suffix when there is one."""
    if not text:
        return None
    match = _NUMBER.search(text.replace("°", " "))
    if not match:
        return None
    return _format_number(match.group(1), match.group(2))


def parse_house_number_range(text: str | None) -> tuple[str, str] | None:
    """ "n°39 à 43" → ("39", "43"); "12" → ("12", "12"); no number → None."""
    if not text:
        return None
    matches = _NUMBER.findall(text.replace("°", " "))
    if not matches:
        return None
    first = _format_number(*matches[0])
    last = _format_number(*matches[-1])
    return first, last


def _format_number(digits: str, suffix: str | None) -> str:
    """ "12 bis", "12B", "9t" → "12", "12", "9".

    Probed on the staging on 2026-09-08: DiaLog geocodes neither "12 bis", nor "12B", nor
    "12bis" (400 "La géolocalisation de la voie entre ces points a échoué"). The bare
    number is the closest address it knows; the suffix is dropped and counted.
    """
    return digits


def parse_bound(
    house_number: str | None, road_name: str | None, address_label: str | None
) -> Bound | None:
    """A section bound from its three possible carriers, most explicit first.

    Recent entries fill `N° adresse` (house number on the road) or `Libellé voie`
    (crossing road). The 2017 data migration filled only `Libellé adresse`, which is either
    "117 QUAI DE LA GARE" (a house number on the road itself: 1 484 of 1 502 numbered
    labels name the road of the location) or "RUE DANTE" (a crossing road).
    """
    number = parse_house_number(house_number)
    if number:
        return Bound(POINT_TYPE_HOUSE_NUMBER, house_number=number)
    # The 2017 migration also copied the *end* address label into `Libellé voie` (1 174
    # rows start with a number, e.g. "22 RUE DES REGLISES"; the start field never does).
    # Sent as a crossing road, DiaLog answers 400 and the whole regulation is refused
    # (staging run of 2026-09-16, 175 of the 506 refused regulations). Read it like the
    # address label: a leading number is a house number on the road itself.
    for label in (road_name, address_label):
        if not label:
            continue
        match = _LEADING_NUMBER.match(label)
        if match:
            return Bound(POINT_TYPE_HOUSE_NUMBER, house_number=_format_number(*match.groups()))
        return Bound(POINT_TYPE_INTERSECTION, road_name=official_street_name(label.strip()))
    return None


def city_code(district: str | None) -> str | None:
    """ "17ème arrondissement" → "75117". Multi-valued ("1er ; 2ème") → the first one.

    None when the arrondissement is missing or unknown: probed on 2026-09-08, DiaLog
    refuses Paris as a whole (`75056`, 400 "Cette adresse n'est pas reconnue") and a
    missing `cityCode` (422). Which arrondissement rules a boundary street is an open
    question for Paris (plan §9, question 7).
    """
    if not district:
        return None
    first = district.split(";")[0].strip()
    return CITY_CODE_BY_DISTRICT.get(first)


def street_key(name: str) -> str:
    """ "Rue d'Auteuil", "RUE D AUTEUIL" → "RUE D AUTEUIL": no accent, no punctuation."""
    ascii_name = unicodedata.normalize("NFKD", name).encode("ascii", "ignore").decode()
    return re.sub(r"[^A-Za-z0-9]+", " ", ascii_name).upper().strip()


@cache
def official_street_names() -> dict[str, str]:
    names = json.loads(STREET_NAMES_FILE.read_text(encoding="utf-8"))
    return {street_key(name): name for name in names}


def official_street_name(label: str) -> str:
    """The official spelling of an upper-case migration label, or the label unchanged."""
    if label != label.upper():
        return label
    return official_street_names().get(street_key(label), label)


def resolve_location(row: dict) -> dict:
    """The `location_*` fields of one raw row, or a rejection reason."""
    fields: dict = {name: None for name in LOCATION_COLUMNS}

    if row["l_file_id"] is None:
        return {**fields, "location_rejection": "measure without location"}
    if row["l_status"] in WITHDRAWN_STATUSES:
        return {**fields, "location_rejection": "withdrawn location"}
    road_name = official_street_name((row["l_road_name"] or "").strip())
    if not road_name:
        return {**fields, "location_rejection": "no road name"}
    if row["l_direction"] in ONE_DIRECTION_LABELS:
        return {**fields, "location_rejection": "one direction only (R-32 freeze)"}
    code = city_code(row["l_district"])
    if code is None:
        return {**fields, "location_rejection": "no arrondissement"}

    scope = row["l_scope"]
    if scope == SCOPE_POINT:
        numbers = parse_house_number_range(row["l_point_house_number"])
        if numbers is None:
            # The 2017 migration put the number in the address label only.
            leading = _LEADING_NUMBER.match(row["l_point_address_label"] or "")
            if leading:
                number = _format_number(*leading.groups())
                numbers = (number, number)
        if numbers is None:
            return {**fields, "location_rejection": "point without house number"}
        start = Bound(POINT_TYPE_HOUSE_NUMBER, house_number=numbers[0])
        end = Bound(POINT_TYPE_HOUSE_NUMBER, house_number=numbers[1])
    elif scope == SCOPE_SECTION:
        start = parse_bound(
            row["l_from_house_number"], row["l_from_road_name"], row["l_from_address_label"]
        )
        end = parse_bound(
            row["l_to_house_number"], row["l_to_road_name"], row["l_to_address_label"]
        )
        if start is None and end is None:
            return {**fields, "location_rejection": "section without bounds"}
        if start is None or end is None:
            return {**fields, "location_rejection": "section with a single bound"}
    elif scope == SCOPE_WHOLE_STREET:
        start = end = None
    elif scope in (SCOPE_ZONE, SCOPE_AXIS):
        return {**fields, "location_rejection": f"scope {scope!r} has no geometry"}
    else:
        return {**fields, "location_rejection": f"unknown scope {scope!r}"}

    fields.update(
        location_road_type=RoadTypeEnum.LANE.value,
        location_city_code=code,
        location_city_label=CITY_LABEL,
        location_road_name=road_name,
        location_direction=DirectionEnum.BOTH.value,
    )
    if start is not None and end is not None:
        fields.update(
            location_from_point_type=start.point_type,
            location_from_house_number=start.house_number,
            location_from_road_name=start.road_name,
            location_to_point_type=end.point_type,
            location_to_house_number=end.house_number,
            location_to_road_name=end.road_name,
        )
    return fields


def compute_location_fields(df: pl.DataFrame) -> pl.DataFrame:
    """Named-street fields (`roadType = lane`) for every location that DiaLog can geocode.

    Produces `location_road_type`, `location_city_code`, `location_city_label`,
    `location_road_name`, `location_from_point_type`, `location_from_house_number`,
    `location_from_road_name`, `location_to_point_type`, `location_to_house_number`,
    `location_to_road_name`, `location_direction`.

    Drops, and counts by reason: rows without location, withdrawn locations (`2768`),
    missing road name, missing arrondissement, points without a house number, sections
    without both bounds, zones, axes, unknown scopes, one-direction locations (R-32 freeze).
    House-number suffixes are dropped; upper-case migration labels are respelled.
    """
    number_columns = ["l_from_house_number", "l_to_house_number", "l_point_house_number"]
    suffixed = df.select(
        pl.any_horizontal(
            [
                pl.col(name).fill_null("").str.contains(_HAS_SUFFIX.pattern)
                for name in number_columns
            ]
        ).sum()
    ).item()
    if suffixed:
        logger.warning(
            f"{suffixed} locations carry a house-number suffix (bis, ter…) that DiaLog cannot "
            "geocode: sending the bare number"
        )

    resolved = pl.Series(
        "resolved",
        [resolve_location(row) for row in df.select(_INPUT_COLUMNS).iter_rows(named=True)],
        dtype=pl.Struct(LOCATION_COLUMNS),
    )
    df = df.with_columns(resolved).unnest("resolved")

    rejected = df.filter(pl.col("location_rejection").is_not_null())
    if rejected.height:
        reasons = dict(
            rejected.get_column("location_rejection").value_counts(sort=True).iter_rows()
        )
        logger.warning(f"Dropping {rejected.height} locations DiaLog cannot geocode: {reasons}")
    df = df.filter(pl.col("location_rejection").is_null()).drop("location_rejection")

    if df.height:
        shapes = dict(df.get_column("l_scope").value_counts(sort=True).iter_rows())
        logger.info(f"Kept {df.height} named-street locations: {shapes}")
    return df
