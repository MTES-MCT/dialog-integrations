"""Paris / Eudonet: from a structured location description to a DiaLog named street.

Eudonet carries no geometry. A location is a road name (`2710`), an arrondissement (`2708`)
and, depending on `2705` "Porte sur", either a house number, two bounds, or nothing. That
is exactly DiaLog's `namedStreet` (`roadType = lane`): DiaLog geocodes it itself.

Every `location_*` column produced here is passed verbatim to `SaveNamedStreetDTO`, so
nothing else may leave this module under that prefix (no label, no geometry).

Crossing-road labels from the 2017 data migration are upper case with no accent nor
apostrophe ("RUE D AUTEUIL"). DiaLog's geocoder does not recognise them; the official name
does (probed on 2026-09-16, 6 cases out of 6). `street_names.json` is the list of official
names (`l_longmin`) of the Paris open data street referential "Dénominations des emprises
des voies actuelles" (ODbL), taken on 2026-09-08. The migration is frozen since 2017, so a
frozen list covers it; only a street renamed since would be missed, and it is then sent
as is. Rebuilt as the sorted set of `l_longmin` of that referential.

The arrondissement (`2708`) is sometimes missing, or lists several ("6ème ; 15ème") for a
street that crosses a boundary. `street_districts.json` says where each official street
lies, from the same street referential joined to the "Adresses" one (`c_ar`), both taken on
2026-09-08: one INSEE code for a street within one arrondissement, otherwise the runs of
consecutive house numbers of one parity that fall in each. A missing arrondissement is a
gap, not an error, and the referential fills it without ambiguity (a translation, R-06).
What the table cannot settle stays dropped when the source cites no arrondissement, and
goes to the first one cited when it cites several. Rebuilt as, for each `l_longmin`, the `c_ar`
of its addresses (`n_sq_vo`), as runs of `n_voie` by parity when there are several.
"""

import json
import re
import unicodedata
from dataclasses import dataclass
from difflib import SequenceMatcher
from functools import cache
from pathlib import Path

import polars as pl
from loguru import logger

from api.dia_log_client.models import DirectionEnum, RoadTypeEnum

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

CITY_CODE_BY_DISTRICT = {
    **{"1er arrondissement": "75101"},
    **{f"{n}ème arrondissement": f"751{n:02d}" for n in range(2, 21)},
}

# `2711` "Sens": any value but "both ways", the "circulation générale" ones included, drops
# the location rather than widen it to both directions (R-32 freeze; Q-60, 2026-10-26).
BOTH_DIRECTIONS_LABEL = "dans les deux sens"
# `2712` "côté" values that limit a location to one side of the street, which the DiaLog
# write API cannot carry (2026-10-23): dropped rather than widened to both sides.
ONE_SIDE_LABELS = ("côté pair", "côté impair")

# A numbered label naming another street than the location's ("28 Place Saint-Ferdinand" on
# rue Brunel) is the corner building on the crossing street: where along that street the
# measure starts cannot be told, so the location is dropped. On the 2026-09-08 extraction,
# spellings of the same street score 0.95 or more ("VILLA PATRICE BOUDARD" / "Boudart"),
# other streets 0.79 or less: 11 locations dropped, whatever the threshold in between.
SAME_STREET_MIN_RATIO = 0.9

STREET_NAMES_FILE = Path(__file__).with_name("street_names.json")
STREET_DISTRICTS_FILE = Path(__file__).with_name("street_districts.json")

DISTRICT_FROM_SOURCE = "source"
DISTRICT_FROM_STREET = "street in one arrondissement"
DISTRICT_FROM_NUMBER = "house number"
DISTRICT_FIRST_CITED = "first cited"

_NUMBER = re.compile(r"(\d+)\s*(bis|ter|quater|[a-zA-Z])?(?![\w])", re.IGNORECASE)
_LEADING_NUMBER = re.compile(r"^\s*(\d+)\s*(bis|ter|quater|[a-zA-Z])?\s+\S", re.IGNORECASE)
# Read by Polars, whose regex engine has no look-ahead: the flag goes inline.
_HAS_SUFFIX = r"(?i)\d\s*(bis|ter|quater|[a-z])\b"

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
    "location_rejection": pl.Utf8,
    "location_district_source": pl.Utf8,
}

_INPUT_COLUMNS = [
    "l_file_id",
    "l_scope",
    "l_road_name",
    "l_district",
    "l_side",
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
    """ "12", "12 bis", "n°39" → "12", "12", "39": the first number, without its suffix.

    Probed on the staging on 2026-09-08: DiaLog geocodes neither "12 bis", nor "12B", nor
    "12bis" (400 "La géolocalisation de la voie entre ces points a échoué"). The bare
    number is the closest address it knows; the suffix is dropped and counted.
    """
    if not text:
        return None
    match = _NUMBER.search(text.replace("°", " "))
    if not match:
        return None
    return match.group(1)


def parse_house_number_range(text: str | None) -> tuple[str, str] | None:
    """ "n°39 à 43" → ("39", "43"); "12" → ("12", "12"); no number → None."""
    if not text:
        return None
    matches = _NUMBER.findall(text.replace("°", " "))
    if not matches:
        return None
    return matches[0][0], matches[-1][0]


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
    label = road_name or address_label
    if not label:
        return None
    match = _LEADING_NUMBER.match(label)
    if match:
        return Bound(POINT_TYPE_HOUSE_NUMBER, house_number=match.group(1))
    return Bound(POINT_TYPE_INTERSECTION, road_name=official_street_name(label.strip()))


def city_codes(district: str | None) -> list[str]:
    """Every arrondissement cited, in order:
    "6ème arrondissement ; 15ème arrondissement" → ["75106", "75115"]."""
    if not district:
        return []
    codes = (CITY_CODE_BY_DISTRICT.get(part.strip()) for part in district.split(";"))
    return [code for code in codes if code]


@cache
def street_districts() -> dict[str, str | dict[str, list[list[int]]]]:
    table = json.loads(STREET_DISTRICTS_FILE.read_text(encoding="utf-8"))
    return {street_key(name): value for name, value in table.items()}


def district_of_number(street: str, number: str) -> str | None:
    """The arrondissement a house number of a street falls in, when the referential says.

    Only within a run of addresses of the same parity: between two runs, or on a street
    the referential does not know, nothing is settled.
    """
    districts = street_districts().get(street_key(street))
    if districts is None or not number.isdigit():
        return None
    if isinstance(districts, str):
        return districts
    value = int(number)
    found = {
        code
        for code, runs in districts.items()
        for first, last in runs
        if first <= value <= last and first % 2 == value % 2
    }
    return found.pop() if len(found) == 1 else None


def resolve_city_code(
    district: str | None, street: str, numbers: list[str]
) -> tuple[str | None, str | None]:
    """(INSEE code, how it was obtained) of a location, or (None, None) to drop it.

    Never Paris as a whole: probed on 2026-09-08, DiaLog refuses `75056` (400 "Cette
    adresse n'est pas reconnue") and a missing `cityCode` (422).
    """
    cited = city_codes(district)
    if len(cited) == 1:
        return cited[0], DISTRICT_FROM_SOURCE

    districts = street_districts().get(street_key(street))
    if not cited and isinstance(districts, str):
        return districts, DISTRICT_FROM_STREET
    by_number = {district_of_number(street, number) for number in numbers}
    if numbers and len(by_number) == 1 and None not in by_number:
        code = by_number.pop()
        if not cited or code in cited:
            return code, DISTRICT_FROM_NUMBER
    if cited:
        return cited[0], DISTRICT_FIRST_CITED
    return None, None


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


def names_another_street(label: str | None, road_name: str) -> bool:
    """True when `label` starts with a house number followed by another street's name."""
    match = _LEADING_NUMBER.match(label or "")
    if not match or label is None:
        return False
    street = street_key(label[match.end() - 1 :])
    similarity = SequenceMatcher(None, street, street_key(road_name)).ratio()
    return similarity < SAME_STREET_MIN_RATIO


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
    if row["l_direction"] not in (None, BOTH_DIRECTIONS_LABEL):
        return {**fields, "location_rejection": "one direction only (R-32 freeze)"}
    if row["l_side"] in ONE_SIDE_LABELS:
        return {**fields, "location_rejection": "one side of the street only"}

    bounds = _bounds(row, road_name)
    if isinstance(bounds, str):
        return {**fields, "location_rejection": bounds}
    start, end = bounds
    bound_numbers = [
        bound.house_number
        for bound in (start, end)
        if bound is not None and bound.house_number is not None
    ]
    code, district_source = resolve_city_code(row["l_district"], road_name, bound_numbers)
    if code is None:
        return {**fields, "location_rejection": "no arrondissement"}

    fields.update(
        location_road_type=RoadTypeEnum.LANE.value,
        location_city_code=code,
        location_city_label=CITY_LABEL,
        location_road_name=road_name,
        location_direction=DirectionEnum.BOTH.value,
        location_district_source=district_source,
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


def _bounds(row: dict, road_name: str) -> tuple[Bound | None, Bound | None] | str:
    """The two bounds of the location by its scope (none for the whole street), or why it
    has none we can send."""
    scope = row["l_scope"]
    if scope == SCOPE_WHOLE_STREET:
        return None, None
    if scope in (SCOPE_ZONE, SCOPE_AXIS):
        return f"scope {scope!r} has no geometry"
    if scope == SCOPE_POINT:
        point_numbers = parse_house_number_range(row["l_point_house_number"])
        if point_numbers is None:
            # The 2017 migration put the number in the address label only.
            leading = _LEADING_NUMBER.match(row["l_point_address_label"] or "")
            if leading:
                point_numbers = (leading.group(1), leading.group(1))
        if point_numbers is None:
            return "point without house number"
        if names_another_street(row["l_point_address_label"], road_name):
            return "bound numbered on another street"
        return (
            Bound(POINT_TYPE_HOUSE_NUMBER, house_number=point_numbers[0]),
            Bound(POINT_TYPE_HOUSE_NUMBER, house_number=point_numbers[1]),
        )
    if scope != SCOPE_SECTION:
        return f"unknown scope {scope!r}"
    start = parse_bound(
        row["l_from_house_number"], row["l_from_road_name"], row["l_from_address_label"]
    )
    end = parse_bound(row["l_to_house_number"], row["l_to_road_name"], row["l_to_address_label"])
    if start is None and end is None:
        return "section without bounds"
    if start is None or end is None:
        return "section with a single bound"
    bound_labels = (
        "l_from_address_label",
        "l_from_road_name",
        "l_to_address_label",
        "l_to_road_name",
    )
    if any(names_another_street(row[column], road_name) for column in bound_labels):
        return "bound numbered on another street"
    return start, end


def compute_location_fields(df: pl.DataFrame) -> pl.DataFrame:
    """Named-street fields (`roadType = lane`) for every location that DiaLog can geocode;
    the others are dropped and counted by reason."""
    number_columns = ["l_from_house_number", "l_to_house_number", "l_point_house_number"]
    suffixed = df.select(
        pl.any_horizontal(
            [pl.col(name).fill_null("").str.contains(_HAS_SUFFIX) for name in number_columns]
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
        logger.warning(f"Dropping {rejected.height} locations DiaLog cannot take: {reasons}")
    df = df.filter(pl.col("location_rejection").is_null()).drop("location_rejection")

    deduced = df.filter(pl.col("location_district_source") != DISTRICT_FROM_SOURCE)
    if deduced.height:
        sources = dict(
            deduced.get_column("location_district_source").value_counts(sort=True).iter_rows()
        )
        logger.info(
            f"{deduced.height} locations without a single arrondissement in the source, "
            f"settled by the street referential or kept on the first cited: {sources}"
        )
    df = df.drop("location_district_source")

    if df.height:
        shapes = dict(df.get_column("l_scope").value_counts(sort=True).iter_rows())
        logger.info(f"Kept {df.height} named-street locations: {shapes}")
    return df
