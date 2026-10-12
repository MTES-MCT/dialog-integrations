"""Port of the back end's `LitteralisTransformer`: the emprises of one regulation become
one import command — general info, then one measure per supported measure name of
each emprise, all sharing the emprise's geometry as a `rawGeoJSON` location.

Reproduced as is, including what looks wrong:

- Polygon and MultiPolygon geometries are dropped, although the ADR describes them as
  the normal shape of a Litteralis emprise;
- any error on any emprise drops the whole regulation, but an emprise whose geometry is
  dropped is not parsed at all, so its errors are never seen;
- a numbered parameter ("Interdiction de dépasser 2 | …") is only recognised when the
  measure name is plain ASCII: PHP's `\\w` without the `u` modifier does not match "é",
  so the parameter is silently ignored;
- "piétons" and "cycles" become restricted types the API refuses as restrictions.
"""

import re

from integrations.dialog.litteralis import php
from integrations.dialog.litteralis import reporter as rec
from integrations.dialog.litteralis.commands import (
    CAN_USE_RAW_GEOJSON,
    GeneralInfo,
    ImportCommand,
    Measure,
    RawGeoJSONLocation,
    VehicleSet,
)
from integrations.dialog.litteralis.period_parser import (
    LitteralisPeriodParser,
    find_parameter_value,
)
from integrations.dialog.litteralis.reporter import Reporter

NO_ENTRY = "noEntry"
SPEED_LIMITATION = "speedLimitation"
PARKING_PROHIBITED = "parkingProhibited"
NO_OVERTAKING = "noOvertaking"
ALTERNATE_ROAD = "alternateRoad"

MEASURE_MAP = {
    "SOGELINK - Circulation interdite": NO_ENTRY,
    "Circulation interdite": NO_ENTRY,
    "Limitation tonnage (Arrêté Permanent)": NO_ENTRY,
    "SOGELINK - Limitation de vitesse": SPEED_LIMITATION,
    "Limitation de vitesse": SPEED_LIMITATION,
    "Limitation de vitesse (Arrêté Permanent)": SPEED_LIMITATION,
    "Interdiction de stationnement": PARKING_PROHIBITED,
    "SOGELINK - Interdiction de stationnement": PARKING_PROHIBITED,
    "Interdiction de dépasser": NO_OVERTAKING,
    "Interdiction de dépasser (Arrêté Permanent)": NO_OVERTAKING,
    # Montpellier, once the "YYYY - " and "AP - " prefixes are removed
    "Circulation interdite RD": NO_ENTRY,
    "Limitation de vitesse RD": SPEED_LIMITATION,
    "Stationnement - Interdiction": PARKING_PROHIBITED,
    "Circulation alternée": ALTERNATE_ROAD,
    "SOGELINK - Circulation alternée": ALTERNATE_ROAD,
}

PERMANENT_REGULATION = "permanentRegulation"
TEMPORARY_REGULATION = "temporaryRegulation"
ROAD_MAINTENANCE = "roadMaintenance"
EVENT = "event"
OTHER = "other"

PEDESTRIANS = "pedestrians"
BICYCLE = "bicycle"
HEAVY_GOODS_VEHICLE = "heavyGoodsVehicle"
OTHER_VEHICLE = "other"

VEHICLE_TYPES = {
    "piétons": PEDESTRIANS,
    "cycles": BICYCLE,
    "poids lourds": HEAVY_GOODS_VEHICLE,
    "véhicules de plus de 3.5 tonnes": HEAVY_GOODS_VEHICLE,
}

# PHP: '/^(?P<name>[\s|\w|-]+) (?P<number>\d+)$/i' — ASCII-only without `u`.
NUMBERED_NAME = re.compile(r"^(?P<name>[\s|\w|-]+) (?P<number>\d+)$", re.IGNORECASE | re.ASCII)
MAX_SPEED = re.compile(r"^(?P<speed>\d+)", re.IGNORECASE | re.ASCII)
YEAR_PREFIX = re.compile(r"^\d{4} - (.+)$", re.ASCII)
NUMBER_PREFIX = re.compile(r"^\d+ - (.+)$", re.ASCII)
PERMANENT_OR_TEMPORARY_PREFIX = re.compile(r"^A(?:P|Temp) - (.+)$")

POLYGON_TYPES = ("Polygon", "MultiPolygon")

TRANSFORMER = "App\\Infrastructure\\Integration\\Litteralis\\LitteralisTransformer"


class LitteralisTransformer:
    def __init__(self, period_parser: LitteralisPeriodParser):
        self.period_parser = period_parser

    def transform(
        self, reporter: Reporter, identifier: str, regulation_features: list[dict]
    ) -> ImportCommand | None:
        if not regulation_features:
            return None

        # (1) General info, read on the first emprise.
        properties = php.key(regulation_features[0], "properties")

        general_info = GeneralInfo(
            identifier=identifier,
            category=self._category(properties),
            subject="",
            title="",
        )
        general_info.subject, general_info.other_category_text = self._subject(properties)
        general_info.title = self._build_description(properties)

        # (2) Measures: each supported measure of each emprise, with its geometry.
        measures: list[Measure] = []

        for feature in regulation_features:
            location = self._parse_location(feature, reporter, properties)
            if location is None:
                continue

            for measure in self._parse_measures(php.key(feature, "properties"), reporter):
                measure.permissions.append(CAN_USE_RAW_GEOJSON)
                measure.locations.append(location)
                measures.append(measure)

        # Errors are only acted upon at the end, so that all of them are reported.
        if reporter.has_new_errors():
            return None

        if not measures:
            reporter.add_notice(
                rec.NOTICE_NO_MEASURES_FOUND,
                {
                    rec.ATTR_REGULATION_ID: php.key(properties, "arretesrcid"),
                    rec.ATTR_URL: php.key(properties, "shorturl"),
                },
            )
            return None

        download_url = properties.get("shorturl")
        if download_url is not None and not isinstance(download_url, str):
            raise php.PhpFatalError(
                "TypeError", "ImportLitteralisRegulationCommand: $downloadUrl must be ?string"
            )
        return ImportCommand(
            general_info=general_info, measures=measures, download_url=download_url
        )

    @staticmethod
    def _category(properties: dict) -> str:
        if php.key(properties, "documenttype") == "ARRETE PERMANENT":
            return PERMANENT_REGULATION
        return TEMPORARY_REGULATION

    @staticmethod
    def _subject(properties: dict) -> tuple[str, str | None]:
        value = php.key(properties, "categoriesmodele")
        if value == "Travaux":
            return ROAD_MAINTENANCE, None
        if value == "Evenements":
            return EVENT, None
        if value is not None and not isinstance(value, str):
            raise php.PhpFatalError(
                "TypeError", "Cannot assign non-string to property ?string $otherCategoryText"
            )
        return OTHER, value

    def _build_description(self, properties: dict) -> str:
        description = self._parse_description(properties)
        return f"{description} (URL : {php.to_string(php.key(properties, 'shorturl'))})"

    def _parse_description(self, properties: dict) -> str:
        parameters = self._parse_regulation_parameters(properties)

        description = find_parameter_value(parameters, "Description des travaux")
        if not php.is_falsy(description):
            return description  # type: ignore[return-value]

        description = find_parameter_value(parameters, "Description évènement")
        if not php.is_falsy(description):
            return description  # type: ignore[return-value]

        # Less descriptive but always present. The return type is `string`.
        return php.require_string(
            php.key(properties, "nommodele"), f"{TRANSFORMER}::parseDescription(): Return value"
        )

    def _parse_regulation_parameters(self, properties: dict) -> list[tuple[str, str]]:
        """`parametresarrete`: "KEY : VALUE" items separated by ";"."""
        parameters = []
        for item in parse_separated_string(php.key(properties, "parametresarrete"), ";"):
            parts = item.split(" :", 1)
            name = php.trim(parts[0])
            value = php.trim(parts[1]) if len(parts) > 1 else None
            if php.is_falsy(name) or php.is_falsy(value):
                continue
            parameters.append((name, value))
        return parameters  # type: ignore[return-value]

    @staticmethod
    def _parse_location(
        feature: dict, reporter: Reporter, regulation_properties: dict
    ) -> RawGeoJSONLocation | None:
        geometry = feature.get("geometry")
        if php.is_falsy(geometry) or not isinstance(geometry, dict):
            reporter.add_notice(
                rec.NOTICE_MISSING_GEOMETRY_EXCLUDED,
                {
                    rec.ATTR_REGULATION_ID: php.key(regulation_properties, "arretesrcid"),
                    rec.ATTR_URL: php.key(regulation_properties, "shorturl"),
                    rec.ATTR_DETAILS: {"idemprise": _idemprise_or_null(feature)},
                },
            )
            return None

        geometry_type = geometry.get("type")
        if geometry_type in POLYGON_TYPES:
            reporter.add_notice(
                rec.NOTICE_POLYGON_LOCATION_EXCLUDED,
                {
                    rec.ATTR_REGULATION_ID: php.key(regulation_properties, "arretesrcid"),
                    rec.ATTR_URL: php.key(regulation_properties, "shorturl"),
                    rec.ATTR_DETAILS: {
                        "idemprise": _idemprise_or_null(feature),
                        "geometry_type": geometry_type,
                    },
                },
            )
            return None

        properties = php.key(feature, "properties")
        localisations = properties.get("localisations")
        label = php.trim(
            php.require_string(
                localisations if localisations is not None else "Localisation sans description",
                "trim()",
            )
        )
        return RawGeoJSONLocation(label=label, geometry=php.json_encode(geometry))

    def _parse_measures(self, properties: dict, reporter: Reporter) -> list[Measure]:
        parameters_by_name = self._gather_measure_parameters(properties, reporter)

        measures = []
        for name, parameters in parameters_by_name.items():
            measure = Measure(type=php.key(MEASURE_MAP, normalize_measure_name(name)))
            if measure.type == SPEED_LIMITATION:
                measure.max_speed = self._parse_max_speed(properties, parameters, reporter)
            measure.vehicle_set = self._parse_vehicle_set(parameters)
            measure.periods = self.period_parser.parse_periods(parameters, properties, reporter)
            measures.append(measure)
        return measures

    def _gather_measure_parameters(
        self, properties: dict, reporter: Reporter
    ) -> dict[str, list[tuple[str, str]]]:
        """The parameters of each supported measure of the emprise, by measure name.

        `mesures` lists the names separated by ";". `parametresmesures` lists
        "NAME | KEY : VALUE" items separated by ";", where NAME may carry a number that
        must equal the 1-based position of the measure in `mesures`; a mismatch is an
        error, since the parameter cannot be attached. `parametresemprise` holds
        "KEY : VALUE" items that apply to every measure, appended last.
        """
        all_names = parse_separated_string(php.key(properties, "mesures"), ";")

        names: list[str] = []
        for name in all_names:
            if normalize_measure_name(name) not in MEASURE_MAP:
                reporter.add_notice(
                    rec.NOTICE_UNSUPPORTED_MEASURE,
                    {
                        rec.ATTR_REGULATION_ID: php.key(properties, "arretesrcid"),
                        rec.ATTR_DETAILS: {
                            "name": name,
                            "idemprise": php.key(properties, "idemprise"),
                        },
                    },
                )
                continue
            names.append(name)

        measure_parameters = parse_separated_string(php.key(properties, "parametresmesures"), ";")

        parameters_by_name: dict[str, list[tuple[str, str]]] = {name: [] for name in names}

        for param in measure_parameters:
            parts = param.split("|", 1)
            if len(parts) != 2:
                reporter.add_error(
                    rec.ERROR_MEASURE_PARAMETER_MALFORMED,
                    {
                        rec.ATTR_REGULATION_ID: php.key(properties, "arretesrcid"),
                        rec.ATTR_URL: php.key(properties, "shorturl"),
                        rec.ATTR_DETAILS: {
                            "idemprise": php.key(properties, "idemprise"),
                            "param": param,
                        },
                    },
                )
                continue

            name, item = (php.trim(part) for part in parts)

            numbered = NUMBERED_NAME.match(name)
            if numbered:
                cleaned_name = numbered["name"]
                number = int(numbered["number"])

                if not php.in_array(cleaned_name, names):
                    continue

                index = php.array_search(cleaned_name, all_names)
                expected = (index if index is not None else 0) + 1
                if number != expected:
                    reporter.add_error(
                        rec.ERROR_MEASURE_PARAMETER_INCONSISTENT_NUMBER,
                        {
                            rec.ATTR_REGULATION_ID: php.key(properties, "arretesrcid"),
                            rec.ATTR_URL: php.key(properties, "shorturl"),
                            rec.ATTR_DETAILS: {
                                "idemprise": php.key(properties, "idemprise"),
                                "measureName": name,
                                "expected": expected,
                                "actual": number,
                            },
                        },
                    )
                    continue

                name = cleaned_name
            elif not php.in_array(name, names):
                continue

            key_value = item.split(" : ", 1)
            parameter_name = php.trim(key_value[0])
            parameter_value = php.trim(key_value[1]) if len(key_value) > 1 else None
            if php.is_falsy(parameter_name) or php.is_falsy(parameter_value):
                continue

            parameters_by_name[_array_key(parameters_by_name, name)].append(
                (parameter_name, parameter_value)  # type: ignore[arg-type]
            )

        for param in parse_separated_string(php.key(properties, "parametresemprise"), ";"):
            parts = param.split(":", 1)
            parameter_name = php.trim(parts[0])
            parameter_value = php.trim(parts[1]) if len(parts) > 1 else None
            if php.is_falsy(parameter_name) or php.is_falsy(parameter_value):
                continue
            for name in names:
                parameters_by_name[name].append((parameter_name, parameter_value))  # type: ignore[arg-type]

        return parameters_by_name

    @staticmethod
    def _parse_vehicle_set(parameters: list[tuple[str, str]]) -> VehicleSet:
        vehicle_set = VehicleSet()
        concerned = parse_separated_string(
            find_parameter_value(parameters, "véhicules concernés") or "", ","
        )

        restricted: list[str] = []
        others: list[str] = []
        for value in concerned:
            vehicle_type = VEHICLE_TYPES.get(value)
            if vehicle_type is None:
                others.append(value)
                continue
            restricted.append(vehicle_type)
            if vehicle_type == HEAVY_GOODS_VEHICLE:
                vehicle_set.heavyweight_max_weight = 3.5

        other_text = None
        if others:
            restricted.append(OTHER_VEHICLE)
            other_text = ", ".join(others)

        vehicle_set.restricted_types = restricted
        vehicle_set.other_restricted_type_text = other_text
        vehicle_set.all_vehicles = not restricted

        # "dérogations" only holds types DiaLog does not know: all of it goes to "other".
        derogations = find_parameter_value(parameters, "dérogations")
        vehicle_set.exempted_types = [OTHER_VEHICLE] if not php.is_falsy(derogations) else []
        vehicle_set.other_exempted_type_text = derogations
        return vehicle_set

    @staticmethod
    def _parse_max_speed(
        properties: dict, parameters: list[tuple[str, str]], reporter: Reporter
    ) -> int | None:
        value = find_parameter_value(parameters, "limite de vitesse")
        if value is None:
            value = find_parameter_value(parameters, "vitesse")

        if php.is_falsy(value):
            reporter.add_error(
                rec.ERROR_MAX_SPEED_VALUE_MISSING,
                {
                    rec.ATTR_REGULATION_ID: php.key(properties, "arretesrcid"),
                    rec.ATTR_URL: php.key(properties, "shorturl"),
                    rec.ATTR_DETAILS: {
                        "idemprise": php.key(properties, "idemprise"),
                        "mesures": php.key(properties, "mesures"),
                    },
                },
            )
            return None

        match = MAX_SPEED.match(value)  # type: ignore[arg-type]
        if not match:
            reporter.add_error(
                rec.ERROR_MAX_SPEED_VALUE_INVALID,
                {
                    rec.ATTR_REGULATION_ID: php.key(properties, "arretesrcid"),
                    rec.ATTR_URL: php.key(properties, "shorturl"),
                    rec.ATTR_DETAILS: {
                        "idemprise": php.key(properties, "idemprise"),
                        "limite de vitesse": value,
                    },
                },
            )
            return None

        return int(match["speed"])


def normalize_measure_name(name: str) -> str:
    """Drop the "YYYY - ", "N - ", "AP - " and "ATemp - " prefixes some producers add."""
    for prefix in (YEAR_PREFIX, NUMBER_PREFIX, PERMANENT_OR_TEMPORARY_PREFIX):
        match = prefix.match(name)
        if match:
            return match[1]
    return name


def parse_separated_string(text, separator: str) -> list[str]:
    """`explode` then `trim`, dropping what PHP finds falsy — "0" included.

    The PHP parameter is typed `string`: null, or a number, is a fatal TypeError.
    """
    text = php.require_string(text, f"{TRANSFORMER}::parseSeparatedString(): Argument #1")
    if php.is_falsy(text):
        return []
    values = []
    for raw in text.split(separator):
        cleaned = php.trim(raw)
        if not php.is_falsy(cleaned):
            values.append(cleaned)
    return values


def _array_key(array: dict[str, list], name: str) -> str:
    """The key under which PHP files `$array[$name][] = …`.

    Keys of a PHP array are compared strictly, but `in_array` above was loose: a name
    accepted by loose comparison may be absent as a key, and PHP then creates it.
    """
    if name not in array:
        array[name] = []
    return name


def _idemprise_or_null(feature: dict):
    """`$feature['properties']['idemprise'] ?? null`: no warning whatever is missing."""
    properties = feature.get("properties")
    return properties.get("idemprise") if isinstance(properties, dict) else None
