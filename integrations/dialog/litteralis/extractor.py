"""Port of the back end's two Litteralis extractors.

Both fetch the features of one layer whose measures DiaLog may know and whose
regulation has not ended, and group them by regulation under the identifier
`{collectiviteagenceid}#{arretesrcid}`:

- the standard flow (`litteralis:litteralis`): one feature is one emprise;
- the "Communication" flow (`litteralis:LIcommunication`): one feature is one measure
  of an emprise. Its measures are merged back into one synthetic emprise per
  `idemprise`, carrying `mesures` and `parametresmesures` as the standard flow writes
  them, so that the same transformer reads both.
"""

from datetime import datetime

from integrations.dialog.litteralis import php
from integrations.dialog.litteralis import reporter as rec
from integrations.dialog.litteralis.client import LitteralisClient
from integrations.dialog.litteralis.reporter import Reporter

MEASURE_ILIKE_ITEMS = [
    "%circulation interdite%",
    "%limitation de vitesse%",
    "%interruption de circulation%",
    "%interdiction de stationnement%",
    "%interdiction de dépasser%",
]


def build_cql_filter(field: str, later_than: datetime) -> str:
    """`(mesures ILIKE '…' OR …) AND (arretefin IS NULL OR arretefin >= '…')`.

    Only the regulation's end is filtered: an emprise that ended inside a regulation
    still running is fetched, and imported with its past dates.
    """
    quoted = [f"'{item}'" for item in MEASURE_ILIKE_ITEMS]
    measures_filter = f"{field} ILIKE " + f" OR {field} ILIKE ".join(quoted)
    end_filter = f"arretefin IS NULL OR arretefin >= '{php.format_iso8601(later_than)}'"
    return f"({measures_filter}) AND ({end_filter})"


def _count_and_fetch(client: LitteralisClient, cql_filter: str, reporter: Reporter) -> list[dict]:
    reporter.add_count(rec.COUNT_TOTAL_FEATURES, client.count(None))
    reporter.add_count(rec.COUNT_MATCHING_FEATURES, client.count(cql_filter))
    return client.fetch_all_paginated(cql_filter)


def _with_crs(feature: dict) -> dict:
    """The coordinates are EPSG:4326 but the geometry does not say so, and PostGIS
    wants it said: the `crs` member is added (or replaced in place)."""
    geometry = feature["geometry"]
    if not isinstance(geometry, dict):
        raise php.PhpFatalError("Error", "Cannot access offset on a non-array geometry")
    geometry["crs"] = {"type": "name", "properties": {"name": "EPSG:4326"}}
    return feature


def extract_standard(
    client: LitteralisClient, later_than: datetime, reporter: Reporter
) -> dict[str, list[dict]]:
    """`LitteralisExtractor::extractFeaturesByRegulation`."""
    cql_filter = build_cql_filter("mesures", later_than)
    features = _count_and_fetch(client, cql_filter, reporter)

    by_regulation: dict[str, list[dict]] = {}
    extracted = 0

    for feature in features:
        properties = php.key(feature, "properties")
        identifier = php.key(properties, "arretesrcid")

        if php.is_falsy(feature.get("geometry")):
            # The geometry is sometimes missing.
            reporter.add_warning(
                rec.WARNING_MISSING_GEOMETRY,
                {
                    rec.ATTR_REGULATION_ID: identifier,
                    rec.ATTR_URL: php.key(properties, "shorturl"),
                    rec.ATTR_DETAILS: {"idemprise": php.key(properties, "idemprise")},
                },
            )
            continue

        _with_crs(feature)
        # Prefixed with the agency, so that two producers cannot share an identifier.
        unique_identifier = (
            f"{php.to_string(php.key(properties, 'collectiviteagenceid'))}"
            f"#{php.to_string(identifier)}"
        )
        by_regulation.setdefault(unique_identifier, []).append(feature)
        extracted += 1

    reporter.add_count(
        rec.COUNT_EXTRACTED_FEATURES, extracted, {"regulationsCount": len(by_regulation)}
    )
    return by_regulation


def extract_communication(
    client: LitteralisClient, later_than: datetime, reporter: Reporter
) -> dict[str, list[dict]]:
    """`LitteralisCommunicationExtractor::extractFeaturesByRegulation`."""
    cql_filter = build_cql_filter("mesure", later_than)
    features = _count_and_fetch(client, cql_filter, reporter)

    # One feature per measure: grouped by (agency, regulation, emprise) first.
    by_emprise: dict[str, list[dict]] = {}
    extracted = 0

    for feature in features:
        properties = feature.get("properties")
        properties = properties if properties is not None else []
        identifier = _get(properties, "arretesrcid", "")
        idemprise = _get(properties, "idemprise", None)

        if php.is_falsy(feature.get("geometry")):
            reporter.add_warning(
                rec.WARNING_MISSING_GEOMETRY,
                {
                    rec.ATTR_REGULATION_ID: identifier,
                    rec.ATTR_URL: _get(properties, "shorturl", ""),
                    rec.ATTR_DETAILS: {"idemprise": idemprise},
                },
            )
            continue

        _with_crs(feature)
        agency = _coalesce(properties, "collectiviteagenceid", "idagence")
        emprise_key = (
            f"{php.to_string(agency)}#{php.to_string(identifier)}#{php.to_string(idemprise)}"
        )
        by_emprise.setdefault(emprise_key, []).append(feature)
        extracted += 1

    # One synthetic emprise per group, in the standard flow's shape.
    by_regulation: dict[str, list[dict]] = {}
    for measure_features in by_emprise.values():
        first = measure_features[0]
        first_properties = php.key(first, "properties")

        names: list[str] = []
        parameter_parts: list[str] = []
        for measure_feature in measure_features:
            properties = php.key(measure_feature, "properties")
            name = php.trim(php.to_string(_get(properties, "mesure", "")))
            if name == "":
                continue
            names.append(name)

            parameters = php.trim(php.to_string(_get(properties, "parametresmesure", "")))
            if parameters != "":
                for item in _split_non_empty(parameters, ";"):
                    parameter_parts.append(f"{name} | {item}")

        synthetic_properties = dict(first_properties)
        synthetic_properties["mesures"] = ";".join(names)
        synthetic_properties["parametresmesures"] = " ; ".join(parameter_parts)
        if synthetic_properties.get("parametresemprise") is None:
            synthetic_properties["parametresemprise"] = ""

        synthetic_feature = {"geometry": first["geometry"], "properties": synthetic_properties}
        agency = _coalesce(first_properties, "collectiviteagenceid", "idagence")
        unique_identifier = (
            f"{php.to_string(agency)}#{php.to_string(php.key(first_properties, 'arretesrcid'))}"
        )
        by_regulation.setdefault(unique_identifier, []).append(synthetic_feature)

    reporter.add_count(
        rec.COUNT_EXTRACTED_FEATURES, extracted, {"regulationsCount": len(by_regulation)}
    )
    return by_regulation


def _get(properties, name: str, default):
    """`$properties['name'] ?? $default`."""
    if isinstance(properties, dict) and properties.get(name) is not None:
        return properties[name]
    return default


def _coalesce(properties, first: str, second: str):
    """`$properties['first'] ?? $properties['second'] ?? ''`."""
    return _get(properties, first, _get(properties, second, ""))


def _split_non_empty(text: str, separator: str) -> list[str]:
    """The Communication extractor's own split: only "" is dropped, "0" is kept."""
    values = []
    for raw in text.split(separator):
        cleaned = php.trim(raw)
        if cleaned != "":
            values.append(cleaned)
    return values
