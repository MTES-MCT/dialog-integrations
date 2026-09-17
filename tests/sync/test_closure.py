"""Closing a regulation that left its source: read → write translation and clamping."""

from datetime import datetime
from zoneinfo import ZoneInfo

import pytest

from integrations.sync.closure import (
    ClosureNotRebuildable,
    close_payload,
    closing_instant,
    is_ended,
    save_payload_from_read,
)

PARIS = ZoneInfo("Europe/Paris")

# What GET /api/regulations/{identifier} returned on staging for a Lyon work site
# (2026-09-17): UTC instants, 1970-dated time slots, vehicle types as objects.
READ = {
    "uuid": "01a0a9ef-9bac-7c62-8074-10a4f24d9ba9",
    "identifier": "MGL-CHP-411747",
    "status": "published",
    "category": "temporaryRegulation",
    "subject": "roadMaintenance",
    "otherCategoryText": None,
    "title": "Travaux – Boulevard des Loisirs",
    "startDate": "2025-09-30T22:00:00+00:00",
    "endDate": "2028-10-30T22:59:00+00:00",
    "documentUrl": None,
    "organization": {"uuid": "x", "name": "Lyon (métropole)"},
    "measures": [
        {
            "uuid": "m1",
            "type": "noEntry",
            "maxSpeed": None,
            "vehicleSet": {
                "restrictedTypes": [],
                "exemptedTypes": [{"name": "localResident"}],
                "maxCharacteristics": [],
            },
            "periods": [
                {
                    "recurrenceType": "everyDay",
                    "startDateTime": "2025-09-30T22:00:00+00:00",
                    "endDateTime": "2028-10-30T22:59:00+00:00",
                    "dailyRange": None,
                    "timeSlots": [
                        {
                            "startTime": "1970-01-01T08:00:00+01:00",
                            "endTime": "1970-01-01T18:00:00+01:00",
                        }
                    ],
                }
            ],
            "locations": [
                {
                    "uuid": "l1",
                    "roadType": "rawGeoJSON",
                    "namedStreet": None,
                    "numberedRoad": None,
                    "rawGeoJSON": {"label": "Boulevard des Loisirs – Rillieux-la-Pape"},
                    "storageArea": None,
                    "zone": None,
                    "geometry": (
                        '{"type":"MultiLineString","coordinates":[[[4.9,45.8],[4.91,45.81]]]}'
                    ),
                }
            ],
        }
    ],
}


def test_the_read_regulation_becomes_a_write_payload():
    payload = save_payload_from_read(READ)

    assert payload["identifier"] == "MGL-CHP-411747"
    assert payload["status"] == "published"
    assert payload["title"] == "Travaux – Boulevard des Loisirs"
    measure = payload["measures"][0]
    assert measure["type"] == "noEntry"
    # Vehicle types come back as objects; `allVehicles` is not returned and is inferred.
    assert measure["vehicleSet"] == {"allVehicles": True, "exemptedTypes": ["localResident"]}
    period = measure["periods"][0]
    # UTC instants go back as Europe/Paris, the way the pipeline writes them.
    assert period["startDate"] == "2025-10-01T00:00:00+02:00"
    assert period["endDate"] == "2028-10-30T23:59:00+01:00"
    assert period["startTime"] == period["startDate"] and period["endTime"] == period["endDate"]
    assert period["isPermanent"] is False
    assert period["timeSlots"] == [
        {"startTime": "1970-01-01T08:00:00+01:00", "endTime": "1970-01-01T18:00:00+01:00"}
    ]
    location = measure["locations"][0]
    assert location["roadType"] == "rawGeoJSON"
    assert location["rawGeoJSON"]["label"] == "Boulevard des Loisirs – Rillieux-la-Pape"
    assert location["rawGeoJSON"]["geometry"].startswith('{"type":"MultiLineString"')


def test_a_restricted_type_means_not_all_vehicles():
    read = {**READ, "measures": [dict(READ["measures"][0])]}
    read["measures"][0]["vehicleSet"] = {
        "restrictedTypes": [{"name": "heavyGoodsVehicle"}],
        "exemptedTypes": [],
        "maxCharacteristics": [],
    }
    vehicle_set = save_payload_from_read(read)["measures"][0]["vehicleSet"]
    assert vehicle_set == {"allVehicles": False, "restrictedTypes": ["heavyGoodsVehicle"]}


def test_a_zone_is_written_back_as_its_computed_sections():
    """PUT answers 500 on a zone (S-14); its sections are what DiaLog applies anyway."""
    read = {**READ, "measures": [dict(READ["measures"][0])]}
    read["measures"][0]["locations"] = [
        {
            "roadType": "zone",
            "zone": {"label": "Rue Gentil – Lyon 2e"},
            "rawGeoJSON": None,
            "geometry": '{"type":"MultiLineString","coordinates":[[[4.83,45.76],[4.84,45.77]]]}',
        }
    ]
    location = save_payload_from_read(read)["measures"][0]["locations"][0]
    assert location["roadType"] == "rawGeoJSON"
    assert location["rawGeoJSON"] == {
        "label": "Rue Gentil – Lyon 2e",
        "geometry": '{"type":"MultiLineString","coordinates":[[[4.83,45.76],[4.84,45.77]]]}',
    }


@pytest.mark.parametrize(
    "vehicle_set",
    [
        {"restrictedTypes": [], "exemptedTypes": [], "maxCharacteristics": [{"name": "w"}]},
        {"restrictedTypes": [{"name": "other"}], "exemptedTypes": [], "maxCharacteristics": []},
        {"restrictedTypes": [{"label": "?"}], "exemptedTypes": [], "maxCharacteristics": []},
    ],
)
def test_what_cannot_be_rebuilt_faithfully_is_refused(vehicle_set):
    read = {**READ, "measures": [dict(READ["measures"][0])]}
    read["measures"][0]["vehicleSet"] = vehicle_set
    with pytest.raises(ClosureNotRebuildable):
        save_payload_from_read(read)


def test_an_unknown_location_type_is_refused():
    read = {**READ, "measures": [dict(READ["measures"][0])]}
    read["measures"][0]["locations"] = [{"roadType": "storageArea", "geometry": "{}"}]
    with pytest.raises(ClosureNotRebuildable):
        save_payload_from_read(read)


# --- Clamping -----------------------------------------------------------------------

CLOSED_AT = datetime(2026, 9, 16, 23, 59, 59, tzinfo=PARIS)


def _payload(start: str, end: str | None, permanent: bool = False) -> dict:
    return {
        "identifier": "X-1",
        "measures": [
            {
                "periods": [
                    {
                        "startDate": start,
                        "startTime": start,
                        "endDate": end,
                        "endTime": end,
                        "isPermanent": permanent,
                        "recurrenceType": "everyDay",
                        "timeSlots": [],
                    }
                ]
            }
        ],
    }


def test_closing_brings_a_running_period_back_to_yesterday():
    closed, changed = close_payload(
        _payload("2026-09-02T00:00:00+02:00", "2026-10-20T23:59:59+02:00"), CLOSED_AT
    )
    period = closed["measures"][0]["periods"][0]
    assert changed is True
    assert period["endDate"] == "2026-09-16T23:59:59+02:00"
    assert period["endTime"] == "2026-09-16T23:59:59+02:00"
    assert period["startDate"] == "2026-09-02T00:00:00+02:00"


def test_a_period_that_already_ended_is_left_alone():
    payload = _payload("2026-09-02T00:00:00+02:00", "2026-09-16T23:59:59+02:00")
    closed, changed = close_payload(payload, CLOSED_AT)
    assert changed is False
    assert closed == payload


def test_a_site_cancelled_before_it_started_ends_when_it_starts():
    closed, changed = close_payload(
        _payload("2026-10-05T00:00:00+02:00", "2026-10-10T23:59:59+02:00"), CLOSED_AT
    )
    period = closed["measures"][0]["periods"][0]
    assert changed is True
    assert period["endDate"] == "2026-10-05T00:00:00+02:00"


def test_a_permanent_period_becomes_temporary():
    closed, changed = close_payload(_payload("2026-01-01T00:00:00+01:00", None, True), CLOSED_AT)
    period = closed["measures"][0]["periods"][0]
    assert changed is True
    assert period["isPermanent"] is False
    assert period["endDate"] == "2026-09-16T23:59:59+02:00"


def test_the_original_payload_is_not_modified():
    payload = _payload("2026-09-02T00:00:00+02:00", "2026-10-20T23:59:59+02:00")
    close_payload(payload, CLOSED_AT)
    assert payload["measures"][0]["periods"][0]["endDate"] == "2026-10-20T23:59:59+02:00"


# --- closing_instant and is_ended -------------------------------------------------------


def test_the_closing_instant_is_the_end_of_yesterday_in_paris():
    now = datetime(2026, 9, 17, 0, 30, tzinfo=ZoneInfo("UTC"))  # 02:30 in Paris
    assert closing_instant(now) == datetime(2026, 9, 16, 23, 59, 59, tzinfo=PARIS)


def _digest(*ends, permanent=False):
    return {"measures": [{"periods": [{"endDate": end, "isPermanent": permanent} for end in ends]}]}


def test_is_ended_reads_the_snapshot_digest():
    assert is_ended(_digest("2026-09-16T23:59:59+02:00"), CLOSED_AT) is True
    assert (
        is_ended(_digest("2026-09-10T23:59:59+02:00", "2026-09-17T23:59:59+02:00"), CLOSED_AT)
        is False
    )
    assert is_ended(_digest(None, permanent=True), CLOSED_AT) is False
    assert is_ended({"measures": []}, CLOSED_AT) is False
    assert is_ended({"measures": [{"periods": [{"endDate": "garbage"}]}]}, CLOSED_AT) is False
