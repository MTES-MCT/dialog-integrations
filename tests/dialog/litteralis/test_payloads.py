"""From the PHP import's commands to the API: same rows stored, two compensations."""

from datetime import datetime, timedelta, timezone

import pytest

from api.dia_log_client.models import PostApiRegulationsAddBodyStatus
from integrations.dialog.litteralis.commands import (
    GeneralInfo,
    ImportCommand,
    Measure,
    Period,
    RawGeoJSONLocation,
    TimeSlot,
    VehicleSet,
)
from integrations.dialog.litteralis.payloads import (
    api_time_slot_clock,
    regulation_body,
    storage_verdict,
)

UTC = timezone.utc


def api_stored_clock(sent: str) -> str:
    """What the API stores for a time slot, as `PeriodsTransformer::timeToClientUtc`
    does since 2026-09-08: the wall clock received, read in Europe/Paris on 1970-01-01
    (UTC+1), converted to UTC."""
    wall = datetime.fromisoformat(sent).strftime("%H:%M")
    hours, minutes = (int(part) for part in wall.split(":"))
    stored = datetime(1970, 1, 1, hours, minutes) - timedelta(hours=1)
    return stored.strftime("%H:%M:%S")


@pytest.mark.parametrize(
    "stored",
    ["06:00", "15:00", "00:00", "23:30", "22:59", "05:45"],
)
def test_the_api_stores_the_clock_the_php_import_stores(stored):
    hours, minutes = (int(part) for part in stored.split(":"))
    php_value = datetime(2026, 10, 12, hours, minutes, tzinfo=UTC)

    sent = api_time_slot_clock(php_value)

    assert sent.startswith("1970-01-0") and sent.endswith("+01:00")
    assert api_stored_clock(sent) == f"{stored}:00"


def test_the_sent_clock_is_the_same_instant_as_the_stored_one():
    sent = api_time_slot_clock(datetime(2026, 7, 1, 6, 0, tzinfo=UTC))
    assert sent == "1970-01-01T07:00:00+01:00"
    assert datetime.fromisoformat(sent).astimezone(UTC).strftime("%H:%M") == "06:00"


def command(download_url: str | None = "https://dl.sogelink.fr/?abc") -> ImportCommand:
    start = datetime(2026, 10, 18, 22, 0, tzinfo=UTC)
    return ImportCommand(
        general_info=GeneralInfo(
            identifier="117374#26/0194",
            category="temporaryRegulation",
            subject="other",
            title="Braderie (URL : https://dl.sogelink.fr/?abc)",
            other_category_text="ODP",
        ),
        measures=[
            Measure(
                type="speedLimitation",
                max_speed=30,
                permissions=["canUseRawGeoJSON"],
                vehicle_set=VehicleSet(
                    all_vehicles=False,
                    restricted_types=["heavyGoodsVehicle", "other"],
                    other_restricted_type_text="bus",
                    heavyweight_max_weight=3.5,
                    exempted_types=["other"],
                    other_exempted_type_text="riverains",
                ),
                periods=[
                    Period(
                        start_date=start,
                        start_time=start,
                        end_date=start + timedelta(days=2),
                        end_time=start + timedelta(days=2),
                        is_permanent=False,
                        recurrence_type="everyDay",
                        time_slots=[
                            TimeSlot(
                                datetime(2026, 10, 12, 6, 0, tzinfo=UTC),
                                datetime(2026, 10, 12, 15, 0, tzinfo=UTC),
                            )
                        ],
                    )
                ],
                locations=[RawGeoJSONLocation(label="RUE X", geometry='{"type":"LineString"}')],
            ),
            Measure(
                type="noEntry",
                vehicle_set=VehicleSet(all_vehicles=True),
                periods=[
                    Period(
                        start_date=start,
                        start_time=start,
                        is_permanent=True,
                        recurrence_type="everyDay",
                    )
                ],
                locations=[RawGeoJSONLocation(label="RUE X", geometry='{"type":"LineString"}')],
            ),
        ],
        download_url=download_url,
    )


def test_the_regulation_body_carries_every_field_the_import_sets():
    body = regulation_body(command(), PostApiRegulationsAddBodyStatus.PUBLISHED).to_dict()

    assert body["identifier"] == "117374#26/0194"
    assert body["status"] == "published"
    assert body["otherCategoryText"] == "ODP"
    speed, no_entry = body["measures"]
    assert speed["type"] == "speedLimitation" and speed["maxSpeed"] == 30
    assert speed["vehicleSet"] == {
        "allVehicles": False,
        "restrictedTypes": ["heavyGoodsVehicle", "other"],
        "otherRestrictedTypeText": "bus",
        "heavyweightMaxWeight": 3.5,
        "exemptedTypes": ["other"],
        "otherExemptedTypeText": "riverains",
    }
    period = speed["periods"][0]
    assert period["startDate"] == period["startTime"] == "2026-10-18T22:00:00+00:00"
    assert period["endDate"] == period["endTime"] == "2026-10-20T22:00:00+00:00"
    assert period["timeSlots"] == [
        {"startTime": "1970-01-01T07:00:00+01:00", "endTime": "1970-01-01T16:00:00+01:00"}
    ]
    assert speed["locations"] == [
        {
            "roadType": "rawGeoJSON",
            "rawGeoJSON": {"label": "RUE X", "geometry": '{"type":"LineString"}'},
        }
    ]
    assert "maxSpeed" not in no_entry
    assert no_entry["vehicleSet"] == {"allVehicles": True}
    assert no_entry["periods"][0]["isPermanent"] is True
    assert no_entry["periods"][0]["endDate"] is None


@pytest.mark.parametrize(
    "url, verdict",
    [
        (None, "none"),
        ("", "empty"),
        ("https://dl.sogelink.fr/?abc", "attach"),
        ("http://dl.sogelink.fr/?abc", "invalid"),
        ("https://localhost/x", "invalid"),
    ],
)
def test_the_document_url_is_judged_before_the_regulation_is_sent(url, verdict):
    assert storage_verdict(command(url)) == verdict
