"""Tests for dating the undated permanent periods at the moment of writing (R-39)."""

import json
from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo

from api.dia_log_client.models import (
    MeasureTypeEnum,
    PostApiRegulationsAddBody,
    PostApiRegulationsAddBodyCategory,
    PostApiRegulationsAddBodyStatus,
    PostApiRegulationsAddBodySubject,
    RoadTypeEnum,
    SaveLocationDTO,
    SaveMeasureDTO,
    SavePeriodDTO,
    SaveRawGeoJSONDTO,
    SaveVehicleSetDTO,
)
from integrations.sync.dating import date_creation, date_update, has_undated_period, run_day
from integrations.sync.state import compute_regulation_digest, fingerprint
from tests.sync.test_closure import READ
from tests.sync_fixtures import geometry

DAY = "2026-09-19T00:00:00+02:00"


def _measure(
    type_=MeasureTypeEnum.SPEEDLIMITATION,
    max_speed: int | None = 30,
    start=None,
    permanent=True,
    vehicle_set=None,
) -> SaveMeasureDTO:
    period = SavePeriodDTO(
        start_date=start,
        start_time=start,
        end_date=None if permanent else "2026-02-01T23:59:59+01:00",
        is_permanent=permanent,
        recurrence_type="everyDay",  # type: ignore[arg-type]
    )
    return SaveMeasureDTO(
        type_=type_,
        max_speed=max_speed,
        periods=[period],
        locations=[
            SaveLocationDTO(
                road_type=RoadTypeEnum.RAWGEOJSON,
                raw_geo_json=SaveRawGeoJSONDTO(label="rue Exemple", geometry=geometry()),
            )
        ],
        vehicle_set=vehicle_set or SaveVehicleSetDTO(all_vehicles=True),
    )


def _regulation(*measures: SaveMeasureDTO) -> PostApiRegulationsAddBody:
    return PostApiRegulationsAddBody(
        identifier="MGL-CT-V30",
        category=PostApiRegulationsAddBodyCategory.PERMANENTREGULATION,
        status=PostApiRegulationsAddBodyStatus.DRAFT,
        subject=PostApiRegulationsAddBodySubject.OTHER,
        title="Limitation à 30 km/h",
        other_category_text=None,
        measures=list(measures),  # type: ignore[arg-type]
    )


def _read(*measures: dict) -> dict:
    read = json.loads(json.dumps(READ))
    read["measures"] = list(measures)
    return read


def _read_measure(
    type_="speedLimitation",
    max_speed: int | None = 30,
    start="2026-09-17T22:00:00+00:00",
    **vs,
):
    vehicle_set = {"restrictedTypes": [], "exemptedTypes": [], "maxCharacteristics": []}
    vehicle_set.update(vs)
    return {
        "type": type_,
        "maxSpeed": max_speed,
        "vehicleSet": vehicle_set,
        "periods": [{"recurrenceType": "everyDay", "startDateTime": start, "endDateTime": None}],
        "locations": [],
    }


def _period(regulation: Any, index=0) -> Any:
    return regulation.measures[index].periods[0]


def _start(regulation, index=0) -> str | None:
    return _period(regulation, index).start_date


# --- The run day ------------------------------------------------------------------


def test_the_run_day_is_french_midnight_with_the_offset_of_that_day():
    assert run_day(datetime(2026, 9, 19, 1, 30, tzinfo=ZoneInfo("UTC"))) == DAY
    assert run_day(datetime(2026, 1, 15, 12, tzinfo=ZoneInfo("UTC"))) == "2026-01-15T00:00:00+01:00"


def test_the_run_day_is_taken_on_the_french_calendar():
    # 23:30 UTC on the 18th is already the 19th in Paris.
    assert run_day(datetime(2026, 9, 18, 23, 30, tzinfo=ZoneInfo("UTC"))) == DAY


# --- Creation ------------------------------------------------------------------------


def test_a_creation_dates_the_undated_period_on_the_run_day():
    regulation = _regulation(_measure())
    assert has_undated_period(regulation)

    assert date_creation(regulation, DAY) == 1

    period = _period(regulation)
    assert (period.start_date, period.start_time) == (DAY, DAY)
    assert not has_undated_period(regulation)


def test_a_dated_period_is_never_touched():
    dated = _regulation(_measure(start="2002-02-19T00:00:00+01:00"))
    temporary = _regulation(_measure(start="2026-01-01T00:00:00+01:00", permanent=False))

    assert date_creation(dated, DAY) == 0 and date_creation(temporary, DAY) == 0
    assert _start(dated) == "2002-02-19T00:00:00+01:00"
    assert _start(temporary) == "2026-01-01T00:00:00+01:00"
    assert not has_undated_period(dated) and not has_undated_period(temporary)


# --- Update --------------------------------------------------------------------------


def test_an_update_keeps_the_date_dialog_holds():
    regulation = _regulation(_measure())

    assert date_update(regulation, _read(_read_measure()), DAY) == 1

    # 2026-09-17T22:00Z is French midnight on the 18th: the day it was first published.
    assert _start(regulation) == "2026-09-18T00:00:00+02:00"
    assert _period(regulation).start_time == "2026-09-18T00:00:00+02:00"


def test_one_measure_on_both_sides_is_the_same_measure_whatever_its_shape():
    # The speed changed: the regulation is still the same one, first published then.
    regulation = _regulation(_measure(max_speed=30))

    date_update(regulation, _read(_read_measure(max_speed=50)), DAY)

    assert _start(regulation) == "2026-09-18T00:00:00+02:00"


def test_measures_are_matched_by_signature_when_there_are_several():
    regulation = _regulation(_measure(max_speed=70), _measure(max_speed=90))
    read = _read(
        _read_measure(max_speed=90, start="2026-03-01T23:00:00+00:00"),
        _read_measure(max_speed=70, start="2026-06-01T22:00:00+00:00"),
    )

    date_update(regulation, read, DAY)

    assert _start(regulation, 0) == "2026-06-02T00:00:00+02:00"
    assert _start(regulation, 1) == "2026-03-02T00:00:00+01:00"


def test_a_measure_dialog_does_not_hold_is_new_and_dated_today():
    regulation = _regulation(_measure(max_speed=70), _measure(max_speed=90))

    date_update(regulation, _read(_read_measure(max_speed=70)), DAY)

    assert _start(regulation, 0) == "2026-09-18T00:00:00+02:00"
    assert _start(regulation, 1) == DAY


def test_a_dimension_limit_is_matched_on_what_dialog_reads_back():
    # Posted as restrictedTypes + heavyweightMaxWeight, read back as maxCharacteristics
    # with an empty restrictedTypes (api-dialog.md, piège 5).
    tonnage = _measure(
        type_=MeasureTypeEnum.NOENTRY,
        max_speed=None,
        vehicle_set=SaveVehicleSetDTO(
            all_vehicles=False, restricted_types=["heavyGoodsVehicle"], heavyweight_max_weight=3.5
        ),
    )
    height = _measure(
        type_=MeasureTypeEnum.NOENTRY,
        max_speed=None,
        vehicle_set=SaveVehicleSetDTO(
            all_vehicles=False, restricted_types=["dimensions"], max_height=4.1
        ),
    )
    regulation = _regulation(tonnage, height)
    read = _read(
        _read_measure(
            "noEntry",
            None,
            start="2026-01-01T23:00:00+00:00",
            maxCharacteristics=[{"name": "height", "value": 4.1}],
        ),
        _read_measure(
            "noEntry",
            None,
            start="2026-02-01T23:00:00+00:00",
            maxCharacteristics=[{"name": "weight", "value": 3.5}],
        ),
    )

    date_update(regulation, read, DAY)

    assert _start(regulation, 0) == "2026-02-02T00:00:00+01:00"
    assert _start(regulation, 1) == "2026-01-02T00:00:00+01:00"


def test_an_exemption_tells_a_pedestrian_area_from_a_closure():
    closure = _measure(type_=MeasureTypeEnum.NOENTRY, max_speed=None)
    area = _measure(
        type_=MeasureTypeEnum.NOENTRY,
        max_speed=None,
        vehicle_set=SaveVehicleSetDTO(all_vehicles=True, exempted_types=["desserteLocale"]),
    )
    regulation = _regulation(closure, area)
    read = _read(
        _read_measure(
            "noEntry",
            None,
            start="2026-01-01T23:00:00+00:00",
            exemptedTypes=[{"name": "desserteLocale"}],
        ),
        _read_measure("noEntry", None, start="2026-02-01T23:00:00+00:00"),
    )

    date_update(regulation, read, DAY)

    assert _start(regulation, 0) == "2026-02-02T00:00:00+01:00"
    assert _start(regulation, 1) == "2026-01-02T00:00:00+01:00"


def test_a_read_without_any_period_falls_back_to_the_run_day():
    regulation = _regulation(_measure())
    read = _read(_read_measure())
    read["measures"][0]["periods"] = []

    date_update(regulation, read, DAY)

    assert _start(regulation) == DAY


# --- The comparison --------------------------------------------------------------------


def test_the_undated_period_is_digested_as_null_so_the_run_day_never_counts_as_a_change():
    monday = _regulation(_measure())
    tuesday = _regulation(_measure())
    before = compute_regulation_digest(monday)
    date_creation(monday, "2026-09-18T00:00:00+02:00")
    date_creation(tuesday, DAY)

    assert before["measures"][0]["periods"][0]["startDate"] is None
    assert fingerprint(before) == fingerprint(compute_regulation_digest(_regulation(_measure())))
    # Dating happens on the DTO after the digest was taken: the snapshot keeps the null.
    assert fingerprint(compute_regulation_digest(monday)) != fingerprint(before)
    assert _start(monday) != _start(tuesday)
