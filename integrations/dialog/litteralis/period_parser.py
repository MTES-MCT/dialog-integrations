"""Port of the back end's `LitteralisPeriodParser`: one period per measure, from the
regulation or emprise dates, and at most one daily time slot read from the
"jours et horaires" parameter.

Reproduced as is, including what looks wrong:

- a time slot is stored in UTC after reading its clock in Europe/Paris *on the day the
  import runs*: the same "8h à 17h" is stored 06:00–15:00 in summer and 07:00–16:00 in
  winter;
- one-digit minutes ("de 8h5 à 17h") make PHP call a method on `false`, an `Error` that
  aborts the import of the whole organization;
- "0" is accepted as a separator between the two hours ("DE 7 H 30 0 17 H 30");
- a missing end date makes the period permanent, whatever the regulation's category.
"""

import re
from datetime import date, datetime, tzinfo
from zoneinfo import ZoneInfo

from integrations.dialog.litteralis import php
from integrations.dialog.litteralis import reporter as rec
from integrations.dialog.litteralis.commands import Period, TimeSlot
from integrations.dialog.litteralis.reporter import Reporter

EVERY_DAY = "everyDay"
DATE_FORMAT = "Y-m-d\\TH:i:sO"

# PHP: '/^(?:la nuit |de nuit )?(?:de )?(?P<startHour>\d{1,2}) ?(?:h|heures?|:)?
# ?(?P<startMinute>\d{1,2})? (?:à|À|0|[aA]) ?(?P<endHour>\d{1,2}) ?(?:h|heures?|:)?
# ?(?P<endMinute>\d{1,2})?\.?$/i', without the `u` modifier: case folding and `\d` are
# ASCII-only, so "à" and "À" are two distinct alternatives.
HOURS_REGEX = re.compile(
    r"^(?:la nuit |de nuit )?(?:de )?(?P<startHour>\d{1,2}) ?(?:h|heures?|:)? ?"
    r"(?P<startMinute>\d{1,2})? (?:à|À|0|[aA]) ?(?P<endHour>\d{1,2}) ?(?:h|heures?|:)? ?"
    r"(?P<endMinute>\d{1,2})?\.?$",
    re.IGNORECASE | re.ASCII,
)

ALL_HOURS_VALUES = [
    "jour et nuit",
    # Corrèze (CD19)
    "24/24 7j/7j",
    "TOUS",
    # Montpellier
    "la journée",
    # Savoie
    "chaque jour de la période, 24h/24",
]


class LitteralisPeriodParser:
    def __init__(self, zone: tzinfo | None = None, today: date | None = None):
        self.zone = zone or ZoneInfo("Europe/Paris")
        # The day PHP fills in when it reads a bare clock: today, in `zone`.
        self.today = today or datetime.now(self.zone).date()

    def parse_periods(
        self, parameters: list[tuple[str, str]], properties: dict, reporter: Reporter
    ) -> list[Period]:
        period = Period()
        self._set_period_dates(period, properties, reporter)
        self._normalize_period_days(period)
        self._set_period_days_and_times(period, parameters, properties, reporter)
        return [period]

    def _set_period_dates(self, period: Period, properties: dict, reporter: Reporter) -> None:
        start_property = (
            "emprisedebut"
            if not php.is_falsy(php.key(properties, "emprisedebut"))
            else "arretedebut"
        )
        start_value = php.require_string(
            php.key(properties, start_property), "DateTimeImmutable::createFromFormat()"
        )
        start = php.create_from_iso8601(start_value)

        if start is not None:
            period.start_date = start
            period.start_time = start
        else:
            reporter.add_error(
                rec.ERROR_DATE_PARSING_FAILED,
                {
                    rec.ATTR_REGULATION_ID: php.key(properties, "arretesrcid"),
                    rec.ATTR_URL: php.key(properties, "shorturl"),
                    rec.ATTR_DETAILS: {
                        "idemprise": php.key(properties, "idemprise"),
                        start_property: start_value,
                        "format": DATE_FORMAT,
                    },
                },
            )

        end_property = (
            "emprisefin" if not php.is_falsy(php.key(properties, "emprisefin")) else "arretefin"
        )
        period.is_permanent = True

        if php.is_falsy(php.key(properties, end_property)):
            return

        period.is_permanent = False
        end_value = php.require_string(
            php.key(properties, end_property), "DateTimeImmutable::createFromFormat()"
        )
        end = php.create_from_iso8601(end_value)

        if end is not None:
            period.end_date = end
            period.end_time = end
        else:
            reporter.add_error(
                rec.ERROR_DATE_PARSING_FAILED,
                {
                    rec.ATTR_REGULATION_ID: php.key(properties, "arretesrcid"),
                    rec.ATTR_URL: php.key(properties, "shorturl"),
                    rec.ATTR_DETAILS: {
                        "idemprise": php.key(properties, "idemprise"),
                        end_property: end_value,
                        "format": DATE_FORMAT,
                    },
                },
            )

    def _normalize_period_days(self, period: Period) -> None:
        """Same start and end instant: one whole day, midnight to 23:59 in `zone`."""
        if period.start_date is None or period.end_date is None:
            return
        if period.start_date != period.end_date:
            return

        start = (
            period.start_date.astimezone(self.zone)
            .replace(hour=0, minute=0, second=0, microsecond=0)
            .astimezone(php.UTC)
        )
        end = (
            period.end_date.astimezone(self.zone)
            .replace(hour=23, minute=59, second=0, microsecond=0)
            .astimezone(php.UTC)
        )
        period.start_date = period.start_time = start
        period.end_date = period.end_time = end

    def _set_period_days_and_times(
        self,
        period: Period,
        parameters: list[tuple[str, str]],
        properties: dict,
        reporter: Reporter,
    ) -> None:
        period.recurrence_type = EVERY_DAY
        value = find_parameter_value(parameters, "jours et horaires")

        if php.is_falsy(value) or php.in_array(value, ALL_HOURS_VALUES):  # type: ignore[arg-type]
            period.time_slots = []
            return

        period.time_slots = self.parse_time_slots(value, properties, reporter)  # type: ignore[arg-type]

    def parse_time_slots(self, value: str, properties: dict, reporter: Reporter) -> list[TimeSlot]:
        value = php.trim(value)
        match = HOURS_REGEX.match(value)

        if match:
            start_minute = match["startMinute"]
            end_minute = match["endMinute"]
            start = php.create_from_clock(
                match["startHour"],
                start_minute if not php.is_falsy(start_minute) else "00",
                self.zone,
                self.today,
            )
            end = php.create_from_clock(
                match["endHour"],
                end_minute if not php.is_falsy(end_minute) else "00",
                self.zone,
                self.today,
            )
            if start is None or end is None:
                raise php.PhpFatalError("Error", "Call to a member function setTimezone() on bool")
            return [
                TimeSlot(start_time=start.astimezone(php.UTC), end_time=end.astimezone(php.UTC))
            ]

        reporter.add_error(
            rec.ERROR_PERIOD_UNPARSABLE,
            {
                rec.ATTR_REGULATION_ID: php.key(properties, "arretesrcid"),
                rec.ATTR_URL: php.key(properties, "shorturl"),
                rec.ATTR_DETAILS: {
                    "idemprise": php.key(properties, "idemprise"),
                    "jours et horaires": value,
                },
            },
        )
        return []


def find_parameter_value(parameters: list[tuple[str, str]], wanted: str) -> str | None:
    """The value of the first parameter named `wanted`."""
    for name, value in parameters:
        if name == wanted:
            return value
    return None
