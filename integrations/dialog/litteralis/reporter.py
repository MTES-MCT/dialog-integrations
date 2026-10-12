"""The back end's integration `Reporter`: records, and the "new errors" flag that
decides whether a regulation is dropped.

Record names and contexts are the PHP ones, so the two implementations can be
compared record by record. Each record is also logged, at the level PHP uses.
"""

from typing import Any

from loguru import logger

ERROR = "error"
WARNING = "warning"
NOTICE = "notice"
COUNT = "count"
FACT = "fact"

ATTR_REGULATION_ID = "regulationId"
ATTR_URL = "url"
ATTR_DETAILS = "details"

COUNT_TOTAL_FEATURES = "litteralis.total_features"
COUNT_MATCHING_FEATURES = "litteralis.matching_features"
COUNT_EXTRACTED_FEATURES = "litteralis.extracted_features"
COUNT_IMPORTED_FEATURES = "litteralis.imported_features"

ERROR_MEASURE_PARAMETER_MALFORMED = "litteralis.measure_parameter_malformed"
ERROR_MEASURE_PARAMETER_INCONSISTENT_NUMBER = "litteralis.measure_parameter_inconsistent_number"
ERROR_MAX_SPEED_VALUE_INVALID = "litteralis.max_speed_value_invalid"
ERROR_MAX_SPEED_VALUE_MISSING = "litteralis.max_speed_value_missing"
ERROR_DATE_PARSING_FAILED = "litteralis.date_parsing_failed"
ERROR_PERIOD_UNPARSABLE = "litteralis.period_unparsable"
ERROR_IMPORT_COMMAND_FAILED = "litteralis.import_command_failed"

WARNING_MISSING_GEOMETRY = "litteralis.missing_geometry"

NOTICE_UNSUPPORTED_MEASURE = "litteralis.unsupported_measure"
NOTICE_NO_MEASURES_FOUND = "litteralis.no_measures_found"
NOTICE_POLYGON_LOCATION_EXCLUDED = "litteralis.polygon_location_excluded"
NOTICE_MISSING_GEOMETRY_EXCLUDED = "litteralis.missing_geometry_excluded"

_LOG_LEVELS = {ERROR: "ERROR", WARNING: "WARNING", NOTICE: "DEBUG", COUNT: "INFO", FACT: "INFO"}


class Reporter:
    def __init__(self) -> None:
        self.records: list[tuple[str, dict[str, Any]]] = []
        self._new_errors = False

    def _push(self, record_type: str, name: str, context: dict[str, Any]) -> None:
        self.records.append((record_type, {record_type: name, **context}))
        logger.log(_LOG_LEVELS[record_type], f"{name} {context}")

    def has_new_errors(self) -> bool:
        """Whether `add_error` was called since the last `acknowledge_new_errors`."""
        return self._new_errors

    def acknowledge_new_errors(self) -> None:
        self._new_errors = False

    def add_error(self, name: str, context: dict[str, Any]) -> None:
        self._push(ERROR, name, context)
        self._new_errors = True

    def add_warning(self, name: str, context: dict[str, Any]) -> None:
        self._push(WARNING, name, context)

    def add_notice(self, name: str, context: dict[str, Any]) -> None:
        self._push(NOTICE, name, context)

    def add_count(self, name: str, value: float, context: dict[str, Any] | None = None) -> None:
        # PHP types the value as float: an int count is widened.
        self._push(COUNT, name, {"value": float(value), **(context or {})})

    def count_of(self, name: str) -> int:
        return sum(1 for record_type, context in self.records if context.get(record_type) == name)
