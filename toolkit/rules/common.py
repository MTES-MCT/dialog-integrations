"""The rules every source applies."""

from toolkit.rules.location import (
    COORDINATES_OUT_OF_BOUNDS,
    GEOMETRY_MISSING,
    ONE_DIRECTION,
    POINT_NOT_PARKING,
)
from toolkit.rules.measure import SPEED_MISSING
from toolkit.rules.period import DATES_MISSING, END_BEFORE_START, EXPIRED, LONG_DURATION
from toolkit.rules.vehicles import HEAVYWEIGHT_ABOVE_LIMIT, THRESHOLD_MISSING

# Unless a source disables one, with its reason.
COMMON_RULES = (
    EXPIRED,
    DATES_MISSING,
    LONG_DURATION,
    POINT_NOT_PARKING,
    ONE_DIRECTION,
    HEAVYWEIGHT_ABOVE_LIMIT,
    THRESHOLD_MISSING,
)

# The canonical model's contract: applied to every source's final rows, after all its own
# work, and never disabled. No source has a reason to send such a row.
CONTRACT_RULES = (END_BEFORE_START, SPEED_MISSING, GEOMETRY_MISSING, COORDINATES_OUT_OF_BOUNDS)
