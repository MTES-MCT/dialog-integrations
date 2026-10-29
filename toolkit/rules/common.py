"""The rules every source applies, unless it disables one with its reason."""

from toolkit.rules.location import ONE_DIRECTION, POINT_NOT_PARKING
from toolkit.rules.vehicles import HEAVYWEIGHT_ABOVE_LIMIT, THRESHOLD_MISSING

COMMON_RULES = (POINT_NOT_PARKING, ONE_DIRECTION, HEAVYWEIGHT_ABOVE_LIMIT, THRESHOLD_MISSING)
