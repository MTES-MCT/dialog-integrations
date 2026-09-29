from api.dia_log_client.models import PostApiRegulationsAddBodyStatus
from integrations.base_integration import BaseIntegration

from .limitations_vitesse.data_source_integration import DataSourceIntegration as LimitationsVitesse
from .restrictions_gabarits.data_source_integration import (
    DataSourceIntegration as RestrictionGabarits,
)


class Integration(BaseIntegration):
    status = PostApiRegulationsAddBodyStatus.PUBLISHED

    # Every identifier we create starts with `AV-` (`AV-GB-`, `AV-LV-`): what the
    # organisation holds outside it is not ours and is never touched.
    identifier_prefix = "AV-"

    # The source changes a few times a year (last edited 2026-02-09): a handful of
    # updates or deletions goes through, more is held for manual review. Creations are
    # not capped. Both sources are permanent regulations, so what leaves them is a
    # deletion, not a closure.
    update_changed = True
    delete_missing = True
    max_updates_per_run = 5
    max_deletions_per_run = 5

    data_sources = [
        RestrictionGabarits,
        LimitationsVitesse,
    ]
