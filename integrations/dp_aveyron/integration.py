from api.dia_log_client.models import PostApiRegulationsAddBodyStatus
from run.organization import BaseIntegration

from .limitations_vitesse.data_source_integration import DataSourceIntegration as LimitationsVitesse
from .restrictions_gabarits.data_source_integration import (
    DataSourceIntegration as RestrictionGabarits,
)


class Integration(BaseIntegration):
    status = PostApiRegulationsAddBodyStatus.PUBLISHED

    # What the organisation holds outside this prefix is not ours and is never touched.
    identifier_prefix = "AV-"

    # The source changes a few times a year (last edited 2026-02-09): more than a handful
    # of updates or deletions is held for review. Permanent regulations: deleted, not closed.
    update_changed = True
    delete_missing = True
    max_updates_per_run = 5
    max_deletions_per_run = 5

    data_sources = [
        RestrictionGabarits,
        LimitationsVitesse,
    ]
