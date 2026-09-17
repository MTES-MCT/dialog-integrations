from api.dia_log_client.models import PostApiRegulationsAddBodyStatus
from integrations.base_integration import BaseIntegration

from .limitations_vitesse.data_source_integration import DataSourceIntegration as LimitationsVitesse
from .restrictions_gabarits.data_source_integration import (
    DataSourceIntegration as RestrictionGabarits,
)


class Integration(BaseIntegration):
    """Main integration class for Aveyron - coordinates multiple data sources."""

    status = PostApiRegulationsAddBodyStatus.PUBLISHED

    # Every identifier we create starts with `AV-` (`AV-GB-`, `AV-LV-`): what the
    # organisation holds outside it is not ours and is never touched.
    identifier_prefix = "AV-"

    # Synchronization in report-only mode: the diff against the last send is computed
    # and reported every night, but nothing is applied — a cap of 0 holds every batch
    # for manual review. Creations are not capped. Both sources are permanent
    # regulations, so what leaves them is a deletion, not a closure.
    update_changed = True
    delete_missing = True
    max_updates_per_run = 0
    max_deletions_per_run = 0

    data_sources = [
        RestrictionGabarits,
        LimitationsVitesse,
    ]
