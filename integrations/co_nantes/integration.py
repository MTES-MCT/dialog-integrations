from api.dia_log_client.models import PostApiRegulationsAddBodyStatus
from integrations.base_integration import BaseIntegration

from .circulation_chantier.data_source_integration import (
    DataSourceIntegration as CirculationChantier,
)


class Integration(BaseIntegration):
    status = PostApiRegulationsAddBodyStatus.PUBLISHED

    identifier_prefix = "NM-"
    update_changed = True
    close_missing = True
    max_updates_per_run = 50
    max_closures_per_run = 50

    data_sources = [CirculationChantier]
