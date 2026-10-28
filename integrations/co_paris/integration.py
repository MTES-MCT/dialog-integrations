from api.dia_log_client.models import PostApiRegulationsAddBodyStatus
from run.organization import BaseIntegration

from .eudonet.data_source_integration import DataSourceIntegration as Eudonet
from .eudonet.regulations import IDENTIFIER_PREFIX


class Integration(BaseIntegration):
    status = PostApiRegulationsAddBodyStatus.PUBLISHED

    data_sources = [Eudonet]

    identifier_prefix = IDENTIFIER_PREFIX
    update_changed = True
    close_missing = True

    max_closures_per_run = 50
    max_updates_per_run = 300
    max_creations_per_run = None
