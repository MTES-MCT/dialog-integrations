from api.dia_log_client.models import PostApiRegulationsAddBodyStatus
from integrations.base_integration import BaseIntegration

from .eudonet.data_source_integration import IDENTIFIER_PREFIX
from .eudonet.data_source_integration import DataSourceIntegration as Eudonet


class Integration(BaseIntegration):
    status = PostApiRegulationsAddBodyStatus.PUBLISHED

    data_sources = [Eudonet]

    identifier_prefix = IDENTIFIER_PREFIX
    update_changed = True
    close_missing = True

    max_closures_per_run = 50
    max_updates_per_run = 300
    # Not armed yet: the initial load is ~2 000 regulations and a cap would hold it
    # whole. Arm it at 500 once the initial corpus is in place.
    max_creations_per_run = None
