from api.dia_log_client.models import PostApiRegulationsAddBodyStatus
from integrations.co_brest.permanent_lineaire.data_source_integration import DataSourceIntegration
from run.organization import BaseIntegration as BaseIntegration


class Integration(BaseIntegration):
    status = PostApiRegulationsAddBodyStatus.PUBLISHED
    # Legacy integration kept as is by decision: it never updates nor removes.
    legacy_additive = True
    data_sources = [DataSourceIntegration]
