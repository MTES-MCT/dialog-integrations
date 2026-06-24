from api.dia_log_client.models import PostApiRegulationsAddBodyStatus
from integrations.base_integration import BaseIntegration

from .chantiers_routiers.data_source_integration import DataSourceIntegration as ChantiersRoutiers


class Integration(BaseIntegration):
    """Main integration class for Dijon - coordinates multiple data sources."""

    status = PostApiRegulationsAddBodyStatus.DRAFT

    data_sources = [ChantiersRoutiers]
