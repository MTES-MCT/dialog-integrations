from api.dia_log_client.models import PostApiRegulationsAddBodyStatus
from integrations.base_integration import BaseIntegration

from .limitations_vitesse.data_source_integration import DataSourceIntegration


class Integration(BaseIntegration):
    """Main integration class for Sarthes - coordinates multiple data sources."""

    status = PostApiRegulationsAddBodyStatus.DRAFT

    data_sources = [DataSourceIntegration]
