from api.dia_log_client.models import PostApiRegulationsAddBodyStatus
from integrations.base_integration import BaseIntegration

from .travaux_voirie.data_source_integration import (
    DataSourceIntegration as TravauxVoirie,
)


class Integration(BaseIntegration):
    """Main integration class for Angers - coordinates multiple data sources."""

    status = PostApiRegulationsAddBodyStatus.DRAFT

    data_sources = [TravauxVoirie]
    
