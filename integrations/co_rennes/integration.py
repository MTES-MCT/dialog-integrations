from api.dia_log_client.models import PostApiRegulationsAddBodyStatus
from integrations.base_integration import BaseIntegration

from .circulation_interdite.data_source_integration import (
    DataSourceIntegration as CirculationInterdite,
)
from .travaux_voirie.data_source_integration import DataSourceIntegration as TravauxVoirie


class Integration(BaseIntegration):
    """Main integration class for Rennes - coordinates multiple data sources."""

    status = PostApiRegulationsAddBodyStatus.DRAFT

    data_sources = [TravauxVoirie, CirculationInterdite]
