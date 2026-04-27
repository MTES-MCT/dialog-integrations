from api.dia_log_client.models import PostApiRegulationsAddBodyStatus
from integrations.base_integration import BaseIntegration

from .circulation_interdite.data_source_integration import (
    DataSourceIntegration as CirculationInterdite,
)


class Integration(BaseIntegration):
    """Main integration class for Aveyron - coordinates multiple data sources."""

    status = PostApiRegulationsAddBodyStatus.PUBLISHED

    # data_sources = [TravauxVoirie]#, CirculationInterdite]
    data_sources = [CirculationInterdite]  # , ]
