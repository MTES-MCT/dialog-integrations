from api.dia_log_client.models import PostApiRegulationsAddBodyStatus
from integrations.base_integration import BaseIntegration

from .circulation_chantier.data_source_integration import (
    DataSourceIntegration as CirculationChantier,
)

class Integration(BaseIntegration):
    """Main integration class for Nantes."""

    status = PostApiRegulationsAddBodyStatus.PUBLISHED

    data_sources = [CirculationChantier]  # , ]
