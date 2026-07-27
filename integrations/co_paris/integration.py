from api.dia_log_client.models import PostApiRegulationsAddBodyStatus
from integrations.base_integration import BaseIntegration
from integrations.co_paris.permanents.data_source_integration import (
    ParisEudonetDataSourceIntegration as Permanents,
)


class Integration(BaseIntegration):
    """Main integration class for Paris."""

    status = PostApiRegulationsAddBodyStatus.DRAFT

    data_sources = [
        # Temporaires,
        Permanents,
    ]
