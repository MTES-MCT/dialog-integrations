from api.dia_log_client.models import PostApiRegulationsAddBodyStatus
from integrations.base_integration import BaseIntegration

from .restrictions_gabarits.data_source_integration import (
    DataSourceIntegration as RestrictionGabarits,
)


class Integration(BaseIntegration):
    """Main integration class for Aveyron - coordinates multiple data sources."""

    status = PostApiRegulationsAddBodyStatus.DRAFT

    data_sources = [RestrictionGabarits]
