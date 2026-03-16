from api.dia_log_client.models import PostApiRegulationsAddBodyStatus
from integrations.base_integration import BaseIntegration

from .restrictions_gabarits.data_source_integration import (
    DataSourceIntegration as RestrictionGabarits,
)

from .limitations_vitesse.data_source_integration import (
    DataSourceIntegration as LimitationsVitesse
)


class Integration(BaseIntegration):
    """Main integration class for Aveyron - coordinates multiple data sources."""

    status = PostApiRegulationsAddBodyStatus.PUBLISHED

    # data_sources = [RestrictionGabarits]
    data_sources = [LimitationsVitesse]
