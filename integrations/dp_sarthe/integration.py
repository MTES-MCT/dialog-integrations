from api.dia_log_client.models import PostApiRegulationsAddBodyStatus
from integrations.base_integration import BaseIntegration

from .chantiers_routiers.data_source_integration import DataSourceIntegration as ChantiersRoutiers
from .limitations_vitesse.data_source_integration import DataSourceIntegration as LimitationsVitesse
from .restrictions_gabarits.data_source_integration import (
    DataSourceIntegration as RestrictionGabarits,
)


class Integration(BaseIntegration):
    """Main integration class for Sarthe - coordinates multiple data sources."""

    status = PostApiRegulationsAddBodyStatus.PUBLISHED

    data_sources = [LimitationsVitesse, RestrictionGabarits, ChantiersRoutiers]
