from api.dia_log_client.models import PostApiRegulationsAddBodyStatus
from run.organization import BaseIntegration

from .chantiers_routiers.data_source_integration import DataSourceIntegration as ChantiersRoutiers
from .limitations_vitesse.data_source_integration import DataSourceIntegration as LimitationsVitesse
from .restrictions_gabarits.data_source_integration import (
    DataSourceIntegration as RestrictionGabarits,
)


class Integration(BaseIntegration):
    status = PostApiRegulationsAddBodyStatus.PUBLISHED
    # Legacy integration kept as is by decision: it never updates nor removes.
    legacy_additive = True

    data_sources = [LimitationsVitesse, RestrictionGabarits, ChantiersRoutiers]
