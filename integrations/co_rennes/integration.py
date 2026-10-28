from api.dia_log_client.models import PostApiRegulationsAddBodyStatus
from run.organization import BaseIntegration

from .circulation_interdite.data_source_integration import (
    DataSourceIntegration as CirculationInterdite,
)
from .travaux_voirie.data_source_integration import DataSourceIntegration as TravauxVoirie


class Integration(BaseIntegration):
    status = PostApiRegulationsAddBodyStatus.PUBLISHED
    # Legacy integration kept as is by decision: it never updates nor removes.
    legacy_additive = True

    data_sources = [TravauxVoirie, CirculationInterdite]
