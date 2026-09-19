from api.dia_log_client.models import PostApiRegulationsAddBodyStatus
from integrations.base_integration import BaseIntegration

from .travaux_voirie.data_source_integration import (
    DataSourceIntegration as TravauxVoirie,
)


class Integration(BaseIntegration):
    status = PostApiRegulationsAddBodyStatus.PUBLISHED

    data_sources = [TravauxVoirie]
