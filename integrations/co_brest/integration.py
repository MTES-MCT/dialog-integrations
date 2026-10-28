"""Brest integration module."""

from api.dia_log_client.models import PostApiRegulationsAddBodyStatus
from integrations.base_integration import BaseIntegration as BaseIntegration
from integrations.co_brest.permanent_lineaire.data_source_integration import DataSourceIntegration


class Integration(BaseIntegration):
    status = PostApiRegulationsAddBodyStatus.PUBLISHED
    # Legacy integration kept as is by decision: it never updates nor removes.
    legacy_additive = True
    data_sources = [DataSourceIntegration]
