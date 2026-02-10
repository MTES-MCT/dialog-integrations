"""Brest integration module."""

from api.dia_log_client.models import PostApiRegulationsAddBodyStatus
from integrations.base_integration import BaseIntegration as BaseIntegration
from integrations.co_brest.permanent_lineaire.data_source_integration import DataSourceIntegration


class Integration(BaseIntegration):
    """Main integration class for Brest - coordinates multiple data sources."""

    status = PostApiRegulationsAddBodyStatus.PUBLISHED
    data_sources = [DataSourceIntegration]
