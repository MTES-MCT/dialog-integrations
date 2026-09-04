from api.dia_log_client.models import PostApiRegulationsAddBodyStatus
from integrations.base_integration import BaseIntegration

from .chaussees_trottoirs.data_source_integration import (
    DataSourceIntegration as ChausseesTrottoirs,
)


class Integration(BaseIntegration):
    """Main integration class for Métropole de Lyon.

    ⚠️ `PUBLISHED` is set on purpose for the staging run of 2026-09-04. Publishing is
    **irreversible** — a published regulation never goes back to draft — and a published
    regulation is broadcast (DATEX II, CIFS). Switch back to `DRAFT` before pointing this
    integration at production.
    """

    status = PostApiRegulationsAddBodyStatus.PUBLISHED

    data_sources = [ChausseesTrottoirs]
