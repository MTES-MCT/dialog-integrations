from api.dia_log_client.models import PostApiRegulationsAddBodyStatus
from integrations.base_integration import BaseIntegration

from .chaussees_trottoirs.data_source_integration import (
    DataSourceIntegration as ChausseesTrottoirs,
)


class Integration(BaseIntegration):
    """Main integration class for Métropole de Lyon.

    `DRAFT` on purpose. Publishing is **irreversible** — a published regulation never goes
    back to draft — and a published regulation is broadcast (DATEX II, CIFS). The batch is
    created as drafts, checked by the bizdev (P-10, step 6), and published once the
    producer agrees (step 8). The staging run of 2026-09-04 was made with `PUBLISHED`;
    that setting must not reach production.
    """

    status = PostApiRegulationsAddBodyStatus.DRAFT

    data_sources = [ChausseesTrottoirs]
