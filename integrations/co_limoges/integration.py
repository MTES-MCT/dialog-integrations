from api.dia_log_client.models import PostApiRegulationsAddBodyStatus
from run.organization import BaseIntegration

from .limitation_tonnage.data_source_integration import (
    DataSourceIntegration as LimitationTonnage,
)


class Integration(BaseIntegration):
    status = PostApiRegulationsAddBodyStatus.PUBLISHED

    identifier_prefix = "LIMOGES-"

    # Permanent limits, arretes from 1999 to 2025 (read on 2026-10-28).
    update_changed = True
    delete_missing = True
    max_updates_per_run = 5
    max_deletions_per_run = 5

    data_sources = [LimitationTonnage]
