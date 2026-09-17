from api.dia_log_client.models import PostApiRegulationsAddBodyStatus
from integrations.base_integration import BaseIntegration

from .eudonet.data_source_integration import DataSourceIntegration as Eudonet


class Integration(BaseIntegration):
    """Main integration class for the City of Paris (Eudonet)."""

    # Draft until the mapping has been reviewed with Paris.
    status = PostApiRegulationsAddBodyStatus.DRAFT

    data_sources = [Eudonet]

    # --- Synchronization (plan §8, journal of 2026-09-08) -------------------------
    # The source is alive: daily creations, amending decrees, nightly expiry,
    # repeals. All three operations are needed.
    #
    # The prefix bounds every deletion. The DiaLog organization "Paris" also holds 25
    # regulations pushed by the former PHP channel, whose identifiers are the bare
    # `1101` numbers; "PARIS-EUDO-" overlaps neither those nor the "Paris_" ones of the
    # prefecture.
    identifier_prefix = "PARIS-EUDO-"
    delete_missing = True
    update_changed = True

    max_deletions_per_run = 50
    max_updates_per_run = 300
    # Not armed yet: the initial load is ~2 800 regulations and a cap would hold it
    # whole. Arm it at 500 once the initial corpus is in place (plan §8).
    max_creations_per_run = None
