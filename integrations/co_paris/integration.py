from api.dia_log_client.models import PostApiRegulationsAddBodyStatus
from integrations.base_integration import BaseIntegration

from .eudonet.data_source_integration import DataSourceIntegration as Eudonet


class Integration(BaseIntegration):
    """Main integration class for the City of Paris (Eudonet)."""

    status = PostApiRegulationsAddBodyStatus.PUBLISHED

    data_sources = [Eudonet]

    # Synchronization. The source is alive: daily creations, amending decrees, nightly
    # expiry, repeals. A regulation that leaves the perimeter (repealed, expired, no longer
    # signed) is closed, never deleted, as for Lyon.
    #
    # The prefix bounds every update and closure. The DiaLog organization "Paris" also
    # holds regulations pushed by the former PHP channel, whose identifiers are the bare
    # `1101` numbers; "PARIS-EUDO-" overlaps neither those nor the "Paris_" ones of the
    # prefecture.
    identifier_prefix = "PARIS-EUDO-"
    update_changed = True
    close_missing = True

    max_closures_per_run = 50
    max_updates_per_run = 300
    # Not armed yet: the initial load is ~2 000 regulations and a cap would hold it
    # whole. Arm it at 500 once the initial corpus is in place (plan §8).
    max_creations_per_run = None
