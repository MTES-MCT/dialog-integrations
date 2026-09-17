"""Métropole de Lyon integration.

Scope agreed with the team: **disruptive work sites only**. The other Grand Lyon
layers explored in June/August 2026 (chaussées et trottoirs, zones apaisées…) are out
of scope for this integration.

The source is a snapshot: a work site that ends, or is withdrawn, disappears from
the layer. The regulation is then **closed, never deleted** — its end date is brought
back to the day before the run and it stays in DiaLog, because the team wants the
history of work sites kept. A site that ends on its announced date needs no write at
all: the regulation expired on its own, on the right day.

`PUBLISHED` is deliberate: a draft is not visible on staging, and this source is
being shown to the team there. It also means that adding `co_lyon` to the matrix in
`.github/workflows/integrate.yml` publishes ~185 regulations to production on the
next nightly run, with no volume ceiling and no dry run. Three questions are open
and none of them belongs to this file — settle them before adding that line:

- the footprints are polygons, not centrelines. They are sent as DiaLog `zone`s, so
  DiaLog itself turns each polygon into the street segments it covers. Roughly a
  third of the footprints cover more than one street (up to 31), and a `noEntry` on
  such a zone closes every street inside it. Does the polygon outline the works or
  the area they disrupt?
- `validite` reads "A vérifier" on 100 % of rows: the producer states the data is
  unvalidated, and we would be redistributing it to satnavs;
- nothing here deletes, so expired regulations pile up — by design, as history.
  Whether DiaLog filters them out when it redistributes is a question for the back
  end, and it decides whether that pile is noise or merely volume.

Measurements behind all three: `explorations/co_lyon/`.
"""

from api.dia_log_client.models import PostApiRegulationsAddBodyStatus
from integrations.base_integration import BaseIntegration

from .chantiers_perturbants.data_source_integration import (
    DataSourceIntegration as ChantiersPerturbants,
)


class Integration(BaseIntegration):
    """Main integration class for the Métropole de Lyon."""

    status = PostApiRegulationsAddBodyStatus.PUBLISHED

    # The footprints are polygons: DiaLog clips every street it intersects, down to
    # slivers of a few metres on neighbouring streets. Read the computed sections back
    # and republish only the ones ≥ 5 m (four calls per regulation, see
    # `BaseIntegration._add_zone_regulation_as_sections`).
    resolve_zones_to_sections = True

    # Synchronization: the layer is a snapshot of live work sites, so a site that ends
    # or is withdrawn leaves it, and one that is extended changes its end date. A site
    # that left the layer is closed (end date brought back to yesterday), not deleted:
    # the history of work sites is kept. Every write outside today's production stays
    # inside the `MGL-CHP-` prefix: the organization also holds ~800 regulations from
    # another channel (`LYON_…`) that are not ours. The caps hold a batch whole for
    # review; a real day moves a handful of sites.
    identifier_prefix = "MGL-CHP-"
    update_changed = True
    close_missing = True
    max_updates_per_run = 50
    max_closures_per_run = 50

    data_sources = [ChantiersPerturbants]
