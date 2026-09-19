"""Métropole de Lyon integration.

Two Grand Lyon layers, each its own data source and identifier namespace:

- `chantiers_perturbants` (`MGL-CHP-`): disruptive work sites, temporary, one
  regulation per site;
- `chaussees_trottoirs` (`MGL-CT-`): the permanent regulation of the road network,
  one regulation per order (or per measure for the unnumbered stretches).

The work-site layer is a snapshot: a site that ends, or is withdrawn, disappears from
it. The regulation is then **closed, never deleted** — its end date is brought back to
the day before the run and it stays in DiaLog, because the team wants the history of
work sites kept. A site that ends on its announced date needs no write at all: the
regulation expired on its own, on the right day. The same policy applies to the road
network layer for want of a per-source one: a stretch that leaves it is closed, and
a batch of more than 50 updates or closures is held for review.

`PUBLISHED` is deliberate: a draft is not visible on staging, and this source is
being shown to the team there. It also means that flipping `co_lyon` to `target: prod`
in `.github/workflows/integrate.yml` publishes every regulation to production on the
next nightly run, with no ceiling on creations. Three questions are open and none of
them belongs to this file — settle them before flipping it:

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
from .chaussees_trottoirs.data_source_integration import (
    DataSourceIntegration as ChausseesTrottoirs,
)


class Integration(BaseIntegration):
    status = PostApiRegulationsAddBodyStatus.PUBLISHED

    # The footprints are polygons: DiaLog clips every street it intersects, down to
    # slivers of a few metres on neighbouring streets. Read the computed sections back
    # and republish only the ones ≥ 5 m (four calls per regulation, `zone_flow.py`).
    resolve_zones_to_sections = True

    # Synchronization policy: see the module docstring. Updates and closures stay
    # inside the `MGL-` prefix (`MGL-CHP-`, `MGL-CT-`): the organization also holds
    # ~800 regulations from another channel (`LYON_…`) that are not ours. The caps
    # hold a batch whole for review; a real day moves a handful of sites.
    identifier_prefix = "MGL-"
    update_changed = True
    close_missing = True
    max_updates_per_run = 50
    max_closures_per_run = 50

    data_sources = [ChantiersPerturbants, ChausseesTrottoirs]
