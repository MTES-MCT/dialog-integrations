"""Métropole de Lyon: two Grand Lyon layers, each with its own identifier prefix.

`chantiers_perturbants` (`MGL-CHP-`) is a snapshot of temporary work sites;
`chaussees_trottoirs` (`MGL-CT-`) the permanent regulation of the road network. A
regulation that leaves either is closed, never deleted: the team wants the history kept.

With `PUBLISHED`, flipping `co_lyon` to `target: prod` in `.github/workflows/integrate.yml`
publishes everything on the next nightly run, with no cap on creations. Open before that:
do the footprints outline the works or the area they disrupt (a `noEntry` zone closes
every street inside, up to 31)? `validite` reads "A vérifier" on 100 % of rows. Does
DiaLog filter out the expired regulations that pile up as history?
"""

from api.dia_log_client.models import PostApiRegulationsAddBodyStatus
from run.organization import BaseIntegration

from .chantiers_perturbants.data_source_integration import (
    DataSourceIntegration as ChantiersPerturbants,
)
from .chaussees_trottoirs.data_source_integration import (
    DataSourceIntegration as ChausseesTrottoirs,
)


class Integration(BaseIntegration):
    status = PostApiRegulationsAddBodyStatus.PUBLISHED

    # DiaLog clips every street a polygon touches, down to slivers of a few metres: only the
    # computed sections ≥ 5 m are republished (four calls per regulation).
    resolve_zones_to_sections = True

    # The organization also holds ~800 `LYON_…` regulations from another channel. A real
    # day moves a handful of sites, so a larger batch is held whole for review.
    identifier_prefix = "MGL-"
    update_changed = True
    close_missing = True
    max_updates_per_run = 50
    max_closures_per_run = 50

    data_sources = [ChantiersPerturbants, ChausseesTrottoirs]
