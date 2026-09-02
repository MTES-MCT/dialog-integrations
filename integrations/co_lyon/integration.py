"""Métropole de Lyon integration.

Scope agreed with the team: **disruptive work sites only**. The other Grand Lyon
layers explored in June/August 2026 (chaussées et trottoirs, zones apaisées…) are out
of scope for this integration.

The source is a snapshot: a work site that ends disappears from the layer. This
pipeline has no deletion pass, so a regulation stays published after its site is
gone. That is bearable here, and only here, because every row carries a real end
date and the producer never withdraws a site before it — the regulation expires on
its own, on the right day. What accumulates is expired regulations, not live
restrictions on roads that reopened.

`PUBLISHED` is deliberate: a draft is not visible on staging, and this source is
being shown to the team there. It also means that adding `co_lyon` to the matrix in
`.github/workflows/integrate.yml` publishes ~185 regulations to production on the
next nightly run, with no volume ceiling and no dry run. Three questions are open
and none of them belongs to this file — settle them before adding that line:

- the footprints are polygons, not centrelines. Roughly a third of them cover more
  than one street (up to 31), and a `noEntry` on such a polygone closes every street
  it touches. Does the polygon outline the works or the area they disrupt?
- `validite` reads "A vérifier" on 100 % of rows: the producer states the data is
  unvalidated, and we would be redistributing it to satnavs;
- nothing here deletes, so expired regulations pile up. Whether DiaLog filters them
  out when it redistributes is a question for the back end, and it decides whether
  that pile is noise or merely volume.

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

    data_sources = [ChantiersPerturbants]
