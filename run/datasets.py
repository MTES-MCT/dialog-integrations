"""The per-dataset lines of the Tchap report: "permanent", "temporaire", or a source's own."""

import polars as pl

from api.dia_log_client.models import PostApiRegulationsAddBody
from canonical.source import BaseDataSourceIntegration
from report.sync_report import SourceFunnel

CATEGORY_LABELS = {
    "permanentRegulation": "permanent",
    "temporaryRegulation": "temporaire",
}


def dataset_label(source: BaseDataSourceIntegration, clean_data: pl.DataFrame) -> str:
    """How the Tchap report names the dataset: "permanent" or "temporaire".

    Read from the regulations the source produced. A source may set `report_label`
    instead, to be reported apart from the others of its kind. When the data says
    nothing (no rows, or both kinds), the source keeps its own name.
    """
    label = getattr(source, "report_label", None)
    if isinstance(label, str) and label:
        return label
    categories: set[str] = set()
    if "regulation_category" in clean_data.columns:
        categories = set(clean_data["regulation_category"].drop_nulls().unique().to_list())
    name = source.name or type(source).__name__
    if len(categories) == 1:
        return CATEGORY_LABELS.get(next(iter(categories)), name)
    return name


def summaries(
    funnels: list[SourceFunnel],
    regulations: dict[str, PostApiRegulationsAddBody],
    source_of: dict[str, str],
    excluded: set[str],
) -> list[dict]:
    """One entry per label for the Tchap report, sources sharing a label together.

    `excluded` holds the identifiers not in DiaLog after the run (held, refused or
    failed creations): they count as produced, not as integrated.
    """
    by_label: dict[str, dict] = {}
    for funnel in funnels:
        entry = by_label.setdefault(
            funnel.label,
            {
                "label": funnel.label,
                "sources": [],
                "regulations": 0,
                "measures": 0,
                "restrictions": 0,
                "retained": 0,
            },
        )
        entry["sources"].append(funnel.name)
        # A source that could not be counted leaves its group without a rate.
        for key, value in (
            ("restrictions", funnel.restrictions),
            ("retained", funnel.retained),
        ):
            entry[key] = None if value is None or entry[key] is None else entry[key] + value

    label_of = {funnel.name: funnel.label for funnel in funnels}
    for identifier, regulation in regulations.items():
        if identifier in excluded:
            continue
        entry = by_label[label_of[source_of[identifier]]]
        entry["regulations"] += 1
        entry["measures"] += len(regulation.measures or [])
    return list(by_label.values())
