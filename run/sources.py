"""Run every data source of an organization, as far as its payloads.

A source that fails or is paused is frozen and the others carry on; what each one
produced is gathered in a `Production` for the rest of the night.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

import polars as pl
from loguru import logger

from api.dia_log_client.models import PostApiRegulationsAddBody
from canonical.source import BaseDataSourceIntegration
from checks import rows as checks
from report.sync_report import SourceFunnel
from run.datasets import dataset_label
from sync.digest import Digest, compute_regulation_digest
from sync.outcome import FAILED, PAUSED, IntegrationOutcome
from sync.plan import assert_identifiers_in_prefix
from sync.snapshot import SnapshotStore

if TYPE_CHECKING:
    from run.organization import BaseIntegration


@dataclass
class Production:
    regulations: dict[str, PostApiRegulationsAddBody] = field(default_factory=dict)
    digests: dict[str, Digest] = field(default_factory=dict)
    source_of: dict[str, str] = field(default_factory=dict)
    funnels: list[SourceFunnel] = field(default_factory=list)
    stores: dict[str, SnapshotStore] = field(default_factory=dict)
    unbuilt: set[str] = field(default_factory=set)
    # Left out by the common checks; also in `unbuilt`, but counted under their reason.
    checked_out: set[str] = field(default_factory=set)
    # Built or not, by source: what the volume guard compares with the previous run.
    produced_by_source: dict[str, int] = field(default_factory=dict)
    prefix_of: dict[str, str | None] = field(default_factory=dict)


def run_all(integration: BaseIntegration, outcome: IntegrationOutcome, limit_to) -> Production:
    production = Production()
    whole_organization = not limit_to

    for data_source in integration.data_sources:
        name = data_source.name or data_source.__name__
        production.stores[name] = SnapshotStore(
            integration.organization, name, target=integration.target
        )
        production.prefix_of[name] = data_source.identifier_prefix or integration.identifier_prefix
        if name in integration.paused_sources:
            logger.warning(f"Data source {name} is paused: not read, nothing in it removed")
            outcome.frozen_sources[name] = PAUSED
            continue
        logger.info(f"Processing data source: {name}")
        try:
            source = data_source(integration.organization_settings, integration.client)
            clean_data, raw_rows = compute_clean_data(source)
        except Exception as error:
            # One source failing must not cost the others their run. What it held in
            # DiaLog stays as it is: an exception says nothing about what left it.
            logger.exception(f"Data source {name} failed, its regulations are left as is")
            outcome.frozen_sources[name] = FAILED
            outcome.source_errors[name] = repr(error)[:300]
            continue

        if not whole_organization:
            logger.info(f"Limiting processing to following ids : {limit_to}")
            clean_data = clean_data.filter(pl.col("regulation_identifier").is_in(limit_to))

        logger.info(f"Total records from source {name}: {clean_data.shape[0]}")
        unbuilt_before = len(production.unbuilt)
        clean_data = apply_checks(name, clean_data, production.checked_out, outcome)
        production.unbuilt |= production.checked_out

        source_regulations = integration.create_regulations(clean_data, source, production.unbuilt)
        production.produced_by_source[name] = (
            len(source_regulations) + len(production.unbuilt) - unbuilt_before
        )
        if data_source.identifier_prefix:
            assert_identifiers_in_prefix(
                (str(r.identifier) for r in source_regulations), data_source.identifier_prefix
            )
        for regulation in source_regulations:
            regulation.identifier = f"{regulation.identifier}"
            regulation.status = integration.status
        num_measures = sum([len(regulation.measures or []) for regulation in source_regulations])
        logger.info(f"Created {len(source_regulations)} regulations with a total of {num_measures}")

        metrics = source_metrics(source)
        outcome.source.update(metrics)
        production.funnels.append(
            SourceFunnel(
                name=name,
                raw_rows=raw_rows,
                restrictions=(
                    source.dataset_restrictions
                    if source.dataset_restrictions is not None
                    else raw_rows
                ),
                retained=(
                    source.retained_restrictions
                    if source.retained_restrictions is not None
                    else clean_data.shape[0]
                ),
                label=dataset_label(source, clean_data),
                clean_rows=clean_data.shape[0],
                regulations=len(source_regulations),
                measures=num_measures,
                metrics=metrics,
            )
        )

        for regulation in source_regulations:
            identifier = str(regulation.identifier)
            if identifier in production.regulations:
                logger.warning(
                    f"Identifier {identifier} is produced by two data sources; "
                    f"keeping the one from {name}"
                )
            production.regulations[identifier] = regulation
            production.digests[identifier] = compute_regulation_digest(regulation)
            production.source_of[identifier] = name

    return production


def apply_checks(
    name: str, clean_data: pl.DataFrame, checked_out: set[str], outcome: IntegrationOutcome
) -> pl.DataFrame:
    """Leave out, whole, every regulation with a row failing a common check.

    They go into `checked_out`, then into `unbuilt`: neither sent nor taken for gone.
    Counted with what the pipeline leaves out; the reasons are in the log only.
    """
    failing = checks.failing_rows(clean_data)
    if failing.is_empty():
        return clean_data
    for (reason,), group in failing.group_by("reason", maintain_order=True):
        identifiers = group["regulation_identifier"].unique().to_list()
        logger.warning(
            f"{name}: {len(identifiers)} regulations left out ({reason}), left as "
            f"DiaLog has them: {sorted(identifiers)[:5]}"
        )
    left_out = set(failing["regulation_identifier"].to_list())
    checked_out |= left_out
    outcome.refused += len(left_out)
    return clean_data.filter(~pl.col("regulation_identifier").cast(pl.Utf8).is_in(list(left_out)))


def compute_clean_data(source: BaseDataSourceIntegration) -> tuple[pl.DataFrame, int | None]:
    """Run the source pipeline, counting the raw rows it read on the way.

    The count is taken by wrapping the instance's `fetch_raw_data`, so a source that
    overrides `compute_data_regulations` is measured just the same, and no data
    source has to know about the report.
    """
    raw_rows: list[int] = []
    original_fetch = source.fetch_raw_data

    def counting_fetch(*args, **kwargs):
        raw = original_fetch(*args, **kwargs)
        raw_rows.append(int(getattr(raw, "height", 0)))
        return raw

    setattr(source, "fetch_raw_data", counting_fetch)
    try:
        clean_data = source.compute_data_regulations()
    finally:
        # The wrapper only ever lived on the instance; the class is left alone.
        source.__dict__.pop("fetch_raw_data", None)

    return clean_data, sum(raw_rows) if raw_rows else None


def source_metrics(source: BaseDataSourceIntegration) -> dict[str, int]:
    """Read the optional `metrics` a data source may expose.

    A source that measures its own volumes sets `self.metrics: dict[str, int]` while
    fetching. Keys are the French labels shown in the Tchap message. Sources that
    define nothing are unaffected.
    """
    metrics = getattr(source, "metrics", None)
    if not isinstance(metrics, dict):
        return {}
    return {
        str(name): int(value)
        for name, value in metrics.items()
        if isinstance(value, (int, float)) and not isinstance(value, bool)
    }
