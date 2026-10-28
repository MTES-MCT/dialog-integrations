"""What a run did, for the report, the CLI and Tchap."""

from __future__ import annotations

from dataclasses import dataclass, field

# Why a source is frozen for the run: nothing under its prefix is closed or deleted.
FAILED = "failed"


PAUSED = "paused"


VOLUME = "volume"


# The pipeline does not redo the API's checks, so a few refusals are expected every
# night (2026-09-24: 2 at Brest, 2 in Sarthe, 1 at Rennes). Flagged beyond the larger
# of these; chosen on 2026-09-29.
REJECTION_ALERT_FLOOR = 10


REJECTION_ALERT_SHARE = 0.05


@dataclass
class IntegrationOutcome:
    """What one `dialog integrate` run did, and what it plans to do."""

    organization: str
    dry_run: bool = False
    created: int = 0
    updated: int = 0
    deleted: int = 0
    # None for organizations that do not close what left their source.
    closed: int | None = None
    # Outages do not fail the run (D-05): the CI marks a run failed only when the
    # command itself exits non-zero.
    error_causes: dict[str, int] = field(default_factory=dict)
    rejections: int = 0
    # Regulations the pipeline itself left out (a zone over parallel roads, a failed check,
    # a measure it cannot build): not an API failure, retried every day.
    refused: int = 0
    planned: dict[str, int] = field(default_factory=dict)
    held: dict[str, int] = field(default_factory=dict)
    source: dict[str, int] = field(default_factory=dict)
    # The corpus, for the Tchap message: rows read from the sources, and the
    # regulations and measures that are in DiaLog after the run — what was produced,
    # minus what could not be created (held, refused, or failed).
    raw_rows: int | None = None
    # One entry per dataset ("permanent", "temporaire"): the regulations and measures
    # in DiaLog after the run, and the restrictions the dataset states versus those the
    # pipeline retained, whose ratio is the share of the dataset retained.
    datasets: list[dict] = field(default_factory=list)
    regulations: int = 0
    measures: int = 0
    produced: int = 0
    total_shift: dict | None = None
    frozen_sources: dict[str, str] = field(default_factory=dict)
    volume_drops: dict[str, list[int]] = field(default_factory=dict)
    source_errors: dict[str, str] = field(default_factory=dict)
    report: str = ""

    @property
    def errors(self) -> int:
        return sum(self.error_causes.values())

    @property
    def failed_sources(self) -> list[str]:
        return [name for name, why in self.frozen_sources.items() if why == FAILED]

    @property
    def rejections_alarming(self) -> bool:
        return self.rejections > max(REJECTION_ALERT_FLOOR, REJECTION_ALERT_SHARE * self.produced)

    def to_result(self) -> dict:
        """The JSON payload handed to the CI step and to the Tchap notifier."""
        result: dict = {
            # A failed source fails its organization in Tchap, whatever the others did.
            "success": not self.failed_sources,
            "created": self.created,
            "updated": self.updated,
            "deleted": self.deleted,
        }
        if self.closed is not None:
            result["closed"] = self.closed
        if self.dry_run:
            result["dry_run"] = True
            result["planned"] = self.planned
        if self.errors:
            result["errors"] = self.errors
            result["error_causes"] = self.error_causes
        if self.rejections:
            result["rejected"] = self.rejections
        if self.rejections_alarming:
            result["rejected_alert"] = True
        if self.total_shift:
            result["total_shift"] = self.total_shift
        if self.frozen_sources:
            result["frozen_sources"] = self.frozen_sources
        if self.volume_drops:
            result["volume_drops"] = self.volume_drops
        if self.refused:
            result["refused"] = self.refused
        if self.held:
            result["held"] = self.held
        if self.source:
            result["source"] = self.source
        result["integrated"] = {"regulations": self.regulations, "measures": self.measures}
        if self.raw_rows is not None:
            result["integrated"]["rows"] = self.raw_rows
        if self.datasets:
            result["datasets"] = self.datasets
        return result
