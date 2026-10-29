from typing import get_type_hints

import pandera.polars as pa
import polars as pl
from loguru import logger

from api.dia_log_client import Client
from canonical.regulation_measure import RegulationMeasure, unknown_canonical_columns
from settings import OrganizationSettings
from toolkit.rules.apply import RulesResult, apply_rules
from toolkit.rules.rule import Outcome, Rule


class BaseDataSourceIntegration:
    """One data source. Subclasses set `name` and `raw_data_schema`, and implement
    `fetch_raw_data` and `compute_clean_data`."""

    name: str | None = None
    raw_data_schema: type[pa.DataFrameModel] | None = None

    # Owned inside the organization's prefix (`AV-LV-` in `AV-`): a failing or paused
    # source freezes only this one.
    identifier_prefix: str | None = None

    # Opt-in: collapse the rows sharing a `measure_group_key` into a single measure
    # carrying every one of their locations. Off by default: the sources already
    # in production emit one measure per row.
    group_locations_by_measure: bool = False

    # Most locations a single POST may carry: above it the regulation is cut into
    # `IDENTIFIER-01`, `IDENTIFIER-02`… slices, ordered by `regulation_split_order`.
    max_locations_per_regulation: int | None = None

    # The source's columns read as canonical ones, for `apply_rules` before its canonical
    # frame exists (a source that groups by order number applies them before grouping).
    rule_inputs: dict[str, pl.Expr] = {}
    # The rules of `toolkit/rules/` this source applies, each in place of its own code.
    rules: tuple[Rule, ...] = ()

    organization_settings: OrganizationSettings
    client: Client

    def __init__(self, organization_settings: OrganizationSettings, client: Client):
        self.organization_settings = organization_settings
        self.client = client
        # By "label (nature)": rows each rule applied to, for the report.
        self.rule_counts: dict[str, int] = {}
        self._applied_codes: set[str] = set()

    @property
    def organization(self) -> str:
        return self.organization_settings.organization

    # In the source's own unit (row, segment or restriction); their ratio, the share of the
    # dataset retained, is shown in the Tchap report. Default: rows fetched, rows returned.
    dataset_restrictions: int | None = None
    retained_restrictions: int | None = None

    def count_dataset_restrictions(self, df: pl.DataFrame) -> pl.DataFrame:
        """Pipe it where every row is one restriction the dataset states: right after
        the step that splits a row into its restrictions, before any filter."""
        self.dataset_restrictions = df.height
        logger.info(f"{df.height} restrictions stated by the dataset")
        return df

    def count_retained_restrictions(self, df: pl.DataFrame) -> pl.DataFrame:
        """Pipe it after the last filter and before rows are merged into measures, so
        the count stays in the unit of `count_dataset_restrictions`."""
        self.retained_restrictions = df.height
        logger.info(f"{df.height} restrictions retained by the pipeline")
        return df

    def compute_data_regulations(self) -> pl.DataFrame:
        """Fetch, validate, clean, then keep the `RegulationMeasure` columns."""
        raw_data = self.fetch_raw_data()
        logger.info(f"Fetched {raw_data.shape[0]} raw records")
        validated_data = self.validate_raw_data(raw_data)
        clean_data = validated_data.pipe(self.compute_clean_data)
        logger.info(f"After cleaning, got {clean_data.shape[0]} records")
        clean_data = self._apply_rules_to_canonical(clean_data)

        clean_data = self.select_regulation_measure_fields(clean_data)
        return clean_data

    def apply_rules(self, df: pl.DataFrame, *own_rules: Rule) -> pl.DataFrame:
        """Apply the source's `rules` and any rule of its own, counting each."""
        rules = [*self.rules, *own_rules]
        if not rules:
            return df
        result = apply_rules(df, rules, self.rule_inputs, source=self.name or "")
        self._count(result, rules)
        self._applied_codes |= {r.code for r in rules if r.code not in result.unevaluated}
        return result.frame

    def _apply_rules_to_canonical(self, df: pl.DataFrame) -> pl.DataFrame:
        """Apply the rules to the canonical rows; a decision already applied must find nothing."""
        rules = [
            rule
            for rule in self.rules
            if rule.code not in self._applied_codes or rule.outcome is Outcome.DROP
        ]
        result = apply_rules(df, rules, source=self.name or "")
        if again := {c: n for c, n in result.applied.items() if c in self._applied_codes}:
            logger.error(f"{self.name}: rules already applied still apply to rows: {again}")
        self._count(result, rules)
        return result.frame

    def _count(self, result: RulesResult, rules: list[Rule]) -> None:
        for rule in rules:
            if applied := result.applied.get(rule.code):
                key = f"{rule.label} ({rule.nature.value}, {rule.outcome.value})"
                self.rule_counts[key] = self.rule_counts.get(key, 0) + applied

    def fetch_raw_data(self) -> pl.DataFrame:
        raise NotImplementedError("Subclasses must implement fetch_raw_data method")

    def preprocess_raw_data(self, raw_data: pl.DataFrame) -> pl.DataFrame:
        """Minimal casts before validation (e.g. booleans); identity by default."""
        return raw_data

    def validate_raw_data(self, raw_data: pl.DataFrame) -> pl.DataFrame:
        """Keep only the schema's columns, preprocess, then validate with Pandera.

        A schema column missing from the source fails the whole integration.
        """
        if self.raw_data_schema is None:
            raise NotImplementedError("Subclasses must set raw_data_schema class attribute")

        logger.info(f"Validating raw data schema with {raw_data.shape[0]} rows")

        columns_to_keep = list(self.raw_data_schema.to_schema().columns.keys())
        logger.info(f"Keeping {len(columns_to_keep)} columns: {columns_to_keep}")
        logger.info(f"Discarding columns: {set(raw_data.columns) - set(columns_to_keep)}")
        df = raw_data.select(columns_to_keep)

        df = self.preprocess_raw_data(df)
        validated_df = self.raw_data_schema.validate(df)

        logger.info(
            f"Raw data validation successful: {validated_df.shape[0]} rows, "
            f"{validated_df.shape[1]} columns"
        )

        return validated_df

    def compute_clean_data(self, raw_data: pl.DataFrame) -> pl.DataFrame:
        raise NotImplementedError("Subclasses must implement compute_clean_data method")

    def select_regulation_measure_fields(self, df: pl.DataFrame) -> pl.DataFrame:
        """Keep the canonical columns; an unknown one with a canonical prefix is logged."""
        unknown = unknown_canonical_columns(df.columns)
        if unknown:
            logger.error(
                f"{self.name}: columns named like the canonical model but not in it, "
                f"dropped: {unknown}"
            )
        required_fields = list(get_type_hints(RegulationMeasure).keys())
        available_fields = [field for field in required_fields if field in df.columns]

        return df.select(available_fields)
