import json
from importlib import util as importlib_util

import polars as pl
from loguru import logger

from api.dia_log_client import Client
from api.dia_log_client.api.private.delete_api_regulations_delete import (
    sync_detailed as delete_regulation,
)
from api.dia_log_client.api.private.get_api_organization_identifiers import (
    sync_detailed as _get_identifiers,
)
from api.dia_log_client.api.private.post_api_regulations_add import (
    sync_detailed as add_regulation,
)
from api.dia_log_client.api.private.put_api_regulations_publish import (
    sync_detailed as publish_regulation,
)
from api.dia_log_client.models import (
    MeasureTypeEnum,
    PostApiRegulationsAddBody,
    PostApiRegulationsAddBodyCategory,
    PostApiRegulationsAddBodyStatus,
    PostApiRegulationsAddBodySubject,
    RoadTypeEnum,
    SaveLocationDTO,
    SaveMeasureDTO,
    SaveNumberedRoadDTO,
    SavePeriodDTO,
    SaveRawGeoJSONDTO,
    SaveVehicleSetDTO,
)
from integrations.base_data_source_integration import BaseDataSourceIntegration, RegulationMeasure
from settings import OrganizationSettings


class BaseIntegration:
    """Base integration class that orchestrates data sources and API interactions."""

    client: Client
    status: PostApiRegulationsAddBodyStatus = PostApiRegulationsAddBodyStatus.DRAFT
    organization_settings: OrganizationSettings
    data_sources: list[type[BaseDataSourceIntegration]]

    def __init__(self, organization_settings: OrganizationSettings, client: Client):  # type: ignore
        self.organization_settings = organization_settings
        self.client = client

    @property
    def organization(self) -> str:
        return self.organization_settings.organization

    @classmethod
    def from_organization(cls, organization: str, env: str = "dev") -> "BaseIntegration":
        """Create Integration from organization name and environment."""
        organization_settings = OrganizationSettings.from_env(organization, env=env)
        return cls.from_settings(organization_settings)

    @classmethod
    def from_settings(cls, organization_settings: OrganizationSettings) -> "BaseIntegration":
        """Create Integration from pre-configured settings."""
        client = Client(
            base_url=organization_settings.base_url,  # type: ignore
            raise_on_unexpected_status=True,
            headers={
                "X-Client-Id": organization_settings.client_id,
                "X-Client-Secret": organization_settings.client_secret,
                "Accept": "application/json",
            },  # type: ignore
        )

        # Import the Integration class from the organization's module
        integration_module = f"integrations.{organization_settings.organization}.integration"
        spec = importlib_util.find_spec(integration_module)
        if spec is None:
            raise ImportError(f"Cannot find module {integration_module}")

        module = importlib_util.module_from_spec(spec)
        if spec.loader is None:
            raise ImportError(f"Cannot load {integration_module}")
        spec.loader.exec_module(module)

        if not hasattr(module, "Integration"):
            raise AttributeError("Integration class not found in module")

        return getattr(module, "Integration")(organization_settings, client)

    def integrate_regulations(self, limit_to=[], update_existing: bool | None = None) -> None:
        """
        Integrate regulations from all data sources.
        Iterates over data sources, collects data, and integrates.
        """
        # Get all data sources
        # Collect clean data from all sources

        # Get existing regulation IDs
        try:
            integrated_regulation_ids = self.fetch_regulation_ids()
        except Exception as e:
            logger.info(
                f"Could not fetch existing regulation ids"
                f", defaulting to update_existing=False - {e}"
            )
            integrated_regulation_ids = []
            update_existing = False

        for data_source in self.data_sources:
            logger.info(f"Processing data source: {data_source.name}")
            clean_data = data_source(
                self.organization_settings, self.client
            ).compute_data_regulations()

            # Only process whitelisted identifiers
            if limit_to and len(limit_to) > 0:
                logger.info(f"Limiting processing to following ids : {limit_to}")
                clean_data = clean_data.filter(pl.col("regulation_identifier").is_in(limit_to))

            logger.info(f"Total records from all sources: {clean_data.shape[0]}")

            # Create regulations from combined data
            regulations = self.create_regulations(clean_data, data_source)
            for regulation in regulations:
                regulation.identifier = f"{regulation.identifier}"
                regulation.status = self.status
            num_measures = sum([len(regulation.measures or []) for regulation in regulations])
            logger.info(f"Created {len(regulations)} regulations with a total of {num_measures}")

            # Filter regulations to create
            regulation_ids_to_create = set([r.identifier for r in regulations]) - set(
                integrated_regulation_ids
            )
            regulations_to_create = [
                regulation
                for regulation in regulations
                if regulation.identifier in regulation_ids_to_create
            ]
            logger.info(f"Found {len(regulations_to_create)} new regulations to integrate")

            # Integrate new regulations
            self._integrate_regulations_add(regulations_to_create)

            if update_existing:
                # Filter regulations to update
                regulation_ids_to_update = set([r.identifier for r in regulations]) & set(
                    integrated_regulation_ids
                )
                regulations_to_update = [
                    regulation
                    for regulation in regulations
                    if regulation.identifier in regulation_ids_to_update
                ]
                logger.info(f"Found {len(regulations_to_update)} regulations to update")

                # Integrate updated regulations
                self._integrate_regulations_update(regulations_to_update)

    def publish_regulations(self) -> None:
        regulation_ids = self.fetch_regulation_ids()
        count_error = 0
        for index, regulation_id in enumerate(regulation_ids):
            try:
                publish_regulation(identifier=regulation_id, client=self.client)
                logger.success(
                    f"Measure {index}/{len(regulation_ids)} successfully published: {regulation_id}"
                )
            except Exception:
                logger.error(
                    f"Measure {index}/{len(regulation_ids)} failed to publish: {regulation_id}"
                )
                count_error += 1

        if count_error > 0:
            logger.error(f"Failed to publish {count_error} identifier(s)")
        logger.success(
            f"Finished publishing {len(regulation_ids) - count_error} measures successfully"
        )

    def _integrate_regulations_add(self, regulations: list[PostApiRegulationsAddBody]) -> None:
        count_error = 0
        for index, regulation in enumerate(regulations):
            logger.info(
                f"Creating regulation {index + 1}/{len(regulations)}: {regulation.identifier}"
            )
            logger.info(f"Contains {len(regulation.measures)} measures.")  # type: ignore
            try:
                resp = add_regulation(client=self.client, body=regulation)
            except Exception as e:
                logger.error(f"Failed to create: {regulation.identifier} - {e}")
                count_error += 1
            else:
                if resp.status_code != 201:
                    logger.error(
                        f"Failed to create: {regulation.identifier} - got status {resp.status_code}"
                    )
                    logger.error(json.loads(resp.content))
                    count_error += 1

        count_success = len(regulations) - count_error
        logger.success(
            f"Finished integrating {count_success}/{len(regulations)} regulations successfully"
        )

    def _integrate_regulations_update(self, regulations: list[PostApiRegulationsAddBody]) -> None:
        count_error = 0
        for index, regulation in enumerate(regulations):
            logger.info(
                f"Updating regulation {index + 1}/{len(regulations)}: {regulation.identifier}"
            )
            logger.info(f"Contains {len(regulation.measures)} measures.")  # type: ignore

            try:
                delete_regulation(identifier=str(regulation.identifier), client=self.client)
                resp = add_regulation(client=self.client, body=regulation)
            except Exception as e:
                logger.error(f"Failed to create: {regulation.identifier} - {e}")
                count_error += 1
            else:
                if resp.status_code != 201:
                    logger.error(
                        f"Failed to create: {regulation.identifier} - got status {resp.status_code}"
                    )
                    logger.error(json.loads(resp.content))
                    count_error += 1
        count_success = len(regulations) - count_error
        logger.success(
            f"Finished updating {count_success}/{len(regulations)} regulations successfully"
        )

    def create_measure(
        self, measure: RegulationMeasure, locations: list[RegulationMeasure] | None = None
    ) -> SaveMeasureDTO:
        """
        Create a single measure from a RegulationMeasure.
        Default implementation that works for most cases.
        Subclasses can override if needed.

        `measure` carries the measure, period and vehicle fields. `locations` optionally
        carries the rows whose locations belong to this measure — one measure covering N
        road segments, which is what a SIG-sourced regulation looks like. When omitted,
        the measure gets the single location held by `measure` itself.
        """
        location_rows = locations if locations is not None else [measure]
        params = {
            "type_": MeasureTypeEnum(measure["measure_type_"]),
            "periods": [self.create_save_period_dto(measure)],
            "locations": [self.create_save_location_dto(row) for row in location_rows],
            "vehicle_set": self.create_save_vehicle_dto(measure),
        }

        # Add max_speed if present and not None
        if measure["measure_type_"] == MeasureTypeEnum.SPEEDLIMITATION.value:
            if measure.get("measure_max_speed"):
                params["max_speed"] = int(measure["measure_max_speed"])  # type: ignore

        return SaveMeasureDTO(**params)

    def create_regulations(
        self,
        clean_data: pl.DataFrame,
        data_source: BaseDataSourceIntegration | type[BaseDataSourceIntegration] | None = None,
    ) -> list[PostApiRegulationsAddBody]:
        """
        Create regulation payloads from clean data.
        Groups by regulation_identifier and creates measures for each group.
        Uses precomputed regulation fields from the DataFrame.

        Two behaviours, driven by the data source (see `BaseDataSourceIntegration`):

        - `group_locations_by_measure` — rows sharing `measure_group_key` collapse into a
          single measure carrying all of their locations, instead of one measure per row;
        - `max_locations_per_regulation` — a regulation carrying more locations than the
          ceiling is cut into `IDENTIFIER-01`, `IDENTIFIER-02`… slices.
        """
        group_by_measure = bool(getattr(data_source, "group_locations_by_measure", False))
        max_locations = getattr(data_source, "max_locations_per_regulation", None)

        regulations = []

        for _, group_df in clean_data.group_by("regulation_identifier"):
            slices = self._split_regulation_rows(group_df, max_locations)

            for index, slice_df in enumerate(slices):
                measures = self._create_measures(slice_df, group_by_measure)

                # Skip if no measures were created
                if not measures:
                    continue

                # Get regulation fields from first row (all rows have same values)
                first_row = slice_df.row(0, named=True)

                identifier = first_row["regulation_identifier"]
                if len(slices) > 1:
                    identifier = f"{identifier}-{index + 1:02d}"

                regulation = PostApiRegulationsAddBody(
                    identifier=identifier,
                    category=PostApiRegulationsAddBodyCategory(first_row["regulation_category"]),
                    status=PostApiRegulationsAddBodyStatus(self.status),
                    subject=PostApiRegulationsAddBodySubject(first_row["regulation_subject"]),
                    title=first_row["regulation_title"],
                    other_category_text=first_row.get("regulation_other_category_text"),
                    measures=measures,  # type: ignore
                )

                # Add document URL to additional_properties if present
                if first_row.get("regulation_document_url"):
                    regulation.additional_properties["documentUrl"] = first_row[
                        "regulation_document_url"
                    ]

                regulations.append(regulation)

        return regulations

    def _create_measures(self, slice_df: pl.DataFrame, group_by_measure: bool) -> list:
        """Build the measures of one regulation slice.

        Without grouping, every row is its own measure with its own location — the
        historical behaviour of every source in production. With grouping, the rows
        sharing a `measure_group_key` become one measure carrying all their locations.
        """
        measures = []

        if not group_by_measure:
            for row in slice_df.iter_rows(named=True):
                try:
                    measures.append(self.create_measure(row))  # type: ignore
                except Exception as e:
                    logger.error(f"Error creating measure: {e}")
            return measures

        for _, measure_df in slice_df.group_by("measure_group_key", maintain_order=True):
            rows = list(measure_df.iter_rows(named=True))
            try:
                measures.append(self.create_measure(rows[0], locations=rows))  # type: ignore
            except Exception as e:
                logger.error(f"Error creating measure: {e}")

        return measures

    def _split_regulation_rows(
        self, group_df: pl.DataFrame, max_locations: int | None
    ) -> list[pl.DataFrame]:
        """Cut a regulation into slices small enough for a single POST.

        The API is not bounded by payload size but by how long it takes to persist the
        locations: past roughly 1 700 of them the request dies on a server-side timeout,
        and splitting the same total across several measures does not help — the ceiling
        is per regulation. Slices are cut along `regulation_split_order`, so a source
        that ranks its rows geographically gets spatially coherent slices.
        """
        if not max_locations or group_df.height <= max_locations:
            return [group_df]

        if "regulation_split_order" in group_df.columns:
            group_df = group_df.sort("regulation_split_order", nulls_last=True)

        slices = [
            group_df.slice(offset, max_locations)
            for offset in range(0, group_df.height, max_locations)
        ]
        logger.info(
            f"Regulation {group_df.row(0, named=True)['regulation_identifier']} carries "
            f"{group_df.height} locations, above the {max_locations} ceiling: "
            f"splitting into {len(slices)} regulations"
        )
        return slices

    def create_save_period_dto(self, measure: RegulationMeasure) -> SavePeriodDTO:
        """
        Create a SavePeriodDTO from a RegulationMeasure with period_ prefixed fields.
        Any field starting with 'period_' will be mapped to SavePeriodDTO,
        with the prefix stripped (e.g., period_start_date -> start_date).

        `startTime` and `endTime` are mirrored from the dates. The API splits a single
        instant across two fields: it takes the day from `startDate` and the clock from
        `startTime`, and reads that clock in Europe/Paris.
        """
        period_fields = {}
        for key, value in measure.items():
            if key.startswith("period_"):
                field_name = key.replace("period_", "", 1)
                period_fields[field_name] = value

        period_fields["start_time"] = period_fields.get("start_date")
        period_fields["end_time"] = period_fields.get("end_date")

        return SavePeriodDTO(**period_fields)

    def create_save_location_dto(
        self, measure: RegulationMeasure
    ) -> SaveLocationDTO | SaveNumberedRoadDTO:
        """
        Create a SaveLocationDTO from a RegulationMeasure with location_ prefixed fields.
        Expects location_road_type (string), location_label, and location_geometry fields.
        """
        road_type_value = measure["location_road_type"]
        road_type = RoadTypeEnum(road_type_value)

        location_fields = {}
        for key, value in measure.items():
            if key.startswith("location_") and key != "location_road_type":
                field_name = key.replace("location_", "", 1)
                location_fields[field_name] = value
        if road_type == RoadTypeEnum.RAWGEOJSON:
            return SaveLocationDTO(
                road_type=road_type,
                raw_geo_json=SaveRawGeoJSONDTO(**location_fields),
            )
        elif road_type in [RoadTypeEnum.DEPARTMENTALROAD, RoadTypeEnum.NATIONALROAD]:
            payload = {
                "road_type": road_type,
                (
                    "national_road"
                    if road_type == RoadTypeEnum.NATIONALROAD
                    else "departmental_road"
                ): SaveNumberedRoadDTO(**location_fields),
            }
            return SaveLocationDTO(**payload)
        else:
            raise Exception(f"Location saving not implemented for  RoadType {road_type.value}")

    def create_save_vehicle_dto(self, measure: RegulationMeasure) -> SaveVehicleSetDTO:
        """
        Create a SaveVehicleSetDTO from a measure with vehicle_ prefixed fields.
        Intelligently handles the all_vehicles flag:
        - If all_vehicles=True and no restrictions/dimensions, only passes all_vehicles
        - Otherwise, includes all relevant fields
        """
        # Extract vehicle fields
        vehicle_fields = {}
        for key, value in measure.items():
            if key.startswith("vehicle_"):
                field_name = key.replace("vehicle_", "", 1)
                vehicle_fields[field_name] = value

        # Clean params: remove None, empty lists
        cleaned = {k: v for k, v in vehicle_fields.items() if v not in (None, [], {})}

        # If all_vehicles is True and there are no other constraints, simplify
        if cleaned.get("all_vehicles") is True and len(cleaned) == 1:
            return SaveVehicleSetDTO(all_vehicles=True)

        return SaveVehicleSetDTO(**cleaned)

    def fetch_regulation_ids(self) -> list[str]:
        logger.info(f"Fetching identifiers for organization: {self.organization}")
        resp = _get_identifiers(client=self.client)

        if resp.parsed is None or not hasattr(resp.parsed, "identifiers"):
            raise Exception("Failed to fetch identifiers")

        identifiers: list[str] = resp.parsed.identifiers  # type: ignore

        logger.info(f"Found {len(identifiers)} identifier(s) for organization {self.organization}")

        return list(identifiers)
