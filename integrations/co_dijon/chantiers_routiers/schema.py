"""Schema for Dijon chantiers routiers CSV data."""

import pandera.polars as pa


class DijonChantiersRoutiersSchema(pa.DataFrameModel):
    """Schema for Dijon chantiers routiers CSV data."""

    objectid: int
    reference: int
    datetime_start: str
    datetime_end: str
    title: str
    geometry: str
    geo_point_2d: str

    class Config(pa.DataFrameModel.Config):
        """Config for the schema."""

        strict = False  # Allow extra columns in raw data
        coerce = True  # Allow type coercion during validation
