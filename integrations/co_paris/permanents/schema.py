"""Schema for Nantes Circulation Chantier."""

import pandera.polars as pa


class ParisEudonetSchema(pa.DataFrameModel):
    def validate(self, data):
        raise NotImplementedError("Implement Paris eudonet schema validation")
