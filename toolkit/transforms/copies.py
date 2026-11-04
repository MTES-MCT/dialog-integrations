import polars as pl
from loguru import logger


def drop_copies(
    df: pl.DataFrame, columns: list[str], prefer_filled: str | None = None
) -> pl.DataFrame:
    """One row per value of `columns`, where its first copy stood; among copies, one whose
    `prefer_filled` is set is kept."""
    indexed = df.with_row_index("_first_copy").with_columns(
        pl.col("_first_copy").min().over(columns)
    )
    ordered = (
        indexed
        if prefer_filled is None
        else indexed.sort(pl.col(prefer_filled).is_null(), maintain_order=True)
    )
    deduplicated = ordered.unique(columns, keep="first").sort("_first_copy").drop("_first_copy")
    if dropped := df.height - deduplicated.height:
        logger.info(f"Dropping {dropped}/{df.height} rows copying another one")
    return deduplicated
