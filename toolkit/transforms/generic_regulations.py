import polars as pl
from loguru import logger

from api.dia_log_client.models import (
    PostApiRegulationsAddBodyCategory,
    PostApiRegulationsAddBodySubject,
)


def generic_regulations(
    df: pl.DataFrame, prefix: str, title: pl.Expr, other_category_text: str
) -> pl.DataFrame:
    """`{prefix}{measure_group_key}` (`LIMOGES-LT-T3.5`): every row of one measure in one
    regulation, its title ending with the number of sections it carries."""
    sections = pl.len().over("regulation_identifier")
    return df.with_columns(
        regulation_identifier=pl.lit(prefix) + pl.col("measure_group_key")
    ).with_columns(
        **_permanent_other(other_category_text),
        regulation_title=pl.format(
            "{} ({} section{})",
            title,
            sections,
            pl.when(sections > 1).then(pl.lit("s")).otherwise(pl.lit("")),
        ),
    )


def _permanent_other(other_category_text: str) -> dict[str, pl.Expr]:
    return {
        "regulation_category": pl.lit(PostApiRegulationsAddBodyCategory.PERMANENTREGULATION.value),
        "regulation_subject": pl.lit(PostApiRegulationsAddBodySubject.OTHER.value),
        "regulation_other_category_text": pl.lit(other_category_text),
    }


def looks_like_act(number: pl.Expr, min_length: int) -> pl.Expr:
    """Whether the number, stripped, has at least `min_length` letters or digits and nothing
    else: shorter ones read as a rank in a list, not as an act."""
    return number.str.strip_chars().str.contains(f"^[0-9A-Za-z]{{{min_length},}}$").fill_null(False)


def numbered_or_generic(
    df: pl.DataFrame,
    *,
    keeps_number: pl.Expr,
    number: pl.Expr,
    prefix: str,
    numbered_title: pl.Expr,
    generic_title: pl.Expr,
    other_category_text: str,
) -> pl.DataFrame:
    """The producer's act as `{prefix}{number}` where `keeps_number`, the generic regulation
    of the measure everywhere else."""
    kept = keeps_number.fill_null(False)
    displaced = df.filter(~kept & number.is_not_null()).select(number.alias("_number")).to_series()
    if not displaced.is_empty():
        logger.info(
            f"{displaced.len()} rows of {displaced.n_unique()} numbers not read as an act go "
            f"to their measure's generic regulation, e.g. {sorted(displaced.unique())[:5]}"
        )
    numbered = df.filter(kept).with_columns(
        regulation_identifier=pl.lit(prefix) + number,
        **_permanent_other(other_category_text),
        regulation_title=numbered_title,
    )
    generic = generic_regulations(df.filter(~kept), prefix, generic_title, other_category_text)
    return pl.concat([numbered, generic], how="diagonal_relaxed")
