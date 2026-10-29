"""Free text cut to the length the API accepts, marked as cut."""

import polars as pl

ELLIPSIS = "..."
TITLE_MAX_LENGTH = 255
OTHER_CATEGORY_TEXT_MAX_LENGTH = 100


def truncate(text: pl.Expr, limit: int = TITLE_MAX_LENGTH) -> pl.Expr:
    """The text, or its start and an ellipsis when it exceeds `limit` characters."""
    return (
        pl.when(text.str.len_chars() > limit)
        .then(text.str.slice(0, limit - len(ELLIPSIS)) + pl.lit(ELLIPSIS))
        .otherwise(text)
    )


def truncate_text(value: str, limit: int = TITLE_MAX_LENGTH) -> str:
    """`truncate`, for a value built row by row in Python."""
    if len(value) <= limit:
        return value
    return value[: limit - len(ELLIPSIS)] + ELLIPSIS
