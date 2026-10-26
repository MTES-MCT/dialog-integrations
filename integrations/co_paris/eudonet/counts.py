import polars as pl


def count_by(df: pl.DataFrame, column: str) -> dict[str, int]:
    if not df.height:
        return {}
    counts = df.get_column(column).fill_null("(empty)").value_counts(sort=True)
    return {str(k): int(v) for k, v in counts.iter_rows()}


def emptied_regulations(df: pl.DataFrame, dropped: pl.Expr) -> int:
    """Regulations every row of which `dropped` removes."""
    kept = set(df.filter(~dropped)["a_identifier"])
    return len(set(df.filter(dropped)["a_identifier"]) - kept)
