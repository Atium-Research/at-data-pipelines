import polars as pl

def compute(window: int) -> pl.Expr:
    return (
        pl.col('return')
        .log1p()
        .rolling_sum(window)
        .mul(-1)
    )