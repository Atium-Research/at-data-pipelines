import polars as pl

def compute(window: int, shift: int) -> pl.Expr:
    return (
        pl.col('return')
        .log1p()
        .rolling_sum(window)
        .shift(shift)
    )