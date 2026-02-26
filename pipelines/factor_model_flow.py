import datetime as dt

import polars as pl
from clients import get_bear_lake_client
from configs import load_risk_model_configs
from prefect import flow, task
from utils import get_etf_returns, get_stock_returns, get_trading_date_range
from atium.factor_model import estimate_factor_model, estimate_factor_covariances


@task
def estimate_regression(
    stock_returns: pl.DataFrame, etf_returns: pl.DataFrame, window: int, half_life: int
) -> tuple[pl.DataFrame, pl.DataFrame]:
    return estimate_factor_model(
        stock_returns=stock_returns,
        factor_returns=etf_returns,
        window=window,
        half_life=half_life,
    )


@task
def upload_and_merge_factor_loadings(factor_loadings: pl.DataFrame) -> None:
    bear_lake_client = get_bear_lake_client()
    table_name = "factor_loadings"

    # Create table if not exists
    bear_lake_client.create(
        name=table_name,
        schema={
            "name": pl.String,
            "ticker": pl.String,
            "date": pl.Date,
            "year": pl.Int32,
            "factor": pl.String,
            "loading": pl.Float64,
        },
        partition_keys=["year"],
        primary_keys=["date", "ticker", "factor", "name"],
        mode="skip",
    )

    # Insert data
    bear_lake_client.insert(name=table_name, data=factor_loadings, mode="append")

    # Optimize
    bear_lake_client.optimize(name=table_name)


@task
def upload_and_merge_idio_vol(idio_vol: pl.DataFrame) -> None:
    bear_lake_client = get_bear_lake_client()
    table_name = "idio_vol"

    # Create table if not exists
    bear_lake_client.create(
        name=table_name,
        schema={
            "name": pl.String,
            "ticker": pl.String,
            "date": pl.Date,
            "year": pl.Int32,
            "idio_vol": pl.Float64,
        },
        partition_keys=["year"],
        primary_keys=["date", "ticker", "name"],
        mode="skip",
    )

    # Insert data
    bear_lake_client.insert(name=table_name, data=idio_vol, mode="append")

    # Optimize
    bear_lake_client.optimize(name=table_name)

@task
def upload_and_merge_factor_covariances(factor_covariances: pl.DataFrame):
    bear_lake_client = get_bear_lake_client()
    table_name = "factor_covariances"

    # Create table if not exists
    bear_lake_client.create(
        name=table_name,
        schema={
            "name": pl.String,
            "date": pl.Date,
            "year": pl.Int32,
            "factor_1": pl.String,
            "factor_2": pl.String,
            "covariance": pl.Float64,
        },
        partition_keys=["year"],
        primary_keys=["date", "factor_1", "factor_2", "name"],
        mode="skip",
    )

    # Insert data
    bear_lake_client.insert(name=table_name, data=factor_covariances, mode="append")

    # Optimize
    bear_lake_client.optimize(name=table_name)

def run_configs(
    configs: list[dict],
    stock_returns: pl.DataFrame,
    etf_returns: pl.DataFrame,
    filter_date: dt.date | None = None,
):
    for config in configs:
        name = config["name"]
        factors = config["factors"]
        window = config["window"]
        half_life = config["half_life"]

        config_etf_returns = etf_returns.filter(pl.col("ticker").is_in(factors))

        factor_loadings, idio_vol = estimate_regression(
            stock_returns, config_etf_returns, window=window, half_life=half_life
        )

        factor_covariances = estimate_factor_covariances(
            factor_returns=config_etf_returns, window=window, half_life=half_life
        )

        if filter_date is not None:
            factor_loadings = factor_loadings.filter(pl.col("date").eq(filter_date))
            idio_vol = idio_vol.filter(pl.col("date").eq(filter_date))
            factor_covariances = factor_covariances.filter(pl.col('date').eq(filter_date))

        factor_loadings = factor_loadings.with_columns(
            pl.lit(name).alias("name"),
            pl.col("date").dt.year().alias("year"),
        )
        idio_vol = idio_vol.with_columns(
            pl.lit(name).alias("name"),
            pl.col("date").dt.year().alias("year"),
        )
        factor_covariances = factor_covariances.with_columns(
            pl.lit(name).alias("name"),
            pl.col('date').dt.year().alias('year')
        )

        upload_and_merge_factor_loadings(factor_loadings)
        upload_and_merge_idio_vol(idio_vol)
        upload_and_merge_factor_covariances(factor_covariances)


@flow
def factor_model_backfill_flow():
    configs = load_risk_model_configs()

    start = dt.date(2020, 7, 28)
    end = dt.date.today() - dt.timedelta(days=1)

    stock_returns = get_stock_returns(start, end)
    etf_returns = get_etf_returns(start, end)

    run_configs(configs, stock_returns, etf_returns)


@flow
def factor_model_daily_flow():
    configs = load_risk_model_configs()

    max_window = max(config["window"] for config in configs)
    date_range = get_trading_date_range(window=max_window * 2)

    start = date_range["date"].min()
    end = date_range["date"].max()

    yesterday = dt.date.today() - dt.timedelta(days=1)

    # Only get new data if yesterday was the last market date
    if end != yesterday:
        print("Market was not open yesterday!")
        print("Last Market Date:", end)
        print("Yesterday:", yesterday)
        return

    stock_returns = get_stock_returns(start, end)
    etf_returns = get_etf_returns(start, end)

    run_configs(configs, stock_returns, etf_returns, filter_date=end)

if __name__ == '__main__':
    factor_model_backfill_flow()