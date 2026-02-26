import datetime as dt

import polars as pl
from clients import get_bear_lake_client
from configs import load_portfolio_configs
from prefect import flow, task
from utils import (get_alphas, get_benchmark_weights,
                   get_factor_covariances, get_factor_loadings, get_idio_vol,
                   get_last_market_date)
from atium.optimizer import MVO
from atium.optimizer_constraints import FullyInvested, LongOnly
from atium.objectives import MaxUtilityWithTargetActiveRisk
from atium.risk_model import FactorRiskModel


OBJECTIVE_REGISTRY = {
    "max-utility-with-target-active-risk": MaxUtilityWithTargetActiveRisk,
}

CONSTRAINT_REGISTRY = {
    "fully-invested": FullyInvested,
    "long-only": LongOnly,
}


@task
def calculate_portfolio_weights(
    config: dict,
    alphas: pl.DataFrame,
    benchmark_weights: pl.DataFrame,
    factor_loadings: pl.DataFrame,
    factor_covariances: pl.DataFrame,
    idio_vol: pl.DataFrame,
) -> pl.DataFrame:
    objective = OBJECTIVE_REGISTRY[config["objective"]](**config["parameters"])
    constraints = [CONSTRAINT_REGISTRY[c]() for c in config["constraints"]]
    mvo = MVO(objective=objective, constraints=constraints)

    risk_model = FactorRiskModel(
        factor_loadings=factor_loadings,
        factor_covariances=factor_covariances,
        idio_vol=idio_vol,
    )

    dates = alphas.select("date").unique().sort("date")["date"].to_list()

    all_weights = []
    for date_ in dates:
        weights = mvo.optimize(
            date_=date_,
            risk_model=risk_model,
            alphas=alphas,
            benchmark_weights=benchmark_weights,
        )
        all_weights.append(weights)

    return pl.concat(all_weights)


@task
def upload_and_merge_portfolio_weights(portfolio_weights: pl.DataFrame):
    bear_lake_client = get_bear_lake_client()
    table_name = "portfolio_weights"

    bear_lake_client.create(
        name=table_name,
        schema={
            "ticker": pl.String,
            "date": pl.Date,
            "year": pl.Int32,
            "name": pl.String,
            "weight": pl.Float64,
        },
        partition_keys=["year"],
        primary_keys=["date", "ticker", "name"],
        mode="skip",
    )

    bear_lake_client.insert(name=table_name, data=portfolio_weights, mode="append")

    bear_lake_client.optimize(name=table_name)


def run_configs(
    configs: list[dict],
    start: dt.date,
    end: dt.date,
    filter_date: dt.date | None = None,
):
    benchmark_weights = get_benchmark_weights(start, end)

    for config in configs:
        name = config["name"]

        alphas = get_alphas(start, end, name=config["alpha"])
        factor_loadings = get_factor_loadings(start, end, name=config["risk_model"])
        factor_covariances = get_factor_covariances(start, end, name=config["risk_model"])
        idio_vol = get_idio_vol(start, end, name=config["risk_model"])

        portfolio_weights = calculate_portfolio_weights(
            config, alphas, benchmark_weights,
            factor_loadings, factor_covariances, idio_vol,
        )

        if filter_date is not None:
            portfolio_weights = portfolio_weights.filter(pl.col("date").eq(filter_date))
            if len(portfolio_weights) == 0:
                raise ValueError(f"No weights found for config '{name}' on {filter_date}!")

        portfolio_weights = portfolio_weights.with_columns(
            pl.lit(name).alias("name"),
            pl.col("date").dt.year().alias("year"),
        ).select("ticker", "date", "year", "name", "weight")

        upload_and_merge_portfolio_weights(portfolio_weights)


@flow
def portfolio_weights_backfill_flow():
    configs = load_portfolio_configs()

    start = dt.date(2022, 7, 29)
    end = dt.date.today() - dt.timedelta(days=1)

    run_configs(configs, start, end)


@flow
def portfolio_weights_daily_flow():
    configs = load_portfolio_configs()

    last_market_date = get_last_market_date()
    yesterday = dt.date.today() - dt.timedelta(days=1)

    if last_market_date != yesterday:
        print("Market was not open yesterday!")
        print("Last Market Date:", last_market_date)
        print("Yesterday:", yesterday)
        return

    run_configs(configs, last_market_date, last_market_date, filter_date=last_market_date)


if __name__ == '__main__':
    portfolio_weights_backfill_flow()
