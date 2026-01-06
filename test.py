from clients import get_bear_lake_client
from utils import get_factor_covariances, get_factor_loadings, get_idio_vol, get_alphas
from rich import print
import bear_lake as bl
import polars as pl
import datetime as dt

bear_lake_client = get_bear_lake_client()

tables = bear_lake_client.list_tables()
print(tables)

# for table in ['factor_loadings', 'idio_vol']:
#     bear_lake_client.drop(table)


start = dt.date(2022, 7, 29)
end = dt.date(2026, 1,2 )

print(
    get_factor_loadings(start, end)
    .filter(
        pl.col('ticker').eq('GEV'),
        pl.col('factor').eq('MTUM'),
        # pl.col('date').eq(end)
    )
    .drop_nulls()
    # .group_by('date', 'factor')
    # .agg(pl.len())
    # .sort('date')
)

# print(
#     get_idio_vol(start, end)
#     .filter(
#         # pl.col('ticker').eq('Q'),
#         pl.col('idio_vol').is_null(),
#         pl.col('date').eq(end)
#     )
#     # .group_by('date')
#     # .agg(pl.len())
#     # .sort('date')
# )

# print(
#     get_alphas(start, end)
#     .filter(
#         # pl.col('ticker').eq('Q'),
#         pl.col('alpha').is_null(),
#         pl.col('date').eq(end)
#     )
#     # .group_by('date')
#     # .agg(pl.len())
#     # .sort('date')
# )
