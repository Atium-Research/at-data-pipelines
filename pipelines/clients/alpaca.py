from alpaca.data import StockHistoricalDataClient
from alpaca.trading import TradingClient, GetOrdersRequest
from alpaca.trading.enums import QueryOrderStatus
import os
from dotenv import load_dotenv
import datetime as dt

load_dotenv()


def get_alpaca_historical_stock_data_client():
    api_key = os.getenv("ALPACA_API_KEY")
    secret_key = os.getenv("ALPACA_SECRET_KEY")

    if not (api_key and secret_key):
        raise RuntimeError(
            f"""
            Environment variables not set:
                ALPACA_API_KEY: {api_key}
                ALPACA_SECRET_KEY: {secret_key}
            """
        )
    return StockHistoricalDataClient(api_key, secret_key)


def get_alpaca_trading_client():
    api_key = os.getenv("ALPACA_API_KEY")
    secret_key = os.getenv("ALPACA_SECRET_KEY")
    paper = os.getenv("ALPACA_PAPER")

    if not (api_key and secret_key):
        raise RuntimeError(
            f"""
            Environment variables not set:
                ALPACA_API_KEY: {api_key}
                ALPACA_SECRET_KEY: {secret_key}
            """
        )
    return TradingClient(api_key, secret_key, paper=paper)


def get_alpaca_filled_orders(after: dt.datetime, until: dt.datetime = None):
    alpaca_client = get_alpaca_trading_client()

    if until is None:
        until = dt.datetime.now()

    filter = GetOrdersRequest(
        status=QueryOrderStatus.CLOSED,
        after=after,
        until=until,
    )

    orders = alpaca_client.get_orders(filter)

    filled_orders = []
    for order in orders:
        if (
            order.filled_at is not None
            and order.filled_qty
            and float(order.filled_qty) > 0
        ):
            filled_orders.append(
                {
                    "ticker": order.symbol,
                    "side": order.side.value,
                    "filled_qty": float(order.filled_qty),
                    "filled_avg_price": (
                        float(order.filled_avg_price) if order.filled_avg_price else 0
                    ),
                    "notional": (
                        float(order.filled_qty) * float(order.filled_avg_price)
                        if order.filled_avg_price
                        else 0
                    ),
                    "filled_at": order.filled_at,
                    "order_id": order.id,
                }
            )

    return filled_orders
