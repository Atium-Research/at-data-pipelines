from calendar_flow import calendar_backfill_flow
from universe_flow import universe_backfill_flow
from prefect import flow, serve
from prefect.schedules import Cron
import datetime as dt

@flow
def daily_flow():
    """Run calendar backfill, then universe backfill sequentially."""
    calendar_backfill_flow()
    universe_backfill_flow()

if __name__ == "__main__":
    serve(
        daily_flow.to_deployment(
            name="daily-flow",
            schedule=Cron("0 2 * * *", timezone="America/Denver")
        ),
        calendar_backfill_flow.to_deployment(name="calendar-backfill-flow"),
        universe_backfill_flow.to_deployment(name="universe-backfill-flow")
    )