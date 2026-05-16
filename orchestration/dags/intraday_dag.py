# intraday_dag.py

from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import pendulum
from airflow.operators.python import ShortCircuitOperator
import pandas_market_calendars as mcal
from airflow.timetables.trigger import CronTriggerTimetable

local_tz = pendulum.timezone("America/New_York")


def is_trading_day(**context: any) -> bool:
    nyse = mcal.get_calendar("NYSE")
    today = context["logical_date"].astimezone(local_tz).date()
    schedule = nyse.schedule(start_date=str(today), end_date=str(today))
    return not schedule.empty  # returns True if trading day, False if holiday


DBT_PROJECT_DIR = "/app/dbt_stocks"
DBT_PROFILES_DIR = "/app/dbt_stocks"

default_args = {
    "retries": 3,
    "retry_delay": timedelta(minutes=1),
    "execution_timeout": timedelta(minutes=30),
}


# Define the schedule
with DAG(
    dag_id="intraday_dag",
    description="Runs every 5 mins during market hours. Refreshes real-time price view with latest indicators from EOD.",
    schedule=CronTriggerTimetable("*/5 9-15 * * 1-5", timezone=local_tz),
    # schedule="*/5 9-15 * * 1-5",  # Every 5 mins, 9am–4pm ET, Mon–Fri
    # timezone=local_tz,
    start_date=datetime(2026, 1, 1, tzinfo=local_tz),
    catchup=False,
    max_active_runs=1,  # one by one
    default_args=default_args,
    tags=["dbt", "stocks", "intraday"],
) as dag:

    check_trading_day = ShortCircuitOperator(
        task_id="check_is_trading_day",
        python_callable=is_trading_day,
        # provide_context=True,
    )

    # Define what to run
    run_intraday = BashOperator(
        task_id="run_fct_stock_intraday",
        bash_command=(
            f"dbt run --select fct_stock_intraday"
            f" --project-dir {DBT_PROJECT_DIR}"
            f" --profiles-dir {DBT_PROFILES_DIR}"
        ),
    )

    check_trading_day >> run_intraday
