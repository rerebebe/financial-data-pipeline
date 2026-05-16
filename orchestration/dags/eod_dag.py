# eod_dag.py
from airflow import DAG
from airflow.operators.bash import BashOperator
import pendulum
from airflow.operators.python import ShortCircuitOperator
import pandas_market_calendars as mcal
from airflow.timetables.trigger import CronTriggerTimetable
from datetime import datetime, timedelta

local_tz = pendulum.timezone("America/New_York")
default_args = {
    "retries": 3,  # retry up to 3 times
    "retry_delay": timedelta(minutes=10),  # wait 10 min between retries
    "execution_timeout": timedelta(hours=1),
}


def is_trading_day(**context: any) -> bool:
    nyse = mcal.get_calendar("NYSE")
    today = context["logical_date"].astimezone(local_tz).date()
    schedule = nyse.schedule(start_date=str(today), end_date=str(today))
    return not schedule.empty  # returns True if trading day, False if holiday


# dbt project lives at /app/dbt_stocks inside the container
# because docker-compose mounts the project root to /app
DBT_PROJECT_DIR = "/app/dbt_stocks"
DBT_PROFILES_DIR = "/app/dbt_stocks"


with DAG(
    dag_id="eod_dag",
    description="Runs once at 4:30pm ET after market close. Builds daily summary → unified history → performance indicators.",
    schedule=CronTriggerTimetable("30 16 * * 1-5", timezone=local_tz),
    # schedule="30 16 * * 1-5",  # 4:30pm ET, Monday–Friday only
    # timezone=local_tz,
    start_date=datetime(2026, 1, 1, tzinfo=local_tz),
    catchup=False,  # Don't backfill missed runs when Airflow first starts
    max_active_runs=1,
    default_args=default_args,
    tags=["dbt", "stocks", "end-of-day"],
) as dag:

    # ── Check if it's trading day ──────────────────────────────────────
    check_trading_day = ShortCircuitOperator(
        task_id="check_is_trading_day",
        python_callable=is_trading_day,
        # provide_context=True,
    )

    # ── Step 1 ─────────────────────────────────────────────────────────
    # Aggregate today's intraday ticks into one summary row per symbol.
    # Input:  int_finnhub_intraday_cleaned
    # Output: 1 row per symbol — n stocks = n rows
    run_daily_summary = BashOperator(
        task_id="run_int_daily_summary_from_intraday",
        bash_command=(
            f"dbt run --select int_daily_summary_from_intraday "
            f"--project-dir {DBT_PROJECT_DIR} "
            f"--profiles-dir {DBT_PROFILES_DIR}"
        ),
    )

    # ── Step 2 ──────────────────────────────────────────────────────────
    # Merge today's summary into the long-running unified history table.
    # First run ever:  loads full 5-year history (materialized='table')
    # Later (incremental): appends today's rows only — cheaper on BigQuery
    # Input:  int_daily_summary_from_intraday + stg_5_year_stock_price
    # Output: full historical price table
    run_unified_history = BashOperator(
        task_id="run_int_unified_stock_history",
        bash_command=(
            f"dbt run --select int_unified_stock_history "
            f"--project-dir {DBT_PROJECT_DIR} "
            f"--profiles-dir {DBT_PROFILES_DIR}"
        ),
    )

    # ── Step 3 ───────────────────────────────────────────────────────────
    # Recalculate ALL technical indicators on top of the unified history.
    # These indicators are what fct_stock_intraday reads the NEXT trading day.
    # Input:  int_unified_stock_history
    # Output: SMA 20/50/200, EMA 9/20, RSI 14, Bollinger Bands, ATR etc.
    run_history_performance = BashOperator(
        task_id="run_fct_stock_history_performance",
        bash_command=(
            f"dbt run --select fct_stock_history_performance "
            f"--project-dir {DBT_PROJECT_DIR} "
            f"--profiles-dir {DBT_PROFILES_DIR}"
        ),
    )

    # ── Step 4 ──────────────────────────────────────────────────────────────
    # Pre-compute the latest indicator row per symbol from the full history.
    # This is a thin slice of fct_stock_history_performance (6 rows only).
    # Exists to avoid scanning 7,800+ rows on every intraday query.
    # Input:  fct_stock_history_performance
    # Output: 1 row per symbol with latest sma, ema, rsi values
    run_latest_indicators = BashOperator(
        task_id="run_fct_latest_indicators",
        bash_command=(
            f"dbt run --select fct_latest_indicators "
            f"--project-dir {DBT_PROJECT_DIR} "
            f"--profiles-dir {DBT_PROFILES_DIR}"
        ),
    )

    # ── Execution order ──────────────────────────────────────────────────
    (
        check_trading_day
        >> run_daily_summary
        >> run_unified_history
        >> run_history_performance
        >> run_latest_indicators
    )
