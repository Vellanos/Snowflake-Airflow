from pathlib import Path

import pendulum
import requests
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.common.sql.operators.sql import SQLCheckOperator, SQLExecuteQueryOperator
from airflow.sdk import TaskGroup, dag, get_current_context, task


@dag(
    dag_id="nyc_taxi_monthly",
    schedule="@monthly",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    end_date=pendulum.datetime(2025, 3, 1, tz="UTC"),
    catchup=True,
    max_active_runs=1,
    template_searchpath="/usr/local/airflow/include/sql",
    params={
        "max_trip_distance_miles": 100,
        "max_trip_duration_min": 180,
        "start_month": "2025-01-01",
        "end_month": "2025-04-01",
        "max_rejected_pct": 10,
    },
    default_args={"retries": 2, "retry_delay": pendulum.duration(minutes=5)},
)
def nyc_taxi_monthly():
    @task
    def nom_du_fichier() -> str:
        ctx = get_current_context()
        month = ctx["data_interval_start"].strftime("%Y-%m")
        return f"yellow_tripdata_{month}.parquet"

    @task
    def verifier_disponibilite(filename: str) -> str:
        url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{filename}"
        with requests.head(url, timeout=30) as response:
            response.raise_for_status()
        return url

    @task
    def telecharger_et_deposer(url: str) -> str:
        destination = Path("/tmp") / url.rsplit("/", 1)[-1]
        try:
            with requests.get(url, stream=True, timeout=(10, 120)) as response:
                response.raise_for_status()
                with destination.open("wb") as output:
                    for chunk in response.iter_content(chunk_size=8 * 1024 * 1024):
                        output.write(chunk)
            SnowflakeHook(snowflake_conn_id="snowflake_nyc_taxi").run(
                f"PUT 'file://{destination}' @NYC_TAXI.RAW.TLC_STAGE "
                "AUTO_COMPRESS=FALSE OVERWRITE=FALSE"
            )
        finally:
            destination.unlink(missing_ok=True)
        return destination.name

    @task
    def copier_dans_raw(filename: str) -> None:
        SnowflakeHook(snowflake_conn_id="snowflake_nyc_taxi").run(f"""
            COPY INTO NYC_TAXI.RAW.YELLOW_TRIPDATA
            FROM @NYC_TAXI.RAW.TLC_STAGE
            FILES = ('{filename}')
            FILE_FORMAT = (FORMAT_NAME = NYC_TAXI.RAW.PARQUET_FF)
            MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
            INCLUDE_METADATA = (
                _source_file = METADATA$FILENAME,
                _loaded_at = METADATA$START_SCAN_TIME
            )
            ON_ERROR = ABORT_STATEMENT
        """)

    copie = copier_dans_raw(telecharger_et_deposer(verifier_disponibilite(nom_du_fichier())))
    raw_mois_charge = SQLCheckOperator(
        task_id="raw_mois_charge",
        conn_id="snowflake_nyc_taxi",
        sql="controles/raw_mois_charge.sql",
        retries=0,
    )
    initialisation = SQLExecuteQueryOperator(
        task_id="initialisation",
        conn_id="snowflake_nyc_taxi",
        sql="00_tables.sql",
        split_statements=True,
    )

    with TaskGroup("staging") as staging:
        for name in ["codes_tlc", "stg_tlc__taxi_zones", "stg_tlc__yellow_trips"]:
            SQLExecuteQueryOperator(
                task_id=name,
                conn_id="snowflake_nyc_taxi",
                sql=f"staging/{name}.sql",
                split_statements=True,
            )

    with TaskGroup("intermediate") as intermediate:
        flagged = SQLExecuteQueryOperator(
            task_id="int_trips__flagged",
            conn_id="snowflake_nyc_taxi",
            sql="intermediate/int_trips__flagged.sql",
            split_statements=True,
        )
        taux_rejet = SQLCheckOperator(
            task_id="taux_rejet_acceptable",
            conn_id="snowflake_nyc_taxi",
            sql="controles/taux_rejet_acceptable.sql",
            retries=0,
        )
        enriched = SQLExecuteQueryOperator(
            task_id="int_trips__enriched",
            conn_id="snowflake_nyc_taxi",
            sql="intermediate/int_trips__enriched.sql",
            split_statements=True,
        )
        flagged >> taux_rejet >> enriched

    with TaskGroup("marts") as marts:
        dimensions = {}
        for name in ["dim_date", "dim_payment_type", "dim_rate_code", "dim_vendor", "dim_zone"]:
            dimensions[name] = SQLExecuteQueryOperator(
                task_id=name,
                conn_id="snowflake_nyc_taxi",
                sql=f"marts/{name}.sql",
                split_statements=True,
            )
        faits = SQLExecuteQueryOperator(
            task_id="fct_trips",
            conn_id="snowflake_nyc_taxi",
            sql="marts/fct_trips.sql",
            split_statements=True,
        )
        sans_doublon = SQLCheckOperator(
            task_id="fct_trips_sans_doublon",
            conn_id="snowflake_nyc_taxi",
            sql="controles/fct_trips_sans_doublon.sql",
            retries=0,
        )
        analyses = {}
        for name in ["mart_daily_revenue", "mart_zone_hourly_demand", "mart_data_quality"]:
            analyses[name] = SQLExecuteQueryOperator(
                task_id=name,
                conn_id="snowflake_nyc_taxi",
                sql=f"marts/{name}.sql",
                split_statements=True,
            )
        faits >> sans_doublon
        [sans_doublon, dimensions["dim_date"], dimensions["dim_payment_type"]] >> analyses["mart_daily_revenue"]
        [sans_doublon, dimensions["dim_zone"]] >> analyses["mart_zone_hourly_demand"]
        sans_doublon >> analyses["mart_data_quality"]

    copie >> raw_mois_charge >> initialisation >> staging >> intermediate >> marts


nyc_taxi_monthly()
