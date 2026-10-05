from pathlib import Path

import pendulum
import requests
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.sdk import dag, get_current_context, task


@dag(
    dag_id="nyc_taxi_monthly",
    schedule="@monthly",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    end_date=pendulum.datetime(2025, 3, 1, tz="UTC"),
    catchup=True,
    max_active_runs=1,
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

    copier_dans_raw(telecharger_et_deposer(verifier_disponibilite(nom_du_fichier())))


nyc_taxi_monthly()
