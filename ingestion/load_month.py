import argparse
import os
import re
from datetime import datetime
from pathlib import Path
from tempfile import TemporaryDirectory

import requests
import snowflake.connector
from cryptography.hazmat.primitives import serialization


def valid_month(value):
    if not re.fullmatch(r"[0-9]{4}-(0[1-9]|1[0-2])", value):
        raise argparse.ArgumentTypeError("Mois attendu : YYYY-MM")
    try:
        datetime.strptime(value, "%Y-%m")
    except ValueError as exc:
        raise argparse.ArgumentTypeError("Mois invalide") from exc
    return value


def main():
    parser = argparse.ArgumentParser(description="Chargement mensuel Yellow Taxi dans RAW")
    parser.add_argument("month", type=valid_month)
    month = parser.parse_args().month
    filename = f"yellow_tripdata_{month}.parquet"
    url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{filename}"
    print(f"Mois : {month}\nFichier : {filename}", flush=True)

    with TemporaryDirectory(prefix="nyc-taxi-", dir="/tmp") as folder:
        path = Path(folder) / filename
        with requests.get(url, stream=True, timeout=(10, 120)) as response:
            response.raise_for_status()
            with path.open("wb") as output:
                for chunk in response.iter_content(chunk_size=8 * 1024 * 1024):
                    output.write(chunk)

        key = serialization.load_pem_private_key(
            Path("~/.ssh/snowflake/rsa_key.p8").expanduser().read_bytes(), password=None
        )
        with snowflake.connector.connect(
            account=os.environ["SNOWFLAKE_ACCOUNT"],
            user="AIRFLOW_SVC",
            private_key=key,
            role="TRANSFORMER",
            warehouse="NYC_TAXI_WH",
            database="NYC_TAXI",
            schema="RAW",
        ) as conn:
            with conn.cursor() as cur:
                cur.execute("USE SECONDARY ROLES NONE")
                cur.execute(
                    f"PUT 'file://{path}' @NYC_TAXI.RAW.TLC_STAGE "
                    "AUTO_COMPRESS=FALSE OVERWRITE=FALSE"
                )
                for row in cur.fetchall():
                    print(f"PUT : {row[6]}", flush=True)
                cur.execute(f"""
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
                columns = [column[0].lower() for column in cur.description]
                results = [dict(zip(columns, row)) for row in cur.fetchall()]
                loaded = sum(int(row.get("rows_loaded") or 0) for row in results)
                if loaded:
                    print(f"COPY : {loaded} lignes chargées", flush=True)
                else:
                    print("COPY : chargement ignoré, aucune ligne ajoutée", flush=True)
                total = cur.execute(
                    "SELECT COUNT(*) FROM NYC_TAXI.RAW.YELLOW_TRIPDATA WHERE _source_file = %s",
                    (filename,),
                ).fetchone()[0]
                print(f"Total pour {filename} : {total}", flush=True)


if __name__ == "__main__":
    main()
