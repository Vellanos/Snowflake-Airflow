-- Exécuter les sections séparément dans Snowsight.

-- A. Volumes finaux — TRANSFORMER
USE ROLE TRANSFORMER;
USE SECONDARY ROLES NONE;
USE WAREHOUSE NYC_TAXI_WH;
USE DATABASE NYC_TAXI;

SELECT 'RAW.YELLOW_TRIPDATA' AS table_name, COUNT(*) AS nb_rows, 11198026 AS expected_rows
FROM NYC_TAXI.RAW.YELLOW_TRIPDATA
UNION ALL
SELECT 'RAW.TAXI_ZONE_LOOKUP', COUNT(*), 265 FROM NYC_TAXI.RAW.TAXI_ZONE_LOOKUP
UNION ALL
SELECT 'INTERMEDIATE.INT_TRIPS__FLAGGED', COUNT(*), 11198026 FROM NYC_TAXI.INTERMEDIATE.INT_TRIPS__FLAGGED
UNION ALL
SELECT 'MARTS.FCT_TRIPS', COUNT(*), 10382378 FROM NYC_TAXI.MARTS.FCT_TRIPS
UNION ALL
SELECT 'MARTS.MART_ZONE_HOURLY_DEMAND', COUNT(*), 11524 FROM NYC_TAXI.MARTS.MART_ZONE_HOURLY_DEMAND
UNION ALL
SELECT 'MARTS.MART_DATA_QUALITY', COUNT(*), 18 FROM NYC_TAXI.MARTS.MART_DATA_QUALITY;

SELECT COUNT(*) = COUNT(DISTINCT trip_sk) AS no_duplicate
FROM NYC_TAXI.MARTS.FCT_TRIPS;

-- B. Droits du rôle et du service — TRANSFORMER
SHOW GRANTS TO ROLE TRANSFORMER;
SHOW GRANTS OF ROLE TRANSFORMER;
SHOW GRANTS TO USER AIRFLOW_SVC;

-- C. Accès refusé — sélectionner ce bloc seul, jamais avec ACCOUNTADMIN
USE ROLE TRANSFORMER;
USE SECONDARY ROLES NONE;
SELECT CURRENT_ROLE(), CURRENT_SECONDARY_ROLES();
CREATE SCHEMA NYC_TAXI.TEST_INTERDIT;
-- Échec attendu. En cas de réussite : DROP SCHEMA NYC_TAXI.TEST_INTERDIT;
-- Supprimer avec son propriétaire, puis arrêter : droits non conformes.

-- D. Historique COPY des trois mois — TRANSFORMER
USE ROLE TRANSFORMER;
USE SECONDARY ROLES NONE;
USE DATABASE NYC_TAXI;
SELECT file_name, last_load_time, status, row_count, row_parsed, error_count
FROM TABLE(NYC_TAXI.INFORMATION_SCHEMA.COPY_HISTORY(
    TABLE_NAME => 'NYC_TAXI.RAW.YELLOW_TRIPDATA',
    START_TIME => DATEADD('day', -14, CURRENT_TIMESTAMP())
))
WHERE REGEXP_LIKE(file_name, '.*yellow_tripdata_2025-0[123][.]parquet')
ORDER BY last_load_time DESC, file_name;

-- E. Crédits — feuille séparée, compte d'administration dans Snowsight
USE ROLE ACCOUNTADMIN;
-- Historique disponible du warehouse : 365 jours, selon ACCOUNT_USAGE.
-- Données différées : jusqu'à 3 h, et 6 h pour les cloud services.
-- Crédits mesurés avant les ajustements de facturation cloud services.
SELECT
    warehouse_name,
    MIN(start_time) AS first_metered_hour,
    MAX(end_time) AS last_metered_hour,
    SUM(credits_used) AS total_credits,
    SUM(credits_used_compute) AS compute_credits,
    SUM(credits_used_cloud_services) AS cloud_services_credits
FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
WHERE warehouse_name = 'NYC_TAXI_WH'
  AND start_time >= DATEADD('day', -365, CURRENT_TIMESTAMP())
  AND start_time < CURRENT_TIMESTAMP()
GROUP BY warehouse_name;

-- F. Anomalies janvier : conditions indépendantes, puis catégories exclusives
USE ROLE TRANSFORMER;
USE SECONDARY ROLES NONE;
USE WAREHOUSE NYC_TAXI_WH;
SELECT
    COUNT(*) AS nb_rows,
    COALESCE(COUNT_IF(pickup_at IS NULL OR dropoff_at IS NULL), 0) AS timestamp_null,
    COALESCE(COUNT_IF(dropoff_at <= pickup_at), 0) AS duration_non_positive,
    COALESCE(COUNT_IF(DATEDIFF('second', pickup_at, dropoff_at) > 180 * 60), 0) AS duration_too_long,
    COALESCE(COUNT_IF(DATE_TRUNC('month', pickup_at) <> source_file_month), 0) AS pickup_outside_file_month,
    COALESCE(COUNT_IF(trip_distance_miles <= 0 OR trip_distance_miles > 100), 0) AS distance_out_of_range,
    COALESCE(COUNT_IF(fare_amount < 0 OR total_amount <= 0), 0) AS amount_non_positive,
    COALESCE(COUNT_IF(pickup_zone_key IS NULL OR dropoff_zone_key IS NULL), 0) AS zone_null
FROM NYC_TAXI.STAGING.STG_TLC__YELLOW_TRIPS
WHERE source_file_month = '2025-01-01'::date;

SELECT status, nb_rows, pct_of_file
FROM NYC_TAXI.MARTS.MART_DATA_QUALITY
WHERE source_file = 'yellow_tripdata_2025-01.parquet'
ORDER BY status;

-- G. Réponse à la direction — zone × heure × paiement
SELECT
    COALESCE(z.zone_name, 'Inconnue') AS pickup_zone,
    COALESCE(z.borough, 'Inconnu') AS borough,
    f.pickup_hour,
    COALESCE(p.payment_type_label, 'Inconnu') AS payment_type,
    COUNT(*) AS nb_trips,
    ROUND(SUM(f.total_amount), 2) AS total_revenue,
    ROUND(AVG(f.total_amount), 2) AS avg_revenue_per_trip
FROM NYC_TAXI.MARTS.FCT_TRIPS f
LEFT JOIN NYC_TAXI.MARTS.DIM_ZONE z
    ON z.zone_key = f.pickup_zone_key
LEFT JOIN NYC_TAXI.MARTS.DIM_PAYMENT_TYPE p
    ON p.payment_type_key = f.payment_type_key
WHERE f.source_file_month >= '2025-01-01'::date
  AND f.source_file_month < '2025-04-01'::date
GROUP BY f.pickup_zone_key, z.zone_name, z.borough,
         f.pickup_hour, f.payment_type_key, p.payment_type_label
ORDER BY nb_trips DESC, f.pickup_zone_key, f.pickup_hour, f.payment_type_key
LIMIT 10;
