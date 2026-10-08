SELECT
    COALESCE(COUNT_IF(rejection_reason IS NOT NULL), 0) * 100.0
        / NULLIF(COUNT(*), 0) < {{ params.max_rejected_pct }}
FROM NYC_TAXI.INTERMEDIATE.INT_TRIPS__FLAGGED
WHERE source_file_month = '{{ ds }}'::date;
