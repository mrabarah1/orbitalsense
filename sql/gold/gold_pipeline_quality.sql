CREATE OR REPLACE TABLE
  `YOUR_PROJECT.orbitalsense_gold.gold_pipeline_quality`
AS

WITH curated AS (

    SELECT
        COUNT(*) AS curated_records,

        COUNT(DISTINCT dedup_key)
            AS unique_semantic_events,

        AVG(latency_ms)
            AS avg_latency_ms,

        MAX(latency_ms)
            AS max_latency_ms

    FROM
        `YOUR_PROJECT.orbitalsense_silver.silver_telemetry`
),

quarantine AS (

    SELECT
        COUNT(*) AS quarantined_records

    FROM
        `YOUR_PROJECT.orbitalsense_raw.quarantine`
),

raw AS (

    SELECT
        COUNT(*) AS raw_records

    FROM
        `YOUR_PROJECT.orbitalsense_raw.raw_telemetry`
)

SELECT

    raw.raw_records,

    curated.curated_records,

    quarantine.quarantined_records,

    curated.unique_semantic_events,

    curated.avg_latency_ms,

    curated.max_latency_ms,

    SAFE_DIVIDE(
        quarantine.quarantined_records,
        raw.raw_records
    ) AS quarantine_rate,

    SAFE_DIVIDE(
        curated.curated_records,
        raw.raw_records
    ) AS curated_rate

FROM raw

CROSS JOIN curated

CROSS JOIN quarantine;