CREATE OR REPLACE TABLE
  `orbitalsense-platform.orbitalsense_silver.silver_thermal`
PARTITION BY DATE(event_timestamp)
CLUSTER BY satellite_id, ground_station_id
AS

SELECT
    message_id,
    satellite_id,
    ground_station_id,
    event_timestamp,
    received_timestamp,

    internal_temp_c,
    external_temp_c,

    latency_ms,
    dedup_key,
    ingestion_timestamp,
    pipeline_version

FROM
  `orbitalsense-platform.orbitalsense_silver.silver_telemetry`

WHERE
    subsystem = 'THERMAL';