CREATE OR REPLACE TABLE
  `YOUR_PROJECT.orbitalsense_silver.silver_power`
PARTITION BY DATE(event_timestamp)
CLUSTER BY satellite_id, ground_station_id
AS

SELECT
    message_id,
    satellite_id,
    ground_station_id,
    event_timestamp,
    received_timestamp,

    battery_voltage_v,
    battery_current_a,
    solar_output_w,

    latency_ms,
    dedup_key,
    ingestion_timestamp,
    pipeline_version

FROM
  `YOUR_PROJECT.orbitalsense_silver.silver_telemetry`

WHERE
    subsystem = 'POWER';