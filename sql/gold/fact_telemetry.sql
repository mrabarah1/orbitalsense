CREATE OR REPLACE TABLE
  `orbitalsense-platform.orbitalsense_gold.fact_telemetry`
PARTITION BY DATE(event_timestamp)
CLUSTER BY satellite_id, subsystem, ground_station_id
AS

SELECT
    message_id,

    satellite_id,

    ground_station_id,

    subsystem,

    event_timestamp,

    received_timestamp,

    latency_ms,

    battery_voltage_v,
    battery_current_a,
    solar_output_w,

    internal_temp_c,
    external_temp_c,

    signal_strength_dbm,
    bit_error_rate,
    comm_status,

    latitude,
    longitude,
    altitude_km,
    velocity_kms,

    dedup_key,

    ingestion_timestamp,

    pipeline_version

FROM
  `orbitalsense-platform.orbitalsense_silver.silver_telemetry`;