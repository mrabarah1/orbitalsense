CREATE OR REPLACE TABLE
  `YOUR_PROJECT.orbitalsense_silver.silver_telemetry`
PARTITION BY DATE(event_timestamp)
CLUSTER BY satellite_id, subsystem, ground_station_id
AS

SELECT
    message_id,
    satellite_id,
    ground_station_id,
    subsystem,

    TIMESTAMP(event_timestamp) AS event_timestamp,
    TIMESTAMP(received_timestamp) AS received_timestamp,

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

    TIMESTAMP(ingestion_timestamp) AS ingestion_timestamp,

    pipeline_version,
    source_ground_station,

    TIMESTAMP_DIFF(
        TIMESTAMP(received_timestamp),
        TIMESTAMP(event_timestamp),
        MILLISECOND
    ) AS latency_ms

FROM
  `YOUR_PROJECT.orbitalsense_raw.curated_telemetry`

WHERE
    satellite_id IS NOT NULL
    AND subsystem IS NOT NULL
    AND event_timestamp IS NOT NULL
    AND received_timestamp IS NOT NULL;