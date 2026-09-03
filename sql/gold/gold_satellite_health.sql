CREATE OR REPLACE TABLE
  `orbitalsense-platform.orbitalsense_gold.gold_satellite_health`
PARTITION BY DATE(metric_date)
CLUSTER BY satellite_id
AS

WITH daily AS (

    SELECT
        DATE(event_timestamp) AS metric_date,
        satellite_id,

        COUNT(*) AS telemetry_rows,

        COUNTIF(subsystem = 'POWER') AS power_readings,

        COUNTIF(subsystem = 'THERMAL') AS thermal_readings,

        COUNTIF(subsystem = 'COMMS') AS comms_readings,

        COUNTIF(subsystem = 'ORBITAL') AS orbital_readings,

        AVG(
            IF(
                subsystem = 'POWER',
                battery_voltage_v,
                NULL
            )
        ) AS avg_battery_voltage_v,

        AVG(
            IF(
                subsystem = 'THERMAL',
                internal_temp_c,
                NULL
            )
        ) AS avg_internal_temp_c,

        AVG(
            IF(
                subsystem = 'COMMS',
                signal_strength_dbm,
                NULL
            )
        ) AS avg_signal_strength_dbm,

        AVG(
            IF(
                subsystem = 'COMMS',
                bit_error_rate,
                NULL
            )
        ) AS avg_bit_error_rate,

        COUNTIF(
            subsystem = 'COMMS'
            AND comm_status = 'DEGRADED'
        ) AS degraded_comms_count,

        COUNTIF(
            subsystem = 'COMMS'
            AND comm_status = 'LOST'
        ) AS lost_comms_count,

        AVG(latency_ms) AS avg_latency_ms,

        MAX(latency_ms) AS max_latency_ms

    FROM
        `orbitalsense-platform.orbitalsense_silver.silver_telemetry`

    GROUP BY
        metric_date,
        satellite_id
)

SELECT
    *,

    SAFE_DIVIDE(
        degraded_comms_count + lost_comms_count,
        comms_readings
    ) AS communication_issue_rate,

    SAFE_DIVIDE(
        telemetry_rows,
        4 * 288
    ) AS daily_completeness_rate

FROM daily;