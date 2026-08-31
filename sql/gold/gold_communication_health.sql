CREATE OR REPLACE TABLE
  `YOUR_PROJECT.orbitalsense_gold.gold_communication_health`
PARTITION BY DATE(metric_date)
CLUSTER BY satellite_id
AS

SELECT
    DATE(event_timestamp) AS metric_date,

    satellite_id,

    COUNT(*) AS comm_readings,

    AVG(signal_strength_dbm) AS avg_signal_strength_dbm,

    MIN(signal_strength_dbm) AS min_signal_strength_dbm,

    MAX(signal_strength_dbm) AS max_signal_strength_dbm,

    AVG(bit_error_rate) AS avg_bit_error_rate,

    COUNTIF(comm_status = 'NOMINAL') AS nominal_count,

    COUNTIF(comm_status = 'DEGRADED') AS degraded_count,

    COUNTIF(comm_status = 'LOST') AS lost_count,

    SAFE_DIVIDE(
        COUNTIF(comm_status IN ('DEGRADED', 'LOST')),
        COUNT(*)
    ) AS degraded_or_lost_rate

FROM
    `YOUR_PROJECT.orbitalsense_silver.silver_comms`

GROUP BY
    metric_date,
    satellite_id;