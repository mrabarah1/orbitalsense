SELECT
    satellite_id,

    ROUND(
        AVG(avg_signal_strength_dbm),
        2
    ) AS avg_signal_strength_dbm,

    ROUND(
        AVG(avg_bit_error_rate),
        6
    ) AS avg_bit_error_rate,

    SUM(degraded_count) AS degraded_count,

    SUM(lost_count) AS lost_count,

    ROUND(
        SAFE_DIVIDE(
            SUM(degraded_count + lost_count),
            SUM(comm_readings)
        ),
        4
    ) AS degraded_or_lost_rate

FROM
    `orbitalsense-platform.orbitalsense_gold.gold_communication_health`

GROUP BY
    satellite_id

ORDER BY
    avg_signal_strength_dbm ASC

LIMIT 1;