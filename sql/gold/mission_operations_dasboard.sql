SELECT

    h.metric_date,

    h.satellite_id,

    h.telemetry_rows,

    ROUND(
        h.daily_completeness_rate * 100,
        2
    ) AS completeness_pct,

    ROUND(
        h.avg_battery_voltage_v,
        3
    ) AS avg_battery_voltage_v,

    ROUND(
        h.avg_internal_temp_c,
        2
    ) AS avg_internal_temp_c,

    ROUND(
        h.avg_signal_strength_dbm,
        2
    ) AS avg_signal_strength_dbm,

    ROUND(
        h.avg_bit_error_rate,
        6
    ) AS avg_bit_error_rate,

    h.degraded_comms_count,

    h.lost_comms_count,

    ROUND(
        h.communication_issue_rate * 100,
        2
    ) AS communication_issue_pct,

    ROUND(
        h.avg_latency_ms,
        2
    ) AS avg_latency_ms,

    ROUND(
        h.max_latency_ms,
        2
    ) AS max_latency_ms

FROM
    `YOUR_PROJECT.orbitalsense_gold.gold_satellite_health` h

ORDER BY
    h.metric_date,
    h.satellite_id;