CREATE OR REPLACE TABLE
  `orbitalsense-platform.orbitalsense_gold.gold_data_quality`
AS

SELECT

    DATE(quarantined_at) AS quarantine_date,

    reason_code,

    COUNT(*) AS rejected_records

FROM
    `orbitalsense-platform.orbitalsense_raw.quarantine`

GROUP BY
    quarantine_date,
    reason_code

ORDER BY
    quarantine_date,
    rejected_records DESC;