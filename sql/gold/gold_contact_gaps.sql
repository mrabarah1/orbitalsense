CREATE OR REPLACE TABLE
  `YOUR_PROJECT.orbitalsense_gold.gold_contact_gaps`
AS

WITH ordered AS (

    SELECT
        satellite_id,

        event_timestamp,

        LAG(event_timestamp)
        OVER (
            PARTITION BY satellite_id
            ORDER BY event_timestamp
        ) AS previous_event_timestamp

    FROM
        `YOUR_PROJECT.orbitalsense_silver.silver_telemetry`
),

gaps AS (

    SELECT

        satellite_id,

        previous_event_timestamp AS gap_start,

        event_timestamp AS gap_end,

        TIMESTAMP_DIFF(
            event_timestamp,
            previous_event_timestamp,
            MINUTE
        ) AS gap_minutes

    FROM ordered

    WHERE
        previous_event_timestamp IS NOT NULL

        AND TIMESTAMP_DIFF(
            event_timestamp,
            previous_event_timestamp,
            MINUTE
        ) > 5
)

SELECT
    satellite_id,
    gap_start,
    gap_end,
    gap_minutes,

    CASE
        WHEN gap_minutes >= 360
        THEN 'MAJOR_CONTACT_GAP'

        WHEN gap_minutes >= 30
        THEN 'SIGNIFICANT_CONTACT_GAP'

        ELSE 'MINOR_CONTACT_GAP'
    END AS gap_severity

FROM gaps

ORDER BY
    gap_minutes DESC;