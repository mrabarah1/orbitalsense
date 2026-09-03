CREATE OR REPLACE TABLE
  `orbitalsense-platform.orbitalsense_gold.gold_data_completeness`
AS

WITH expected AS (

    SELECT
        satellite_id,

        DATE(event_timestamp) AS event_date,

        COUNT(*) AS expected_readings

    FROM (

        SELECT
            satellite_id,

            timestamp AS event_timestamp

        FROM UNNEST(
            GENERATE_TIMESTAMP_ARRAY(
                TIMESTAMP('2026-08-01 00:00:00+00'),
                TIMESTAMP('2026-08-14 23:55:00+00'),
                INTERVAL 5 MINUTE
            )
        ) AS timestamp

        CROSS JOIN UNNEST([
            'SAT-01',
            'SAT-02',
            'SAT-03',
            'SAT-04',
            'SAT-05',
            'SAT-06',
            'SAT-07',
            'SAT-08',
            'SAT-09',
            'SAT-10',
            'SAT-11',
            'SAT-12'
        ]) AS satellite_id

    )

    GROUP BY
        satellite_id,
        event_date
),

actual AS (

    SELECT

        satellite_id,

        DATE(event_timestamp) AS event_date,

        COUNT(*) AS actual_readings

    FROM
        `orbitalsense-platform.orbitalsense_silver.silver_telemetry`

    GROUP BY
        satellite_id,
        event_date
)

SELECT

    expected.satellite_id,

    expected.event_date,

    expected.expected_readings,

    COALESCE(
        actual.actual_readings,
        0
    ) AS actual_readings,

    expected.expected_readings
      - COALESCE(actual.actual_readings, 0)
      AS missing_readings,

    SAFE_DIVIDE(
        COALESCE(actual.actual_readings, 0),
        expected.expected_readings
    ) AS completeness_rate

FROM expected

LEFT JOIN actual

    ON expected.satellite_id = actual.satellite_id

    AND expected.event_date = actual.event_date

ORDER BY
    event_date,
    satellite_id;