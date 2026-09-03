CREATE OR REPLACE TABLE
  `orbitalsense-platform.orbitalsense_gold.dim_satellite`
AS

SELECT
    satellite_id,

    CASE
        WHEN satellite_id = 'SAT-07'
        THEN 'DROP_OUT_TEST_SATELLITE'

        WHEN satellite_id = 'SAT-11'
        THEN 'DEGRADED_COMMS_TEST_SATELLITE'

        ELSE 'NOMINAL'
    END AS mission_test_profile,

    CASE
        WHEN satellite_id = 'SAT-07'
        THEN TRUE
        ELSE FALSE
    END AS known_dropout_satellite,

    CASE
        WHEN satellite_id = 'SAT-11'
        THEN TRUE
        ELSE FALSE
    END AS known_degraded_comms_satellite

FROM UNNEST([
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
]) AS satellite_id;