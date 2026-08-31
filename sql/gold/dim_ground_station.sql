CREATE OR REPLACE TABLE
  `YOUR_PROJECT.orbitalsense_gold.dim_ground_station`
AS

SELECT
    ground_station_id,

    CASE ground_station_id
        WHEN 'GS-1' THEN 'Ground Station 1'
        WHEN 'GS-2' THEN 'Ground Station 2'
        WHEN 'GS-3' THEN 'Ground Station 3'
        WHEN 'GS-4' THEN 'Ground Station 4'
    END AS ground_station_name

FROM UNNEST([
    'GS-1',
    'GS-2',
    'GS-3',
    'GS-4'
]) AS ground_station_id;