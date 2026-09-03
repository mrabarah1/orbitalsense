

CREATE SCHEMA IF NOT EXISTS `orbitalsense-platform.orbitalsense_raw`
OPTIONS (location = "us-central1");

CREATE SCHEMA IF NOT EXISTS `orbitalsense-platform.orbitalsense_silver`
OPTIONS (location = "us-central1");

CREATE SCHEMA IF NOT EXISTS `orbitalsense-platform.orbitalsense_gold`
OPTIONS (location = "us-central1");


# bq query --use_legacy_sql=false < sql/00_create_datasets.sql