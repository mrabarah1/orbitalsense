

CREATE SCHEMA IF NOT EXISTS `YOUR_PROJECT.orbitalsense_raw`
OPTIONS (location = "us-central1");

CREATE SCHEMA IF NOT EXISTS `YOUR_PROJECT.orbitalsense_silver`
OPTIONS (location = "us-central1");

CREATE SCHEMA IF NOT EXISTS `YOUR_PROJECT.orbitalsense_gold`
OPTIONS (location = "us-central1");


# bq query --use_legacy_sql=false < sql/00_create_datasets.sql