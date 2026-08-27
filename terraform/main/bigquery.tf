
#BigQuery Datasets - 6

resource "google_bigquery_dataset" "raw" {
  dataset_id = "orbitalsense_raw"
  location = var.region
}

resource "google_bigquery_dataset" "curated" {
  dataset_id = "orbitalsense_curated"
  location = var.region
}

resource "google_bigquery_dataset" "quarantine" {
  dataset_id = "orbitalsense_quarantine"
  location = var.region
}

# Raw BigQuery Table
resource "google_bigquery_table" "raw_telemetry" {
  dataset_id = google_bigquery_dataset.raw.dataset_id
  table_id = "telemetry_raw"

  time_partitioning {
    type = "DAY"
    field = "ingestion_timestamp"
  }

  clustering = [
    "satellite_id",
    "ground_station_id",
    "subsystem"
  ]

  schema = <<E0F
  [
    {"name":"message_id","type":"STRING","mode":"REQUIRED"},
    {"name":"satellite_id","type":"STRING","mode":"REQUIRED"},
    {"name":"ground_station_id","type":"STRING","mode":"REQUIRED"},
    {"name":"subsystem","type":"STRING","mode":"REQUIRED"},
    {"name":"event_timestamp","type":"TIMESTAMP","mode":"REQUIRED"},
    {"name":"received_timestamp","type":"TIMESTAMP","mode":"REQUIRED"},
    {"name":"battery_voltage_v","type":"FLOAT"},
    {"name":"battery_current_a","type":"FLOAT"},
    {"name":"solar_output_w","type":"FLOAT"},
    {"name":"internal_temp_c","type":"FLOAT"},
    {"name":"external_temp_c","type":"FLOAT"},
    {"name":"signal_strength_dbm","type":"FLOAT"},
    {"name":"bit_error_rate","type":"FLOAT"},
    {"name":"comm_status","type":"STRING"},
    {"name":"latitude","type":"FLOAT"},
    {"name":"longitude","type":"FLOAT"},
    {"name":"altitude_km","type":"FLOAT"},
    {"name":"velocity_kms","type":"FLOAT"},
    {"name":"is_malformed","type":"BOOL"},
    {"name":"malformed_reason","type":"STRING"},
    {"name":"is_duplicate","type":"BOOL"},
    {"name":"duplicate_of_message_id","type":"STRING"},
    {"name":"ingestion_timestamp","type":"TIMESTAMP"},
    {"name":"pipeline_version","type":"STRING"},
    {"name":"source","type":"STRING"}
  ]
  E0F
}



# Quarantine BigQuery Table
resource "google_bigquery_table" "quarantine" {
  dataset_id = google_bigquery_dataset.quarantine.dataset_id
  table_id = "telemetry_quarantine"

  time_partitioning {
    type = "DAY"
    field = "quarantine_at"
  }

  clustering = [
    "satellite_id",
    "reason_code"
  ]

  schema = <<E0F
  [
    {"name":"message_id","type":"STRING"},
    {"name":"satellite_id","type":"STRING"},
    {"name":"ground_station_id","type":"STRING"},
    {"name":"subsystem","type":"STRING"},
    {"name":"event_timestamp","type":"TIMESTAMP"},
    {"name":"reason_code","type":"STRING"},
    {"name":"reason_detail","type":"STRING"},
    {"name":"quarantined_at","type":"TIMESTAMP"},
    {"name":"pipeline_version","type":"STRING"},
    {"name":"raw_payload","type":"JSON"}
  ]
  E0F
}

# BigQuery Curated Table
resource "google_bigquery_table" "curated" {
  dataset_id = google_bigquery_dataset.curated.dataset_id
  table_id = "telemetry_curated"

  time_partitioning {
    type = "DAY"
    field = "event_timestamp"
  }

  clustering = [
    "satellite_id",
    "ground_station_id",
    "subsystem"
  ]

  schema = <<E0F
  [
    {"name":"message_id","type":"STRING","mode":"REQUIRED"},
    {"name":"dedup_key","type":"STRING","mode":"REQUIRED"},
    {"name":"satellite_id","type":"STRING","mode":"REQUIRED"},
    {"name":"ground_station_id","type":"STRING","mode":"REQUIRED"},
    {"name":"subsystem","type":"STRING","mode":"REQUIRED"},
    {"name":"event_timestamp","type":"TIMESTAMP","mode":"REQUIRED"},
    {"name":"received_timestamp","type":"TIMESTAMP","mode":"REQUIRED"},
    {"name":"battery_voltage_v","type":"FLOAT"},
    {"name":"battery_current_a","type":"FLOAT"},
    {"name":"solar_output_w","type":"FLOAT"},
    {"name":"internal_temp_c","type":"FLOAT"},
    {"name":"external_temp_c","type":"FLOAT"},
    {"name":"signal_strength_dbm","type":"FLOAT"},
    {"name":"bit_error_rate","type":"FLOAT"},
    {"name":"comm_status","type":"STRING"},
    {"name":"latitude","type":"FLOAT"},
    {"name":"longitude","type":"FLOAT"},
    {"name":"altitude_km","type":"FLOAT"},
    {"name":"velocity_kms","type":"FLOAT"},
    {"name":"ingestion_timestamp","type":"TIMESTAMP","mode":"REQUIRED"},
    {"name":"pipeline_version","type":"STRING","mode":"REQUIRED"},
    {"name":"source_ground_station","type":"STRING","mode":"REQUIRED"}
  ]
  E0F
}