
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

resource "google_bigquery_dataset" "gold" {
  dataset_id = "orbitalsense_gold"
  location = var.region
}

# Raw BigQuery Table
resource "google_bigquery_table" "raw_telemetry" {
  dataset_id = google_bigquery_dataset.raw.dataset_id
  table_id = "telemetry_raw"

  deletion_protection = false

  time_partitioning {
    type = "DAY"
    field = "ingestion_timestamp"
  }

  clustering = [
    "satellite_id",
    "ground_station_id",
    "subsystem"
  ]

  schema = <<EOF
[
  {"name":"raw_payload","type":"STRING","mode":"REQUIRED"},
  {"name":"message_id","type":"STRING","mode":"NULLABLE"},
  {"name":"satellite_id","type":"STRING","mode":"NULLABLE"},
  {"name":"ground_station_id","type":"STRING","mode":"NULLABLE"},
  {"name":"subsystem","type":"STRING","mode":"NULLABLE"},
  {"name":"ingestion_timestamp","type":"TIMESTAMP","mode":"REQUIRED"},
  {"name":"pipeline_version","type":"STRING","mode":"REQUIRED"},
  {"name":"source","type":"STRING","mode":"REQUIRED"}
]
EOF

  depends_on = [
    google_project_service.services
  ]
}



# Quarantine BigQuery Table
resource "google_bigquery_table" "quarantine" {
  dataset_id = google_bigquery_dataset.quarantine.dataset_id
  table_id = "telemetry_quarantine"

  deletion_protection = false

  time_partitioning {
    type = "DAY"
    field = "quarantine_at"
  }

  clustering = [
    "satellite_id",
    "reason_code"
  ]

 schema = <<EOF
[
  {"name":"message_id","type":"STRING","mode":"NULLABLE"},
  {"name":"satellite_id","type":"STRING","mode":"NULLABLE"},
  {"name":"ground_station_id","type":"STRING","mode":"NULLABLE"},
  {"name":"subsystem","type":"STRING","mode":"NULLABLE"},
  {"name":"event_timestamp","type":"TIMESTAMP","mode":"NULLABLE"},
  {"name":"reason_code","type":"STRING","mode":"REQUIRED"},
  {"name":"reason_detail","type":"STRING","mode":"NULLABLE"},
  {"name":"quarantined_at","type":"TIMESTAMP","mode":"REQUIRED"},
  {"name":"pipeline_version","type":"STRING","mode":"REQUIRED"},
  {"name":"raw_payload","type":"JSON","mode":"NULLABLE"}
]
EOF
}



# BigQuery Curated Table
resource "google_bigquery_table" "curated" {
  dataset_id = google_bigquery_dataset.curated.dataset_id
  table_id   = "telemetry_curated"

  deletion_protection = false 

  time_partitioning {
    type  = "DAY"
    field = "ingestion_timestamp"
  }

  clustering = [
    "satellite_id",
    "ground_station_id",
    "subsystem"
  ]

  schema = <<EOF
[
  {"name":"message_id","type":"STRING","mode":"REQUIRED"},
  {"name":"dedup_key","type":"STRING","mode":"REQUIRED"},
  {"name":"satellite_id","type":"STRING","mode":"REQUIRED"},
  {"name":"ground_station_id","type":"STRING","mode":"REQUIRED"},
  {"name":"subsystem","type":"STRING","mode":"REQUIRED"},

  {"name":"event_timestamp","type":"TIMESTAMP","mode":"REQUIRED"},
  {"name":"received_timestamp","type":"TIMESTAMP","mode":"REQUIRED"},

  {"name":"battery_voltage_v","type":"FLOAT","mode":"NULLABLE"},
  {"name":"battery_current_a","type":"FLOAT","mode":"NULLABLE"},
  {"name":"solar_output_w","type":"FLOAT","mode":"NULLABLE"},

  {"name":"internal_temp_c","type":"FLOAT","mode":"NULLABLE"},
  {"name":"external_temp_c","type":"FLOAT","mode":"NULLABLE"},

  {"name":"signal_strength_dbm","type":"FLOAT","mode":"NULLABLE"},
  {"name":"bit_error_rate","type":"FLOAT","mode":"NULLABLE"},
  {"name":"comm_status","type":"STRING","mode":"NULLABLE"},

  {"name":"latitude","type":"FLOAT","mode":"NULLABLE"},
  {"name":"longitude","type":"FLOAT","mode":"NULLABLE"},
  {"name":"altitude_km","type":"FLOAT","mode":"NULLABLE"},
  {"name":"velocity_kms","type":"FLOAT","mode":"NULLABLE"},

  {"name":"alert_flag","type":"BOOLEAN","mode":"REQUIRED"},
  {"name":"alert_code","type":"STRING","mode":"NULLABLE"},

  {"name":"ingestion_timestamp","type":"TIMESTAMP","mode":"REQUIRED"},
  {"name":"pipeline_version","type":"STRING","mode":"REQUIRED"},
  {"name":"source_ground_station","type":"STRING","mode":"REQUIRED"}
]
EOF
}





# resource "google_bigquery_table" "satellite_summary" {
#   dataset_id = google_bigquery_dataset.gold.dataset_id
#   table_id   = "satellite_telemetry_summary"

#   schema = <<EOF
# [
#   {"name":"satellite_id","type":"STRING","mode":"REQUIRED"},
#   {"name":"telemetry_count","type":"INT64","mode":"REQUIRED"},
#   {"name":"ground_station_count","type":"INT64","mode":"REQUIRED"},
#   {"name":"first_event_timestamp","type":"TIMESTAMP"},
#   {"name":"last_event_timestamp","type":"TIMESTAMP"}
# ]
# EOF
# }