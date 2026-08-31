resource "google_bigquery_table" "gold_volume" {
  dataset_id = google_bigquery_dataset.gold.dataset_id
  table_id   = "v_satellite_telemetry_volume"

  deletion_protection = false

  view {
    query          = file("${path.module}/../sql/gold/01_satellite_telemetry_volume.sql")
    use_legacy_sql = false
  }
}

resource "google_bigquery_table" "gold_battery" {
  dataset_id = google_bigquery_dataset.gold.dataset_id
  table_id   = "v_battery_health"

  deletion_protection = false

  view {
    query          = file("${path.module}/../sql/gold/02_battery_health.sql")
    use_legacy_sql = false
  }
}

resource "google_bigquery_table" "gold_comms" {
  dataset_id = google_bigquery_dataset.gold.dataset_id
  table_id   = "v_communication_signal"

  deletion_protection = false

  view {
    query          = file("${path.module}/../sql/gold/03_communication_signal.sql")
    use_legacy_sql = false
  }
}

resource "google_bigquery_table" "gold_alerts" {
  dataset_id = google_bigquery_dataset.gold.dataset_id
  table_id   = "v_subsystem_alerts"

  deletion_protection = false

  view {
    query          = file("${path.module}/../sql/gold/04_subsystem_alerts_and_quarantine.sql")
    use_legacy_sql = false
  }
}