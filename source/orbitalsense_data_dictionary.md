# OrbitalSense Telemetry Dataset: Data Dictionary

`orbitalsense_telemetry_raw.csv` — 199,077 rows, ~30 MB. Simulates 14 days of raw telemetry (August 1–15, 2026) from 12 satellites relayed through 4 ground stations, before validation or curation. This is the "as received" data a producer would publish; use it to seed Pub/Sub messages, load a raw BigQuery table directly, or replay at controlled speed for testing.

## Columns

| Column | Type | Notes |
|---|---|---|
| message_id | string (UUID) | Unique per message, including duplicates |
| satellite_id | string | SAT-01 through SAT-12 |
| ground_station_id | string | GS-1 through GS-4; rotates per satellite and tick, so every station carries traffic from every satellite |
| subsystem | string | POWER, THERMAL, COMMS, or ORBITAL — only that subsystem's columns are populated per row |
| event_timestamp | ISO 8601 | When the satellite generated the reading |
| received_timestamp | ISO 8601 | When the ground station received it, normally 1–8 seconds after event_timestamp |
| battery_voltage_v | float | POWER only |
| battery_current_a | float | POWER only |
| solar_output_w | float | POWER only |
| internal_temp_c | float | THERMAL only |
| external_temp_c | float | THERMAL only |
| signal_strength_dbm | float | COMMS only |
| bit_error_rate | float | COMMS only |
| comm_status | string | COMMS only — NOMINAL, DEGRADED, or LOST |
| latitude / longitude | float | ORBITAL only |
| altitude_km | float | ORBITAL only |
| velocity_kms | float | ORBITAL only |
| is_malformed | bool | true for injected bad records (see below) |
| malformed_reason | string | Populated when is_malformed is true |
| is_duplicate | bool | true for injected duplicate records |
| duplicate_of_message_id | string | Points to the original message_id when is_duplicate is true |

## What's deliberately baked in

- **Malformed records (~2%):** out-of-physical-range values (negative voltage, impossible altitude, positive dBm signal), missing required fields, timestamps in the future, and non-numeric values in a numeric field. `malformed_reason` documents which, but a validation pipeline should catch these from the data itself, not from that column — it's there for grading/answer-key purposes, not for the pipeline to read.
- **Duplicate records (~3%):** exact content duplicates with a new message_id and a slightly later received_timestamp, simulating Pub/Sub's at-least-once redelivery.
- **A dropout window:** SAT-07 has zero rows of any subsystem between 2026-08-06 14:20 UTC and 20:20 UTC (a 6-hour silence), simulating loss of contact.
- **A degraded-comms satellite:** SAT-11's COMMS readings skew toward weak signal strength and DEGRADED/LOST status far more often than the other 11 satellites, so a "weakest communication signal" query has a clear, findable answer.
- **Rows arrive shuffled**, not in generation order, since ground stations relay whatever they currently have in view — a pipeline that assumes strict arrival order will misbehave on this data, which is the point.

## Companion file

`orbitalsense_dataset_summary.json` — exact counts, the dropout window, and which satellite is degraded, for use as an answer key when checking whether a team's analytics queries actually found these patterns.

## Regenerating with different parameters

The generator (`generate_telemetry.py`) is seeded (`random.seed(42)`) for reproducibility. To produce a different variant, for example a distinct dataset per team as described in the project spec's Section 6.2, change the seed and the `DROPOUT_SATELLITE` / `DEGRADED_SATELLITE` constants before rerunning.
