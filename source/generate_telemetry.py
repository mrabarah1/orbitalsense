import random
import uuid
import csv
import json
from datetime import datetime, timedelta, timezone

random.seed(42)

SATELLITES = [f"SAT-{i:02d}" for i in range(1, 13)]
GROUND_STATIONS = ["GS-1", "GS-2", "GS-3", "GS-4"]
SUBSYSTEMS = ["POWER", "THERMAL", "COMMS", "ORBITAL"]

# 14 days of history, one reading per subsystem per satellite every 5 minutes
START = datetime(2026, 8, 1, 0, 0, 0, tzinfo=timezone.utc)
DAYS = 14
INTERVAL_MIN = 5
TOTAL_TICKS = int(DAYS * 24 * 60 / INTERVAL_MIN)

# One satellite goes silent for a stretch (simulates a dropout window)
DROPOUT_SATELLITE = "SAT-07"
DROPOUT_START_TICK = int(TOTAL_TICKS * 0.4)          # roughly day 6
DROPOUT_DURATION_TICKS = int(6 * 60 / INTERVAL_MIN)   # ~6 hours of silence

# A second satellite has patchy, degraded comms (frequent weak signal / lost status)
DEGRADED_SATELLITE = "SAT-11"

MALFORMED_RATE = 0.02
DUPLICATE_RATE = 0.03

def ground_station_for(satellite_idx, tick):
    # Rotates which station is in view, so any station can carry any satellite's traffic
    return GROUND_STATIONS[(satellite_idx + tick) % len(GROUND_STATIONS)]

def base_orbit(satellite_idx, tick):
    # crude circular-ish orbit so lat/lon/alt/velocity look plausible and vary smoothly
    phase = (tick * 0.045 + satellite_idx * 0.7) % (2 * 3.14159265)
    lat = 51.6 * random.uniform(-1, 1) * abs(__import__("math").sin(phase))
    lon = ((tick * 1.5 + satellite_idx * 30) % 360) - 180
    alt = 540 + random.uniform(-8, 8) + (satellite_idx % 3) * 5
    vel = 7.6 + random.uniform(-0.05, 0.05)
    return round(lat, 5), round(lon, 5), round(alt, 2), round(vel, 3)

def make_reading(sat_idx, satellite_id, subsystem, event_time, tick):
    row = {
        "message_id": uuid.uuid4().hex,
        "satellite_id": satellite_id,
        "ground_station_id": ground_station_for(sat_idx, tick),
        "subsystem": subsystem,
        "event_timestamp": event_time.isoformat(),
        "received_timestamp": (event_time + timedelta(seconds=random.uniform(1, 8))).isoformat(),
        "battery_voltage_v": "", "battery_current_a": "", "solar_output_w": "",
        "internal_temp_c": "", "external_temp_c": "",
        "signal_strength_dbm": "", "bit_error_rate": "", "comm_status": "",
        "latitude": "", "longitude": "", "altitude_km": "", "velocity_kms": "",
        "is_malformed": "false", "malformed_reason": "",
        "is_duplicate": "false", "duplicate_of_message_id": "",
    }

    if subsystem == "POWER":
        # gentle daily charge/discharge cycle plus noise
        cycle = 0.4 * __import__("math").sin(tick * 0.02 + sat_idx)
        row["battery_voltage_v"] = round(28.0 + cycle + random.uniform(-0.15, 0.15), 3)
        row["battery_current_a"] = round(2.1 + random.uniform(-0.3, 0.3), 3)
        row["solar_output_w"] = round(max(0, 140 + 60 * __import__("math").sin(tick * 0.02 + sat_idx) + random.uniform(-10, 10)), 2)

    elif subsystem == "THERMAL":
        row["internal_temp_c"] = round(21 + random.uniform(-3, 3), 2)
        row["external_temp_c"] = round(-40 + random.uniform(-25, 60), 2)

    elif subsystem == "COMMS":
        if satellite_id == DEGRADED_SATELLITE:
            strength = round(-95 + random.uniform(-15, 8), 1)
        else:
            strength = round(-65 + random.uniform(-12, 12), 1)
        row["signal_strength_dbm"] = strength
        row["bit_error_rate"] = round(max(0, random.gauss(0.0005, 0.0006)), 6)
        if strength < -95:
            row["comm_status"] = "LOST"
        elif strength < -80:
            row["comm_status"] = "DEGRADED"
        else:
            row["comm_status"] = "NOMINAL"

    elif subsystem == "ORBITAL":
        lat, lon, alt, vel = base_orbit(sat_idx, tick)
        row["latitude"], row["longitude"], row["altitude_km"], row["velocity_kms"] = lat, lon, alt, vel

    return row

def corrupt(row, subsystem):
    """Mutate a row in place to become a malformed record; return the reason."""
    choice = random.choice(["range", "missing", "future_ts", "bad_type"])
    if choice == "range" and subsystem == "POWER":
        row["battery_voltage_v"] = round(random.uniform(-40, -5), 2)  # physically impossible
        return "battery_voltage_v out of physical range"
    if choice == "range" and subsystem == "THERMAL":
        row["internal_temp_c"] = round(random.uniform(500, 900), 2)  # impossible for internal bus
        return "internal_temp_c out of physical range"
    if choice == "range" and subsystem == "COMMS":
        row["signal_strength_dbm"] = round(random.uniform(10, 60), 1)  # positive dBm, implausible
        return "signal_strength_dbm out of physical range"
    if choice == "range" and subsystem == "ORBITAL":
        row["altitude_km"] = round(random.uniform(-500, 0), 2)  # negative altitude
        return "altitude_km out of physical range"
    if choice == "missing":
        key = {
            "POWER": "battery_voltage_v", "THERMAL": "internal_temp_c",
            "COMMS": "signal_strength_dbm", "ORBITAL": "latitude",
        }[subsystem]
        row[key] = ""
        return f"{key} missing"
    if choice == "future_ts":
        future = datetime.now(timezone.utc) + timedelta(days=random.randint(1, 30))
        row["event_timestamp"] = future.isoformat()
        return "event_timestamp in the future"
    # bad_type
    key = {
        "POWER": "battery_current_a", "THERMAL": "external_temp_c",
        "COMMS": "bit_error_rate", "ORBITAL": "velocity_kms",
    }[subsystem]
    row[key] = "N/A"
    return f"{key} non-numeric"

rows = []
for tick in range(TOTAL_TICKS):
    event_time = START + timedelta(minutes=tick * INTERVAL_MIN)
    for sat_idx, satellite_id in enumerate(SATELLITES):
        if satellite_id == DROPOUT_SATELLITE and DROPOUT_START_TICK <= tick < DROPOUT_START_TICK + DROPOUT_DURATION_TICKS:
            continue  # satellite is out of contact -- no telemetry at all this tick

        for subsystem in SUBSYSTEMS:
            row = make_reading(sat_idx, satellite_id, subsystem, event_time, tick)

            if random.random() < MALFORMED_RATE:
                reason = corrupt(row, subsystem)
                row["is_malformed"] = "true"
                row["malformed_reason"] = reason

            rows.append(row)

            if random.random() < DUPLICATE_RATE:
                dup = dict(row)
                dup["message_id"] = uuid.uuid4().hex
                dup["received_timestamp"] = (
                    datetime.fromisoformat(row["received_timestamp"]) + timedelta(seconds=random.uniform(0.5, 4))
                ).isoformat()
                dup["is_duplicate"] = "true"
                dup["duplicate_of_message_id"] = row["message_id"]
                rows.append(dup)

random.shuffle(rows)  # arrival order isn't generation order -- ground stations relay out of sequence

fieldnames = list(rows[0].keys())
out_path = "source/orbitalsense_telemetry_raw.csv"
with open(out_path, "w", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(rows)

# Summary stats for the facilitator
n = len(rows)
n_malformed = sum(1 for r in rows if r["is_malformed"] == "true")
n_duplicate = sum(1 for r in rows if r["is_duplicate"] == "true")
per_sat = {}
for r in rows:
    per_sat[r["satellite_id"]] = per_sat.get(r["satellite_id"], 0) + 1

summary = {
    "total_rows": n,
    "malformed_rows": n_malformed,
    "malformed_pct": round(100 * n_malformed / n, 3),
    "duplicate_rows": n_duplicate,
    "duplicate_pct": round(100 * n_duplicate / n, 3),
    "date_range": [START.isoformat(), (START + timedelta(days=DAYS)).isoformat()],
    "satellites": SATELLITES,
    "ground_stations": GROUND_STATIONS,
    "subsystems": SUBSYSTEMS,
    "dropout_satellite": DROPOUT_SATELLITE,
    "dropout_window_utc": [
        (START + timedelta(minutes=DROPOUT_START_TICK * INTERVAL_MIN)).isoformat(),
        (START + timedelta(minutes=(DROPOUT_START_TICK + DROPOUT_DURATION_TICKS) * INTERVAL_MIN)).isoformat(),
    ],
    "degraded_comms_satellite": DEGRADED_SATELLITE,
    "rows_per_satellite": per_sat,
}
with open("source/orbitalsense_dataset_summary.json", "w") as f:
    json.dump(summary, f, indent=2)

print("rows:", n)
print("malformed:", n_malformed, f"({summary['malformed_pct']}%)")
print("duplicates:", n_duplicate, f"({summary['duplicate_pct']}%)")
