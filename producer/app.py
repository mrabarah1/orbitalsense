import json
import os
import random
import uuid
from datetime import datetime, timedelta, timezone

from flask import Flask, jsonify, request
from google.cloud import pubsub_v1




# Flask application
app = Flask(__name__)


# Configuration
PROJECT_ID = os.environ["PROJECT_ID"]
TOPIC_ID = os.environ["PUBSUB_TOPIC"]

PIPELINE_VERSION = os.environ.get(
    "PIPELINE_VERSION",
    "1.0.0",
)


publisher = pubsub_v1.PublisherClient()

topic_path = publisher.topic_path(
    PROJECT_ID,
    TOPIC_ID,
)



# OrbitalSense entities
SATELLITES = [
    f"SAT-{i:02d}"
    for i in range(1, 13)
]

GROUND_STATIONS = [
    "GS-1",
    "GS-2",
    "GS-3",
    "GS-4",
]

SUBSYSTEMS = [
    "POWER",
    "THERMAL",
    "COMMS",
    "ORBITAL",
]



# Mission fault scenario
#
# SAT-07 experiences a telemetry dropout.
#
# For the live producer, we use a configurable short
# simulation window rather than a six-hour wall-clock pause.
#
# The original project brief's historical dataset scenario
# remains represented by SAT-07 being the dropout satellite.


DROPOUT_SATELLITE = "SAT-07"


try:

    DROPOUT_DURATION_SECONDS = int(
        os.environ.get(
            "DROPOUT_DURATION_SECONDS",
            "300",
        )
    )

except ValueError:

    DROPOUT_DURATION_SECONDS = 300


# Dropout starts shortly after service startup.
SERVICE_START = datetime.now(
    timezone.utc
)

DROPOUT_START = (
    SERVICE_START
    + timedelta(seconds=60)
)

DROPOUT_END = (
    DROPOUT_START
    + timedelta(
        seconds=DROPOUT_DURATION_SECONDS
    )
)



# Utility functions
def utc_now():
    return datetime.now(timezone.utc)


def publish_event(event):
    """
    Publish one event to Pub/Sub and wait for the
    publish operation to complete.
    """

    payload = json.dumps(event, separators=(",", ":"),).encode("utf-8")

    future = publisher.publish(topic_path, payload)

    return future.result()


def satellite_is_silent(satellite_id, event_time):
    """
    Determine whether a satellite is currently
    experiencing the simulated telemetry dropout.
    """

    if satellite_id != DROPOUT_SATELLITE:
        return False

    return (
        DROPOUT_START
        <= event_time
        <= DROPOUT_END
    )



# Ground-station coverage
def ground_station(satellite_index, tick):
    """
    Deterministic ground-station coverage model.

    Satellites move between the four ground stations
    rather than being permanently attached to one station.
    """

    return GROUND_STATIONS[(satellite_index + tick) % len(GROUND_STATIONS)]



# Telemetry generation
def generate_event(satellite_index, tick, subsystem):
    """
    Generate one OrbitalSense telemetry event.

    Every event contains the common telemetry envelope.
    Subsystem-specific measurements are populated according
    to the subsystem.
    """

    satellite_id = SATELLITES[satellite_index]

    event_time = utc_now()

    receive_delay = random.uniform(1, 8)

    received_time = (event_time + timedelta(seconds=receive_delay))

    event = {
        "message_id": uuid.uuid4().hex,

        "satellite_id": satellite_id,

        "ground_station_id": ground_station(
            satellite_index,
            tick,
        ),

        "subsystem": subsystem,

        "event_timestamp": ( event_time.isoformat()),

        "received_timestamp": (received_time.isoformat()),

        # POWER
        "battery_voltage_v": None,
        "battery_current_a": None,
        "solar_output_w": None,

        # THERMAL
        "internal_temp_c": None,
        "external_temp_c": None,

        # COMMS
        "signal_strength_dbm": None,
        "bit_error_rate": None,
        "comm_status": None,

        # ORBITAL
        "latitude": None,
        "longitude": None,
        "altitude_km": None,
        "velocity_kms": None,
    }

   
    # POWER
    if subsystem == "POWER":

        event["battery_voltage_v"] = round(random.uniform(27.5, 28.5), 3)

        event["battery_current_a"] = round(random.uniform(1.8, 2.4), 3)

        event["solar_output_w"] = round(random.uniform(80, 200), 2)

    # THERMAL
    elif subsystem == "THERMAL":

        event["internal_temp_c"] = round(random.uniform(18, 24,), 2)
       
        event["external_temp_c"] = round(random.uniform(-60, 20,), 2)

    # COMMS
    # SAT-11 deliberately has degraded communication.
    elif subsystem == "COMMS":
        if satellite_id == "SAT-11":
            signal_strength = random.uniform(-110, -88)
            
        else:
            signal_strength = random.uniform(-75, -50)

        event["signal_strength_dbm"] = round(signal_strength, 1)

        if signal_strength < -95:

            status = "LOST"

        elif signal_strength < -80:

            status = "DEGRADED"

        else:

            status = "NOMINAL"

        event["comm_status"] = status

        event["bit_error_rate"] = round(random.uniform(0, 0.002),6)


    # ORBITAL
    elif subsystem == "ORBITAL":
        event["latitude"] = round(random.uniform(-51.6, 51.6),5)

        event["longitude"] = round(random.uniform(-180, 180),5)

        event["altitude_km"] = round(random.uniform(530, 550),2)

        event["velocity_kms"] = round(random.uniform(7.55, 7.65),3)

    return event



# Health endpoint
@app.get("/")
def health():

    now = utc_now()

    dropout_active = (
        DROPOUT_START
        <= now
        <= DROPOUT_END
    )

    return jsonify(
        {
            "service": "orbitalsense-producer",
            "status": "healthy",
            "pipeline_version": (
                PIPELINE_VERSION
            ),
            "project_id": PROJECT_ID,
            "topic": TOPIC_ID,
            "satellites": len(
                SATELLITES
            ),
            "ground_stations": len(
                GROUND_STATIONS
            ),
            "subsystems": len(
                SUBSYSTEMS
            ),
            "dropout_satellite": (
                DROPOUT_SATELLITE
            ),
            "dropout_active": (
                dropout_active
            ),
            "dropout_start": (
                DROPOUT_START.isoformat()
            ),
            "dropout_end": (
                DROPOUT_END.isoformat()
            ),
        }
    )


# Normal telemetry
@app.post("/publish")
def publish():
    body = (request.get_json(silent=True) or {})

    try:
        cycles = int(body.get("cycles", 1))
    except (TypeError, ValueError):

        return jsonify({"error": ("cycles must be an integer")}), 400

    if cycles < 1:

        return jsonify({"error": ("cycles must be at least 1")}), 400

    if cycles > 100:

        return jsonify({"error": ("maximum cycles is 100")}), 400

    published = 0
    skipped = 0

    published_message_ids = []

    for tick in range(cycles):

        for satellite_index, satellite_id in enumerate(SATELLITES):
            event_time = utc_now()

            # Simulated satellite dropout
            if satellite_is_silent(satellite_id, event_time):
                skipped += len(SUBSYSTEMS)
                continue

            # Four subsystem readings
            for subsystem in SUBSYSTEMS:
                event = generate_event(satellite_index, tick,subsystem)

                message_id = publish_event(event)

                published += 1

                published_message_ids.append(message_id)

    return jsonify(
        {
            "published": published,
            "skipped_due_to_dropout": skipped,
            "dropout_satellite": (
                DROPOUT_SATELLITE
            ),
            "message_ids": (
                published_message_ids
            ),
        }
    )


# Invalid physical value
@app.post("/publish-invalid")
def publish_invalid():

    event = generate_event(satellite_index=0, tick=1, subsystem="POWER")

    # Deliberately impossible battery voltage.
    event["battery_voltage_v"] = -500

    message_id = publish_event(event)

    return jsonify(
        {
            "published": 1,
            "message_id": message_id,
            "type": "physically_invalid",
            "expected_reason": (
                "OUT_OF_PHYSICAL_RANGE"
            ),
        }
    )



# Malformed JSON
@app.post("/publish-malformed")
def publish_malformed():

    payload = (b'{"message_id":"broken-json"}')

    message_id = publisher.publish(topic_path, payload).result()

    return jsonify(
        {
            "published": 1,
            "message_id": message_id,
            "type": "malformed_json",
            "expected_reason": (
                "INVALID_JSON"
            ),
        }
    )



# Missing required field
@app.post("/publish-missing-field")
def publish_missing_field():

    event = generate_event(satellite_index=0, tick=1, subsystem="POWER")

    # Remove a required field.
    del event["satellite_id"]

    message_id = publish_event(event)

    return jsonify(
        {
            "published": 1,
            "message_id": message_id,
            "type": "missing_required_field",
            "expected_reason": (
                "MISSING_REQUIRED_FIELD"
            ),
        }
    )


# Invalid satellite
@app.post("/publish-invalid-satellite")
def publish_invalid_satellite():

    event = generate_event(satellite_index=0, tick=1, subsystem="POWER")

    event["satellite_id"] = "SAT-99"

    message_id = publish_event(event)

    return jsonify(
        {
            "published": 1,
            "message_id": message_id,
            "type": "invalid_satellite",
            "expected_reason": (
                "INVALID_SATELLITE"
            ),
        }
    )



# Semantic duplicate
@app.post("/publish-duplicate")
def publish_duplicate():

    
    # First telemetry event
    event = generate_event(satellite_index=0, tick=1, subsystem="POWER")

    first_message_id = publish_event(event)

   
    # Duplicate event
    # message_id is changed.
    # received_timestamp is changed.
    #
    # The semantic telemetry fields remain identical,
    # therefore Beam's dedup_key remains identical.
   
    duplicate = dict(event)

    duplicate["message_id"] = (uuid.uuid4().hex)

    duplicate["received_timestamp"] = (utc_now().isoformat())

    second_message_id = publish_event(duplicate)

    return jsonify(
        {
            "published": 2,
            "first_message_id": (
                first_message_id
            ),
            "second_message_id": (
                second_message_id
            ),
            "type": "semantic_duplicate",
            "message": (
                "Both events have the same semantic "
                "telemetry identity and should produce "
                "the same dedup_key."
            ),
        }
    )



# Multiple duplicates
@app.post("/publish-duplicates")
def publish_duplicates():

    body = (request.get_json(silent=True) or {})

    try:
        copies = int(body.get("copies", 5))
    except (TypeError, ValueError):

        return jsonify({"error": ("copies must be an integer")}), 400

    if copies < 2:
        return jsonify({"error": ("copies must be at least 2")}), 400

    if copies > 20:
        return jsonify({"error": ("maximum copies is 20")}), 400

    event = generate_event(satellite_index=0, tick=1, subsystem="POWER")
    

    message_ids = []

    for copy_number in range(copies):
        duplicate = dict(event)

        duplicate["message_id"] = (uuid.uuid4().hex)

        duplicate["received_timestamp"] = (utc_now().isoformat())

        message_ids.append(publish_event(duplicate))

    return jsonify({
            "published": copies,
            "message_ids": message_ids,
            "type": "semantic_duplicates",
            "expected_curated_records": 1,
        }
    )



# Application entrypoint
if __name__ == "__main__":

    port = int(os.environ.get("PORT", "8080",))

    app.run(host="0.0.0.0", port=port)