import json
import os
import random
import uuid
from datetime import datetime, timedelta, timezone
from flask import Flask, jsonify, request
from google.cloud import pubsub_v1

app = Flask(__name__)

PROJECT_ID = os.environ["PROJECT_ID"]
TOPIC_ID = os.environ["PUBSUB_TOPIC"]

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)

SATELLITES = [f"SAT-{i:02d}" for i in range(1, 13)]
GROUND_STATIONS = ["GS-1", "GS-2", "GS-3", "GS-4"]
SUBSYSTEMS = ["POWER", "THERMAL", "COMMS", "ORBITAL"]

random.seed()


def ground_station(satellite_index, tick):
    return GROUND_STATIONS[(satellite_index + tick) % len(GROUND_STATIONS)]
    
    
# Telemetry Generator   
def generate_event(satellite_index, tick):
    satellite_id = SATELLITES[satellite_index]
    subsystem = random.choice(SUBSYSTEMS)
    
    event_time = datetime.now(timezone.utc)
    
    # Simulate 1–8 second transport delay
    # receive_delay = random.uniform(1, 8,)

    # received_time = (event_time + timedelta(seconds=receive_delay))

    
    event = {
        "message_id": uuid.uuid4().hex,
        "satellite_id": satellite_id,
        "ground_station_id": ground_station(satellite_index, tick),
        "subsystem": subsystem,
        "event_timestamp": event_time.isoformat(),
        "received_timestamp": datetime.now(timezone.utc).isoformat(),
        "battery_voltage_v": None,
        "battery_current_a": None,
        "solar_output_w": None,
        "internal_temp_c": None,
        "external_temp_c": None,
        "signal_strength_dbm": None,
        "bit_error_rate": None,
        "comm_status": None,
        "latitude": None,
        "longitude": None,
        "altitude_km": None,
        "velocity_kms": None,
    }
    
    if subsystem == "POWER":
        event["battery_voltage_v"] = round(random.uniform(27.5,28.5), 3)
        event["battery_current_a"] = round(random.uniform(1.8, 2.4), 3)
        event["solar_output_w"] = round(random.uniform(80, 200), 2)
        
    elif subsystem == "THERMAL":
        event["internal_temp_c"] = round(random.uniform(18,24), 2)
        event["external_temp_c"] = round(random.uniform(-60, 20), 2)
        
    elif subsystem == "COMMS":  
        if satellite_id == "SAT-11":
            strength = random.uniform(-110, -88)
        else:
            strength = random.uniform(-75, -50)
            
        event["signal_strength_dbm"] = round(strength, 1)
        
        if strength < -95:
            status = "LOST"
        elif strength < -80:
            status = "DEGRADED"
        else:
            status = "NOMINAL"
            
        event["comm_status"] = status
        event["bit_error_rate"] = round(random.uniform(0, 0.002), 6)
        
    elif subsystem == "ORBITAL":

        event["latitude"] = round(random.uniform(-51.6, 51.6), 5)

        event["longitude"] = round(random.uniform(-180, 180), 5)

        event["altitude_km"] = round(random.uniform(530, 550), 2)

        event["velocity_kms"] = round(random.uniform(7.55, 7.65), 3)
    
    else:
        event["latitude"] = round(random.uniform(-51.6, 51.6), 5)
        event["longitude"] = round(random.uniform(-180, 180), 5)
        event["altitude_km"] = round(random.uniform(530, 550), 2)
        event["velocity_kms"] = round(random.uniform(7.55, 7.65), 3)
    return event

@app.get("/")
def health():
    return jsonify({
        "service": "orbitalsense-producer",
        "status": "healthy"
    })
    
# Normal Publish    
@app.get("/publish")
def publish():
    body = request.get_json(silent=True) or {}
    count = int(body.get("count", 10))
    
    try:
        count = int(body.get("count", 10))
    except (TypeError, ValueError):
        return jsonify({"error": "count must be an integer"}), 400
    
    if count < 1:
        return jsonify({"error": "count must be at least 1"}), 400
    
    if count > 1000:
        return jsonify({"error": "maximum count is 1000"}), 400
        
    futures = []
    
    for i in range(count):
        satellite_index = i % len(SATELLITES)
        
        event = generate_event(satellite_index, i)
        payload = json.dumps(event).encode("utf-8")
        
        futures.append(publisher.publish(topic_path, payload))
        
    for future in futures:
        future.result()
        
    return jsonify({"published": count})

# Malformed test
@app.post("/publish-malformed")
def publish_malformed():
    event = generate_event(0, 1)
    event["battery_voltage_v"] = -500
    
    publisher.publish(topic_path, json.dumps(event).encode("utf-8")).result()
    
    return jsonify({"published": 1, "type": "malformed"})


@app.post("/publish-duplicate")
def publish_duplicate():
    event = generate_event(0, 1)
    
    publisher.publish(topic_path, json.dumps(event).encode("utf-8")).result()
        
    duplicate = dict(event)
    duplicate["message_id"] = uuid.uuid4().hex
    
    publisher.publish(topic_path, json.dumps(duplicate).encode("utf-8")).result()
    
    return jsonify({"published": 2, "type": "duplicate"})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
    