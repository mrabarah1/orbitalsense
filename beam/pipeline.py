import argparse
import hashlib
import json
import math
from datetime import datetime, timezone

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.bigquery import WriteToBigQuery
from apache_beam.transforms.window import FixedWindows


PIPELINE_VERSION = "1.0.0"

SATELLITES = {
    f"SAT-{i:02d}" for i in range(1, 13)
}

GROUND_STATIONS = {
    "GS-1",
    "GS-2",
    "GS-3",
    "GS-4",
}


SUBSYSTEMS = {
    "POWER",
    "THERMAL",
    "COMMS",
    "ORBITAL",
}


REQUIRED_FIELDS = [
    "message_id",
    "satellite_id",
    "ground_station_id",
    "subsystem",
    "event_timestamp",
    "received_timestamp"
]


def dedup_key(record):
    """
    The source deliberately changes message_id and received_timestamp
    when creating a duplicate.

    Therefore those fields MUST NOT be used as the semantic
    duplicate key.
    """

    fields = [
        "satellite_id",
        "ground_station_id",
        "subsystem",
        "event_timestamp",
        "battery_voltage_v",
        "battery_current_a",
        "solar_output_w",
        "internal_temp_c",
        "external_temp_c",
        "signal_strength_dbm",
        "bit_error_rate",
        "comm_status",
        "latitude",
        "longitude",
        "altitude_km",
        "velocity_kms",
    ]

    payload = "|".join(
        str(record.get(field, ""))
        for field in fields
    )

    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


# Timestamp Parsing
def parse_timestamp(value):
    if not value:
        raise ValueError("Timestamp is empty")
    
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


# Validation
def validate_record(record):
    # json validation
    if not isinstance(record, dict):
        return False, "INVALID_JSON", "Payload is not an object"

    #Required fields
    for field in REQUIRED_FIELDS:
        if field not in record:
            return (False, "MISSING_REQUIRED_FIELD",field)
        
        if record[field] in ("", None):
            return (False, "MISSING_REQUIRED_FIELD", field)
        
    
     # Satellite
    if record["satellite_id"] not in SATELLITES:
        return (False, "INVALID_SATELLITE", record["satellite_id"])

    # Ground station
    if record["ground_station_id"] not in GROUND_STATIONS:

        return (False, "INVALID_GROUND_STATION", record["ground_station_id"])


    # Subsystem validation
    subsystem = record["subsystem"]

    if subsystem not in SUBSYSTEMS:
        return (False,"INVALID_SUBSYSTEM", subsystem)
    
    
    # Timestamps Validation
    try:
        event_timestamp = parse_timestamp(record["event_timestamp"])
        received_timestamp = parse_timestamp(record["received_timestamp"])
        
    except Exception as exc:
        return (False, "INVALID_TIMESTAMP", str(exc))

    now = datetime.now(timezone.utc)

    if event_timestamp > now:
        return (False, "FUTURE_EVENT_TIMESTAMP", record["event_timestamp"])
    
    if received_timestamp < event_timestamp:
        return (False, "INVALID_RECEIVED_TIMESTAMP","received_timestamp before event_timestamp")
    
    

    # Subsystem-specific validation
    try:

        if subsystem == "POWER":

            voltage = float(record["battery_voltage_v"])
            if not 20 <= voltage <= 35:
                return (False, "OUT_OF_PHYSICAL_RANGE", "battery_voltage_v")


            current = float(record["battery_current_a"])
            if not 0 <= current <= 10:
                return (False, "OUT_OF_PHYSICAL_RANGE", "battery_current_a")
            
            
            solar = float(record["solar_output_w"])
            if not 0 <= solar <= 1000:
                return (False, "OUT_OF_PHYSICAL_RANGE", "solar_output_w")

        elif subsystem == "THERMAL":

            internal = float(record["internal_temp_c"])
            if not -100 <= internal <= 100:
                return (False, "OUT_OF_PHYSICAL_RANGE", "internal_temp_c")
            
            
            external = float(record["external_temp_c"])
            if not -200 <= external <= 150:
                return (False, "OUT_OF_PHYSICAL_RANGE", "external_temp_c")
            

        elif subsystem == "COMMS":

            signal = float(record["signal_strength_dbm"])
            if not -150 <= signal <= 0:
                return (False, "OUT_OF_PHYSICAL_RANGE", "signal_strength_dbm")

            ber = float(record["bit_error_rate"])
            if not 0 <= ber <= 1:
                return (False, "OUT_OF_PHYSICAL_RANGE", "bit_error_rate")
            
            if record["comm_status"] not in {"NOMINAL","DEGRADED","LOST"}:
                return (False, "INVALID_COMM_STATUS",record["comm_status"])

        elif subsystem == "ORBITAL":

            altitude = float(record["altitude_km"])
            if not 0 <= altitude <= 2000:
                return (False, "OUT_OF_PHYSICAL_RANGE", "altitude_km")

            latitude = float(record["latitude"])
            if not -90 <= latitude <= 90:
                return (False, "OUT_OF_PHYSICAL_RANGE", "latitude")

            longitude = float(record["longitude"])
            if not -180 <= longitude <= 180:
                return (False, "OUT_OF_PHYSICAL_RANGE", "longitude")
            
            velocity = float(record["velocity_kms"])
            if not 0 <= velocity <= 15:
                return (False, "OUT_OF_PHYSICAL_RANGE", "velocity_kms")

    except (TypeError, ValueError):
        return (False, "INVALID_NUMERIC_TYPE", subsystem)

    return True, None, None


class ParseValidateEnrich(beam.DoFn):

    QUARANTINE_TAG = "quarantine"

    def process(self, element):

        raw_payload = element.decode( "utf-8", errors="replace",)

        
        # Parse JSON
        try:

            record = json.loads(raw_payload)

        except Exception as exc:

            yield beam.pvalue.TaggedOutput(
                self.QUARANTINE_TAG,
                {
                    "message_id": None,
                    "satellite_id": None,
                    "ground_station_id": None,
                    "subsystem": None,
                    "event_timestamp": None,
                    "reason_code": "INVALID_JSON",
                    "reason_detail": str(exc),
                    "quarantined_at": datetime.now(
                        timezone.utc
                    ).isoformat(),
                    "pipeline_version": PIPELINE_VERSION,
                    "raw_payload": raw_payload,
                },
            )

            return

    
        # Validate
        valid, code, detail = validate_record(
            record
        )

        if not valid:

            yield beam.pvalue.TaggedOutput(
                self.QUARANTINE_TAG,
                {
                    "message_id": record.get(
                        "message_id"
                    ),
                    "satellite_id": record.get(
                        "satellite_id"
                    ),
                    "ground_station_id": record.get(
                        "ground_station_id"
                    ),
                    "subsystem": record.get(
                        "subsystem"
                    ),
                    "event_timestamp": record.get(
                        "event_timestamp"
                    ),
                    "reason_code": code,
                    "reason_detail": detail,
                    "quarantined_at": datetime.now(
                        timezone.utc
                    ).isoformat(),
                    "pipeline_version": PIPELINE_VERSION,
                    "raw_payload": raw_payload,
                },
            )

            return
        
        
        
    # Convert numeric fields from CSV strings
        numeric_fields = [
            "battery_voltage_v",
            "battery_current_a",
            "solar_output_w",
            "internal_temp_c",
            "external_temp_c",
            "signal_strength_dbm",
            "bit_error_rate",
            "latitude",
            "longitude",
            "altitude_km",
            "velocity_kms",
        ]
        
        for field in numeric_fields:
                    
            value = record.get(field)
        
            if value in (None, ""):
        
                record[field] = None
            else:
        
                record[field] = float(value)
        

        # Enrichment
        record["dedup_key"] = dedup_key(record)

        record["ingestion_timestamp"] = (datetime.now(timezone.utc).isoformat())

        record["pipeline_version"] = (PIPELINE_VERSION)

        record["source_ground_station"] = (record["ground_station_id"])
        
        yield record
        
      


 # RAW RECORD
class CreateRawRecord(beam.DoFn):

    def process(self, element):

        raw_payload = element.decode(
            "utf-8",
            errors="replace",
        )

        yield {
            "raw_payload": raw_payload,
            "ingestion_timestamp": datetime.now(
                timezone.utc
            ).isoformat(),
            "pipeline_version": PIPELINE_VERSION,
        }



# DEDUPLICATION
class KeepFirst(beam.DoFn):
    """
    Keep the earliest received record for each
    semantic telemetry event.
    """

    def process(self, element):

        key, records = element

        records = list(records)

        if not records:
            return
        
        records.sort(key=lambda record: parse_timestamp(record["received_timestamp"]))
        
        yield records[0]



# BIGQUERY SCHEMAS
RAW_SCHEMA = {
    "fields": [

        {
            "name": "raw_payload",
            "type": "STRING",
            "mode": "REQUIRED",
        },

        {
            "name": "ingestion_timestamp",
            "type": "TIMESTAMP",
            "mode": "REQUIRED",
        },

        {
            "name": "pipeline_version",
            "type": "STRING",
            "mode": "REQUIRED",
        },
    ]
}

CURATED_SCHEMA = {
    "fields": [
        {
            "name": "message_id",
            "type": "STRING",
            "mode": "NULLABLE",
        },
        {
            "name": "satellite_id",
            "type": "STRING",
            "mode": "REQUIRED",
        },
        {
            "name": "ground_station_id",
            "type": "STRING",
            "mode": "REQUIRED",
        },
        {
            "name": "subsystem",
            "type": "STRING",
            "mode": "REQUIRED",
        },
        {
            "name": "event_timestamp",
            "type": "TIMESTAMP",
            "mode": "REQUIRED",
        },
        {
            "name": "received_timestamp",
            "type": "TIMESTAMP",
            "mode": "REQUIRED",
        },
        {
            "name": "battery_voltage_v",
            "type": "FLOAT",
            "mode": "NULLABLE",
        },
        {
            "name": "battery_current_a",
            "type": "FLOAT",
            "mode": "NULLABLE",
        },
        {
            "name": "solar_output_w",
            "type": "FLOAT",
            "mode": "NULLABLE",
        },
        {
            "name": "internal_temp_c",
            "type": "FLOAT",
            "mode": "NULLABLE",
        },
        {
            "name": "external_temp_c",
            "type": "FLOAT",
            "mode": "NULLABLE",
        },
        {
            "name": "signal_strength_dbm",
            "type": "FLOAT",
            "mode": "NULLABLE",
        },
        {
            "name": "bit_error_rate",
            "type": "FLOAT",
            "mode": "NULLABLE",
        },
        {
            "name": "comm_status",
            "type": "STRING",
            "mode": "NULLABLE",
        },
        {
            "name": "latitude",
            "type": "FLOAT",
            "mode": "NULLABLE",
        },
        {
            "name": "longitude",
            "type": "FLOAT",
            "mode": "NULLABLE",
        },
        {
            "name": "altitude_km",
            "type": "FLOAT",
            "mode": "NULLABLE",
        },
        {
            "name": "velocity_kms",
            "type": "FLOAT",
            "mode": "NULLABLE",
        },
        {
            "name": "dedup_key",
            "type": "STRING",
            "mode": "REQUIRED",
        },
        {
            "name": "ingestion_timestamp",
            "type": "TIMESTAMP",
            "mode": "REQUIRED",
        },
        {
            "name": "pipeline_version",
            "type": "STRING",
            "mode": "REQUIRED",
        },
        {
            "name": "source_ground_station",
            "type": "STRING",
            "mode": "REQUIRED",
        },
    ]
}


QUARANTINE_SCHEMA = {
    "fields": [
        {
            "name": "message_id",
            "type": "STRING",
            "mode": "NULLABLE",
        },
        {
            "name": "satellite_id",
            "type": "STRING",
            "mode": "NULLABLE",
        },
        {
            "name": "ground_station_id",
            "type": "STRING",
            "mode": "NULLABLE",
        },
        {
            "name": "subsystem",
            "type": "STRING",
            "mode": "NULLABLE",
        },
        {
            "name": "event_timestamp",
            "type": "TIMESTAMP",
            "mode": "NULLABLE",
        },
        {
            "name": "reason_code",
            "type": "STRING",
            "mode": "REQUIRED",
        },
        {
            "name": "reason_detail",
            "type": "STRING",
            "mode": "NULLABLE",
        },
        {
            "name": "quarantined_at",
            "type": "TIMESTAMP",
            "mode": "REQUIRED",
        },
        {
            "name": "pipeline_version",
            "type": "STRING",
            "mode": "REQUIRED",
        },
        {
            "name": "raw_payload",
            "type": "JSON",
            "mode": "NULLABLE",
        },
    ]
}

# Pipeline
def run():

    parser = argparse.ArgumentParser()

    parser.add_argument("--input_subscription", required=True)

    parser.add_argument("--raw_table", required=True)

    parser.add_argument("--curated_table", required=True)

    parser.add_argument("--quarantine_table", required=True)

    known_args, pipeline_args = parser.parse_known_args()

    options = PipelineOptions(pipeline_args, streaming=True, save_main_session=True)

    with beam.Pipeline(options=options) as pipeline:
        # Pub/Sub
        messages = (
            pipeline
            | "ReadPubSub"
            >> beam.io.ReadFromPubSub(subscription=known_args.input_subscription)
        )

        # Raw Branch
        raw_records = (
            messages
            | "CreateRawRecords"
            >> beam.ParDo(CreateRawRecord())
        )

        (
            raw_records
            | "WriteRawToBigQuery"
            >> WriteToBigQuery(
                table=known_args.raw_table,
                schema=RAW_SCHEMA,
                write_disposition=(
                    beam.io.BigQueryDisposition
                    .WRITE_APPEND
                ),
                create_disposition=(
                    beam.io.BigQueryDisposition
                    .CREATE_NEVER
                ),
            )
        )

        
        # VALIDATION
        processed = (
            messages
            | "ParseValidateEnrich"
            >> beam.ParDo(
                ParseValidateEnrich()
            ).with_outputs(
                "quarantine",
                main="valid",
            )
        )

        valid = processed.valid

        quarantine = processed.quarantine


        # QUARANTINE
        (
            quarantine
            | "WriteQuarantine"
            >> WriteToBigQuery(
                table=known_args.quarantine_table,
                schema=QUARANTINE_SCHEMA,
                write_disposition=(
                    beam.io.BigQueryDisposition
                    .WRITE_APPEND
                ),
                create_disposition=(
                    beam.io.BigQueryDisposition
                    .CREATE_NEVER
                ),
            )
        )

        # EVENT-TIME WINDOWING
        windowed = (
            valid
            | "AddEventTimestamp"
            >> beam.Map(
                lambda record: beam.window.TimestampedValue(
                    record,
                    parse_timestamp(
                        record["event_timestamp"]
                    ).timestamp(),
                )
            )
            | "FiveMinuteWindows"
            >> beam.WindowInto(
                FixedWindows(300),
                allowed_lateness=3600,
            )
        )


        # DEDUPLICATION
        deduplicated = (
            windowed
            | "KeyByDedupKey"
            >> beam.Map(
                lambda record: (
                    record["dedup_key"],
                    record,
                )
            )
            | "GroupByDedupKey"
            >> beam.GroupByKey()
            
            | "KeepFirstRecord"
            >> beam.ParDo(
                KeepFirst()
            )
        )

        # CURATED BIGQUERY
        (
            deduplicated
            | "WriteCurated"
            >> WriteToBigQuery(
                table=known_args.curated_table,
                schema=CURATED_SCHEMA,
                write_disposition=(
                    beam.io.BigQueryDisposition
                    .WRITE_APPEND
                ),
                create_disposition=(
                    beam.io.BigQueryDisposition
                    .CREATE_NEVER
                ),
            )
        )
        
        
if __name__ == "__main__":
    run()