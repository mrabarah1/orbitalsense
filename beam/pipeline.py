import argparse
import hashlib
import json
from datetime import datetime, timezone

import apache_beam as beam
from apache_beam.coders import BooleanCoder
from apache_beam.io.gcp.bigquery import WriteToBigQuery
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms import userstate
from apache_beam.transforms.timeutil import TimeDomain
from apache_beam.transforms.window import FixedWindows
from apache_beam.transforms.userstate import (
    ReadModifyWriteStateSpec,
    TimerSpec,
    TimeDomain,
)




# OrbitalSense pipeline configuration
PIPELINE_VERSION = "1.0.0"

WINDOW_SECONDS = 5 * 60
ALLOWED_LATENESS_SECONDS = 60 * 60

SOURCE = "pubsub"

SATELLITES = {
    f"SAT-{i:02d}"
    for i in range(1, 13)
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
    "received_timestamp",
]


NUMERIC_FIELDS = [
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



# Utility functions
def utc_now():
    return datetime.now(timezone.utc)


def timestamp_to_iso(value):
    return value.isoformat()


def parse_timestamp(value):
    """
    Parse ISO-8601 timestamps.

    Supports timestamps ending in Z as well as
    explicit UTC offsets.
    """

    if value in (None, ""):
        raise ValueError("Timestamp is empty")

    parsed = datetime.fromisoformat(
        str(value).replace("Z", "+00:00")
    )

    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)

    return parsed.astimezone(timezone.utc)


def dedup_key(record):
    """
    Create a semantic identity for a telemetry reading.

    Volatile ingestion metadata is deliberately excluded:

        message_id
        received_timestamp
        ingestion_timestamp

    This allows Pub/Sub redelivery or duplicate messages
    with different transport metadata to be recognized
    as the same telemetry event.
    """

    identity_fields = [
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

    canonical = {field: record.get(field) for field in identity_fields}

    canonical_json = json.dumps(
        canonical,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )

    return hashlib.sha256(canonical_json.encode("utf-8")).hexdigest()



# Validation
def validate_record(record):
    """
    Validate one telemetry record.

    Returns:

        (True, None, None)

    or:

        (False, reason_code, reason_detail)
    """

    if not isinstance(record, dict):
        return (False, "INVALID_JSON", "Payload must be a JSON object")



    # Required fields
    for field in REQUIRED_FIELDS:

        if field not in record:
            return (False, "MISSING_REQUIRED_FIELD", field)

        if record[field] in (None, ""):
            return (False, "MISSING_REQUIRED_FIELD", field)

 
    # Satellite
    satellite_id = record["satellite_id"]

    if satellite_id not in SATELLITES:
        return (False, "INVALID_SATELLITE", str(satellite_id))

    # Ground station
    ground_station_id = record["ground_station_id"]

    if ground_station_id not in GROUND_STATIONS:
        return (False, "INVALID_GROUND_STATION", str(ground_station_id))

    # Subsystem
    subsystem = record["subsystem"]

    if subsystem not in SUBSYSTEMS:
        return (False, "INVALID_SUBSYSTEM", str(subsystem))

    # Timestamp validation
    try:
        event_timestamp = parse_timestamp(record["event_timestamp"])

        received_timestamp = parse_timestamp(record["received_timestamp"])

    except Exception as exc:
        return (False, "INVALID_TIMESTAMP", str(exc))

    now = utc_now()

    if event_timestamp > now:
        return (False, "FUTURE_EVENT_TIMESTAMP", record["event_timestamp"])

    if received_timestamp < event_timestamp:
        return (False, "INVALID_RECEIVED_TIMESTAMP", "received_timestamp is before event_timestamp")

    # Physical validation
    try:  
        # POWER
        if subsystem == "POWER":
            voltage = float(record.get("battery_voltage_v"))
            
            if not 20 <= voltage <= 35:
                return (False, "OUT_OF_PHYSICAL_RANGE", "battery_voltage_v")

            current = float(record.get("battery_current_a"))
            
            if not 0 <= current <= 10:
                return (False, "OUT_OF_PHYSICAL_RANGE", "battery_current_a")

            solar = float(record.get("solar_output_w"))

            if not 0 <= solar <= 1000:
                return (False, "OUT_OF_PHYSICAL_RANGE", "solar_output_w")

        # THERMAL
        elif subsystem == "THERMAL":

            internal = float(record.get("internal_temp_c"))

            if not -100 <= internal <= 100:
                return (False, "OUT_OF_PHYSICAL_RANGE", "internal_temp_c")

            external = float(record.get("external_temp_c"))

            if not -200 <= external <= 150:
                return (False, "OUT_OF_PHYSICAL_RANGE", "external_temp_c")

        # COMMS
        elif subsystem == "COMMS":

            signal = float(record.get("signal_strength_dbm"))

            if not -150 <= signal <= 0:
                return (False, "OUT_OF_PHYSICAL_RANGE", "signal_strength_dbm")

            ber = float(record.get("bit_error_rate"))

            if not 0 <= ber <= 1:
                return (False, "OUT_OF_PHYSICAL_RANGE", "bit_error_rate")

            status = record.get("comm_status")

            if status not in {"NOMINAL", "DEGRADED", "LOST"}:
                return (False, "INVALID_COMM_STATUS", str(status))

            # Cross-field consistency.
            if signal >= -80 and status != "NOMINAL":
                return (False, "INCONSISTENT_COMM_STATUS", "Signal >= -80 dBm must be NOMINAL")

            if -95 <= signal < -80 and status != "DEGRADED":
                return ( False, "INCONSISTENT_COMM_STATUS", "Signal between -95 and -80 dBm must be DEGRADED")

            if signal < -95 and status != "LOST":
                return (False, "INCONSISTENT_COMM_STATUS", "Signal below -95 dBm must be LOST")

        
        # ORBITAL
        elif subsystem == "ORBITAL":
            latitude = float(record.get("latitude"))

            if not -90 <= latitude <= 90:
                return (False, "OUT_OF_PHYSICAL_RANGE", "latitude")

            longitude = float(record.get("longitude"))

            if not -180 <= longitude <= 180:
                return (False, "OUT_OF_PHYSICAL_RANGE", "longitude")
            

            altitude = float(record.get("altitude_km"))
            if not 0 <= altitude <= 2000:
                return (False, "OUT_OF_PHYSICAL_RANGE", "altitude_km")

            velocity = float(record.get("velocity_kms"))

            if not 0 <= velocity <= 15:
                return (False, "OUT_OF_PHYSICAL_RANGE", "velocity_kms")

    except (TypeError, ValueError):
        return (False, "INVALID_NUMERIC_TYPE", subsystem)

    return True, None, None




# Raw record
class CreateRawRecord(beam.DoFn):

    def process(self, element):
        raw_payload = element.decode("utf-8", errors="replace")

        message_id = None
        satellite_id = None
        ground_station_id = None
        subsystem = None

        try:
            record = json.loads(raw_payload)

            if isinstance(record, dict):
                message_id = record.get("message_id" )               

                satellite_id = record.get("satellite_id")

                ground_station_id = record.get("ground_station_id")

                subsystem = record.get("subsystem")

        except Exception:
            # The complete payload is still retained.
            pass

        yield {
            "raw_payload": raw_payload,
            "message_id": message_id,
            "satellite_id": satellite_id,
            "ground_station_id": ground_station_id,
            "subsystem": subsystem,
            "ingestion_timestamp": timestamp_to_iso(
                utc_now()
            ),
            "pipeline_version": PIPELINE_VERSION,
            "source": SOURCE,
        }



# Parse, validate and enrich
class ParseValidateEnrich(beam.DoFn):
    QUARANTINE_TAG = "quarantine"

    def process(self, element):
        raw_payload = element.decode("utf-8", errors="replace")

   
        # JSON parsing
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
                    "quarantined_at": timestamp_to_iso(
                        utc_now()
                    ),
                    "pipeline_version": PIPELINE_VERSION,
                    "raw_payload": raw_payload,
                },
            )

            return

        
        # Validation
        valid, reason_code, reason_detail = (validate_record(record))

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
                    "reason_code": reason_code,
                    "reason_detail": reason_detail,
                    "quarantined_at": timestamp_to_iso(
                        utc_now()
                    ),
                    "pipeline_version": PIPELINE_VERSION,
                    "raw_payload": raw_payload,
                },
            )

            return

     
        # Numeric normalization
        for field in NUMERIC_FIELDS:
            value = record.get(field)

            if value in (None, ""):
                record[field] = None

            else:
                record[field] = float(value)

        
        # Semantic identity
        record["dedup_key"] = dedup_key(record)

        # Ingestion metadata
        record["ingestion_timestamp"] = (timestamp_to_iso(utc_now()))

        record["pipeline_version"] = (PIPELINE_VERSION)

        record["source_ground_station"] = (record["ground_station_id"])

        yield record



# Stateful deduplication
class StatefulDeduplicate(beam.DoFn):
    """
    Stateful semantic deduplication.

    The incoming PCollection must be keyed by dedup_key.

    State is scoped by Beam key + window, which means
    duplicates are suppressed within the event-time window
    and allowed-lateness period.
    """

    SEEN = userstate.ReadModifyWriteStateSpec("seen", BooleanCoder())

    EXPIRY = userstate.TimerSpec("expiry", TimeDomain.WATERMARK)

    def process(
        self,
        element,
        timestamp=beam.DoFn.TimestampParam,
        seen=beam.DoFn.StateParam(SEEN),
        expiry=beam.DoFn.TimerParam(EXPIRY),
    ):

        dedup_key_value, record = element

        # Already seen semantic event.
        if seen.read():
            return

        # Mark semantic event as seen.
        seen.write(True)

        # Keep state until allowed lateness has passed.
        expiry.set(timestamp + ALLOWED_LATENESS_SECONDS)

        yield record

    @userstate.on_timer(EXPIRY)
    
    
    def clear_state(self, seen=beam.DoFn.StateParam(SEEN)):
        seen.clear()



# BigQuery schemas
RAW_SCHEMA = {
    "fields": [
        {
            "name": "raw_payload",
            "type": "STRING",
            "mode": "REQUIRED",
        },
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
            "name": "source",
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
            "mode": "REQUIRED",
        },
        {
            "name": "dedup_key",
            "type": "STRING",
            "mode": "REQUIRED",
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

    parser.add_argument("--input_subscription", required=True, help="Pub/Sub subscription")

    parser.add_argument("--raw_table", required=True, help="BigQuery raw table")

    parser.add_argument("--curated_table", required=True, help="BigQuery curated table")

    parser.add_argument("--quarantine_table", required=True, help="BigQuery quarantine table")

    known_args, pipeline_args = (parser.parse_known_args())

    options = PipelineOptions(pipeline_args, streaming=True, save_main_session=True)

    with beam.Pipeline(options=options) as pipeline:        
        # Pub/Sub
        messages = (
            pipeline
            | "ReadTelemetryFromPubSub"
            >> beam.io.ReadFromPubSub(
                subscription=(
                    known_args.input_subscription
                )
            )
        )


        # RAW / BRONZE
        #
        # Every Pub/Sub message is preserved.
        # No validation is performed before this write.

        (
            messages
            | "CreateRawRecords"
            >> beam.ParDo(
                CreateRawRecord()
            )
            | "WriteRawTelemetry"
            >> WriteToBigQuery(
                table=known_args.raw_table,
                schema=RAW_SCHEMA,
                write_disposition=(
                    beam.io.BigQueryDisposition.WRITE_APPEND
                ),
                create_disposition=(
                    beam.io.BigQueryDisposition.CREATE_NEVER
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
                    beam.io.BigQueryDisposition.WRITE_APPEND
                ),
                create_disposition=(
                    beam.io.BigQueryDisposition.CREATE_NEVER
                ),
            )
        )

   
        # EVENT TIME
        windowed = (
            valid

            | "AssignEventTimestamp"
            >> beam.Map(
                lambda record:
                beam.window.TimestampedValue(
                    record,
                    parse_timestamp(
                        record[
                            "event_timestamp"
                        ]
                    ).timestamp(),
                )
            )

            | "FiveMinuteEventWindows"
            >> beam.WindowInto(
                FixedWindows(
                    WINDOW_SECONDS
                ),
                allowed_lateness=(
                    ALLOWED_LATENESS_SECONDS
                ),
            )
        )

       
        # STATEFUL SEMANTIC DEDUPLICATION
        deduplicated = (
            windowed

            | "KeyBySemanticDedupKey"
            >> beam.Map(
                lambda record: (
                    record["dedup_key"],
                    record,
                )
            )

            | "StatefulSemanticDeduplication"
            >> beam.ParDo(
                StatefulDeduplicate()
            )
        )

       
        # CURATED / SILVER
        (
            deduplicated

            | "WriteCuratedTelemetry"
            >> WriteToBigQuery(
                table=known_args.curated_table,
                schema=CURATED_SCHEMA,
                write_disposition=(
                    beam.io.BigQueryDisposition.WRITE_APPEND
                ),
                create_disposition=(
                    beam.io.BigQueryDisposition.CREATE_NEVER
                ),
            )
        )


if __name__ == "__main__":
    run()