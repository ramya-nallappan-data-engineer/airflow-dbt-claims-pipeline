"""
Simulates new healthcare claims arriving in real time and publishes
them to the Kafka topic `claims_events`.

This represents an event-driven ingestion path alongside the batch
dbt pipeline -- e.g. a claims intake system emitting an event the
moment a claim is submitted, rather than waiting for a nightly batch.
"""
import json
import random
import time
from datetime import datetime, timezone

from kafka import KafkaProducer

TOPIC = "claims_events"
PROVIDERS = [
    ("PR01", "Riverside Clinic"),
    ("PR02", "Northgate Medical"),
    ("PR03", "Lakeside Health"),
    ("PR04", "Summit Family Care"),
    ("PR05", "Harborview Pediatrics"),
]
PROCEDURE_DIAGNOSIS = [
    ("99213", "J45.909"),
    ("99214", "E11.9"),
    ("71046", "J18.9"),
    ("80053", "I10"),
    ("99392", "Z00.129"),
]
STATUSES = ["PAID", "DENIED", "PENDING"]


def make_event(claim_seq: int) -> dict:
    provider_id, provider_name = random.choice(PROVIDERS)
    procedure_code, diagnosis_code = random.choice(PROCEDURE_DIAGNOSIS)
    billed = round(random.uniform(80, 400), 2)
    status = random.choice(STATUSES)
    paid = 0.0 if status == "DENIED" else round(billed * random.uniform(0.6, 1.0), 2)
    return {
        "claim_id": f"STREAM{claim_seq:04d}",
        "patient_id": f"P{random.randint(100, 999)}",
        "provider_id": provider_id,
        "provider_name": provider_name,
        "claim_date": datetime.now(timezone.utc).isoformat(),
        "procedure_code": procedure_code,
        "diagnosis_code": diagnosis_code,
        "billed_amount": billed,
        "paid_amount": paid,
        "claim_status": status,
    }


def main():
    producer = KafkaProducer(
        bootstrap_servers="localhost:9092",
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    print(f"Producing to topic '{TOPIC}' -- Ctrl+C to stop")
    seq = 1
    try:
        while True:
            event = make_event(seq)
            producer.send(TOPIC, value=event)
            producer.flush()
            print(f"Sent: {event['claim_id']} | {event['provider_id']} | {event['claim_status']}")
            seq += 1
            time.sleep(2)  # simulate claims arriving every ~2 seconds
    except KeyboardInterrupt:
        print("\nStopped.")
    finally:
        producer.close()


if __name__ == "__main__":
    main()
