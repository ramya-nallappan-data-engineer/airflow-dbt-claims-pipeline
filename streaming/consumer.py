"""
Consumes claim events from Kafka and lands them in a Postgres table,
`streaming_claims` -- an independent ingestion path from the batch
dbt pipeline, landing raw events as they arrive rather than on a
schedule.
"""
import json

import psycopg2
from kafka import KafkaConsumer

TOPIC = "claims_events"

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS streaming_claims (
    claim_id        VARCHAR PRIMARY KEY,
    patient_id      VARCHAR,
    provider_id     VARCHAR,
    provider_name   VARCHAR,
    claim_date      TIMESTAMPTZ,
    procedure_code  VARCHAR,
    diagnosis_code  VARCHAR,
    billed_amount   NUMERIC,
    paid_amount     NUMERIC,
    claim_status    VARCHAR,
    ingested_at     TIMESTAMPTZ DEFAULT now()
);
"""

INSERT_SQL = """
INSERT INTO streaming_claims
    (claim_id, patient_id, provider_id, provider_name, claim_date,
     procedure_code, diagnosis_code, billed_amount, paid_amount, claim_status)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (claim_id) DO NOTHING;
"""


def main():
    conn = psycopg2.connect(
        host="localhost", port=5432, user="airflow", password="airflow",
        dbname="claims_warehouse",
    )
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute(CREATE_TABLE_SQL)

    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers="localhost:9092",
        auto_offset_reset="earliest",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        group_id="claims-streaming-consumer",
    )
    print(f"Consuming from topic '{TOPIC}' -- Ctrl+C to stop")
    try:
        for message in consumer:
            event = message.value
            with conn.cursor() as cur:
                cur.execute(INSERT_SQL, (
                    event["claim_id"], event["patient_id"], event["provider_id"],
                    event["provider_name"], event["claim_date"], event["procedure_code"],
                    event["diagnosis_code"], event["billed_amount"], event["paid_amount"],
                    event["claim_status"],
                ))
            print(f"Landed: {event['claim_id']} | {event['provider_id']} | {event['claim_status']}")
    except KeyboardInterrupt:
        print("\nStopped.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
