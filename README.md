# Healthcare Claims Pipeline: Airflow + dbt

An orchestrated ELT pipeline for healthcare claims data. Airflow schedules a dbt project that stages, de-duplicates and aggregates raw claims into a provider-level mart, and dbt tests act as a quality gate before the mart is considered ready.

> Synthetic data only. Built to show pipeline design patterns (layering, data-quality gating, orchestration, infrastructure as code), not to process real patient data.

## Architecture

```
raw_claims.csv (dbt seed)
        |
        v
  FileSensor  -->  dbt seed  -->  dbt run  -->  dbt test  -->  log_pipeline_complete
   (Airflow)                       |              (quality gate)
                                   v
                  staging -> intermediate -> marts   (Postgres)
```

| Layer | Model | Purpose |
|---|---|---|
| Staging | `stg_claims` | Thin, faithful copy of the source. No business logic. |
| Intermediate | `int_claims_deduped` | Removes duplicate claim records (for example resubmissions). |
| Marts | `dim_providers`, `fct_claims_by_provider` | Provider dimension and claim counts and recovery rates per provider. |

## Design decisions

- **Staging stays thin.** Deduplication lives in the intermediate layer, so a bug can always be traced to either "the source did this" or "our logic did this".
- **Tests gate the mart.** In the Airflow DAG, `dbt_test` runs before the completion step. If tests fail, nothing downstream treats the mart as trustworthy.
- **A deliberate data-quality problem.** The seed includes duplicate and null records, which mirror issues common in claims feeds.
- **Simple local infrastructure.** `LocalExecutor` with Postgres in Docker. This is a portfolio setup, not a production deployment.

## Orchestration

- DAG: `healthcare_claims_pipeline`, scheduled `@daily`, `catchup=False`, one retry with a two-minute delay.
- Tasks: `check_for_new_file` (a `FileSensor`) -> `dbt_seed` -> `dbt_run` -> `dbt_test` -> `log_pipeline_complete`.
- The sensor mirrors a real upstream-file wait pattern. In this project the "upstream file" is the static seed.

## Infrastructure as code

`terraform/` uses the `cyrilgdn/postgresql` provider to create a read-only reporting role (`claims_reporting`) with `SELECT` access. It mirrors provisioning a BI or analyst service account on a warehouse.

## Streaming (experimental)

`streaming/producer.py` publishes simulated claim events to a Kafka topic (`claims_events`), and `streaming/consumer.py` lands them in a `streaming_claims` table in Postgres. It runs independently of the scheduled batch pipeline.

## Quickstart

Prerequisites: Docker Desktop, Git, about 15 minutes for first image builds.

```bash
git clone https://github.com/ramya-nallappan-data-engineer/airflow-dbt-claims-pipeline.git
cd airflow-dbt-claims-pipeline
docker compose up --build
```

1. Open `http://localhost:8080` (local-only demo login: `admin` / `admin`).
2. Unpause `healthcare_claims_pipeline` and trigger it.
3. Check the result in Postgres:

```sql
select * from fct_claims_by_provider;
```

Running dbt without Airflow:

```bash
docker compose up -d postgres
pip install dbt-postgres
mkdir -p ~/.dbt && cp dbt/claims_project/profiles.yml.example ~/.dbt/profiles.yml
cd dbt/claims_project
dbt seed && dbt run && dbt test
```

## Repository layout

```
dags/                  Airflow DAG
dbt/claims_project/    dbt project: seeds, models (staging, intermediate, marts), tests
streaming/             Kafka producer and consumer
terraform/             Postgres role provisioning
docker-compose.yml     Postgres, Airflow, Kafka
```

## Limitations and roadmap

- [ ] Replace the file sensor with an object-store sensor (S3 or GCS).
- [ ] CI: run `dbt build` on pull requests.
- [ ] Larger synthetic dataset and incremental models.
- [ ] Document the Kafka path end to end.

## License

MIT
