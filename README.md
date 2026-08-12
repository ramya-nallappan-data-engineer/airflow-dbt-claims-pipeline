# Healthcare Claims Pipeline — Airflow + dbt Portfolio Project

A small but real end-to-end pipeline: raw healthcare claims → dbt
transformations (staging → intermediate → marts) → orchestrated by
Airflow with a data-quality gate. Built to mirror the kind of
pipeline work described in the author's professional experience
(healthcare data platforms, ETL/ELT, reliability practices).

## What this project demonstrates
- dbt: seeds, staging/intermediate/marts layering, tests, `ref()`
- Airflow: DAGs, sensors, BashOperator, task dependencies, scheduling
- A real (if small) data quality problem: duplicate records, nulls —
  and how the pipeline handles them

---

## Prerequisites
- Docker Desktop installed and running
- Git installed
- ~15 minutes for first-time image builds

---

## Step 0 — Initialize git (do this first)

```bash
cd healthcare-claims-pipeline
git init
git add .
git commit -m "Initial project scaffold: docker-compose, dbt project, Airflow DAG"
```

Commit early and often through every step below — a commit history
that shows the pipeline being built incrementally is itself useful
signal in an interview ("walk me through how you built this").

---

## Step 1 — Get dbt talking to Postgres (no Airflow yet)

This is the fastest path to something *working*, and it's how you'll
actually learn the dbt concepts before Airflow adds a layer of
complexity on top.

1. Start just Postgres:
   ```bash
   docker compose up -d postgres
   ```

2. Install dbt locally (outside Docker, for this step only):
   ```bash
   pip install dbt-postgres
   ```

3. Set up your profile:
   ```bash
   mkdir -p ~/.dbt
   cp dbt/claims_project/profiles.yml.example ~/.dbt/profiles.yml
   ```

4. Run it:
   ```bash
   cd dbt/claims_project
   dbt seed        # loads seeds/raw_claims.csv into Postgres
   dbt run         # builds stg_claims -> int_claims_deduped -> marts
   dbt test        # runs the tests defined in schema.yml
   ```

5. Check your work — connect to Postgres (`psql -h localhost -U airflow -d claims_warehouse`)
   and query `select * from fct_claims_by_provider;`. You should see
   one row per provider with claim counts and recovery rates.

**Concept check before moving on** — make sure you can answer:
- Why does `stg_claims` not remove the duplicate `C1003` row, but
  `int_claims_deduped` does? *(Answer: staging should be a thin,
  faithful copy of the source — business logic like deduplication
  belongs in intermediate, so you can always trace a bug back to
  "did the source do this, or did our logic do this?")*
- What does `{{ ref('stg_claims') }}` actually do at run time?
  *(It resolves to the real, environment-specific table name — this
  is what makes the same dbt code work in dev/staging/prod without
  edits.)*

**Commit:**
```bash
git add .
git commit -m "dbt pipeline working end-to-end locally: seed -> staging -> intermediate -> marts, tests passing"
```

---

## Step 2 — Get Airflow running (still not connected to your DAG's logic yet)

1. Build and start everything:
   ```bash
   docker compose up --build
   ```
   First build takes several minutes (installing dbt into the Airflow image).

2. Open [http://localhost:8080](http://localhost:8080) — log in with
   `admin` / `admin`.

3. You should see `healthcare_claims_pipeline` in the DAG list
   (paused by default). Don't unpause yet.

**Concept check:**
- What's the difference between the *scheduler* and the *webserver*?
  *(Scheduler decides what runs when and executes tasks; webserver
  is just the UI — killing the webserver doesn't stop your pipeline.)*

**Commit:**
```bash
git add .
git commit -m "Airflow environment running locally via docker-compose"
```

---

## Step 3 — Connect Airflow to dbt and run the real DAG

1. In the Airflow UI, unpause `healthcare_claims_pipeline` and trigger
   it manually (the play button).

2. Watch the task flow in the Graph view:
   `check_for_new_file → dbt_seed → dbt_run → dbt_test → log_pipeline_complete`

3. Click into `dbt_run`'s logs — you'll see the actual `dbt run`
   output, same as when you ran it locally in Step 1. This is the
   moment the two tools "click" — Airflow isn't doing anything
   magical, it's just running the same CLI commands you already
   understand, on a schedule, with retry/logging/dependency handling
   wrapped around them.

**Concept check:**
- If `dbt_test` fails, does `log_pipeline_complete` run?
  *(No — that's the point of the `>>` dependency chain. This is your
  data-quality gate: nothing downstream should treat the mart as
  trustworthy if tests failed.)*
- Why use `FileSensor` before `dbt_seed` instead of just running
  `dbt_seed` first? *(In a real system, the file might not have
  landed yet — the sensor mirrors waiting on an upstream drop, and
  is a very common real-world pattern to know.)*

**Commit:**
```bash
git add .
git commit -m "End-to-end DAG run successful in Airflow UI: seed -> run -> test -> log"
```

---

## Step 4 — Break something on purpose (this is where real learning happens)

Try each of these, one at a time, and observe what happens:

1. Edit `seeds/raw_claims.csv` and add a row with a `NULL` `claim_id`.
   Rerun — `dbt_test` should fail on the `not_null` test on
   `stg_claims.claim_id`. Look at the failure output.
2. Comment out the `check_for_new_file >>` dependency and see the
   DAG structure change in the Graph view.
3. Temporarily typo a column name in `fct_claims_by_provider.sql`
   and watch `dbt_run` fail with a real SQL compile error — this is
   normal, and reading these errors quickly is a core day-to-day
   skill.

Undo your changes, confirm the pipeline is green again, then:

```bash
git add .
git commit -m "Verified failure modes: null constraint, broken dependency, SQL compile error"
```

---

## Step 5 — Push to GitHub

```bash
git remote add origin <your-empty-github-repo-url>
git branch -M main
git push -u origin main
```

Your commit history now tells the story of how you built this — that's
worth more on a resume/portfolio than a single "final" commit.

---

## What to say about this project in an interview

- "I built an orchestrated ELT pipeline — Airflow triggers a dbt
  project that stages, deduplicates, and aggregates healthcare claims
  data, with dbt tests acting as a quality gate before the mart is
  considered ready."
- "I deliberately included a duplicate-record scenario to practice
  handling the kind of data quality issue you actually see in claims
  feeds — resubmissions and batch reprocessing."
- Mention the layered dbt structure (staging/intermediate/marts) by
  name — it signals you know the modeling convention, not just the
  tool.

## Natural next steps (Phase 2+ in the learning path)
- Swap the `FileSensor` for an S3/GCS sensor to make the ingestion
  realistic
- Add a Terraform file to provision the Postgres/warehouse resources
  (Phase 2 of your learning path)
- Add a small Kafka producer feeding a streaming claims topic
  (Phase 3)
