FROM apache/airflow:2.10.5-python3.11

USER airflow

# dbt-postgres pulls in dbt-core automatically
RUN pip install --no-cache-dir "dbt-postgres==1.8.*" "dbt-core>=1.8.0,<2.0"
