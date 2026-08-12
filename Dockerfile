FROM apache/airflow:2.9.3-python3.11

USER airflow

# dbt-postgres pulls in dbt-core automatically
RUN pip install --no-cache-dir dbt-postgres==1.8.*
