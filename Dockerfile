FROM apache/airflow:2.10.5

COPY --chown=airflow:root requirements.txt /requirements.txt

USER airflow

RUN pip install --no-cache-dir -r /requirements.txt