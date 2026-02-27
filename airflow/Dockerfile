FROM apache/airflow:2.9.3-python3.11

USER airflow

COPY requirements.txt /
RUN pip install --no-cache-dir -r /requirements.txt

RUN mkdir -p /opt/airflow/dags
COPY dags/* /opt/airflow/dags

COPY entrypoint.sh /entrypoint.sh
ENTRYPOINT ["/entrypoint.sh"]
