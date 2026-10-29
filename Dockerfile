ARG AIRFLOW_IMAGE=apache/airflow:2.10.5-python3.11
FROM ${AIRFLOW_IMAGE}

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

WORKDIR /opt/retail
COPY requirements.txt /tmp/retail-requirements.txt
COPY spark/requirements.txt /tmp/spark-requirements.txt
COPY config.py landing.py run_ingestion.py ./
COPY sources ./sources
COPY spark ./spark
COPY dags ./dags
COPY streaming ./streaming

RUN mkdir -p /opt/retail/data \
    && chown -R airflow:root /opt/retail

USER airflow
RUN pip install --no-cache-dir \
      --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.10.5/constraints-3.11.txt" \
      "apache-airflow==2.10.5" \
      "psycopg2-binary>=2.9,<3" \
      -r /tmp/retail-requirements.txt \
      -r /tmp/spark-requirements.txt

ENV AIRFLOW__CORE__DAGS_FOLDER=/opt/retail/dags
ENV PYTHONPATH=/opt/retail
