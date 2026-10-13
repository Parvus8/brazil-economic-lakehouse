# Contexto de build: raiz do repositório.
ARG AIRFLOW_VERSION=3.3.2
ARG PYTHON_VERSION=3.12
FROM apache/airflow:${AIRFLOW_VERSION}-python${PYTHON_VERSION} AS base

ARG AIRFLOW_VERSION
COPY docker/airflow/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /tmp/requirements.txt


# Kubernetes: imagem enxuta com as DAGs e o pacote lakehouse embutidos.
# Spark e dbt não ficam aqui: rodam em pods próprios (KubernetesPodOperator), com imagens próprias.
FROM base AS kubernetes
COPY --chown=airflow:root src /opt/airflow/src
COPY --chown=airflow:root dags /opt/airflow/dags
ENV PYTHONPATH=/opt/airflow/src


# Docker Compose: uma imagem roda tudo (Airflow, Spark em modo local e dbt), com o código montado do host.
FROM base AS compose

# Java para o driver local do Spark, mais os jars do Iceberg e do S3A (checksums verificados pelo script).
USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends default-jre-headless \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/default-java
COPY docker/spark/fetch-jars.sh /tmp/fetch-jars.sh
RUN sh /tmp/fetch-jars.sh /opt/spark-jars
USER airflow

# Spark e dbt têm virtualenvs próprios, isolados das dependências do Airflow.
COPY docker/dbt/requirements.txt /tmp/requirements-dbt.txt
RUN python -m venv /opt/airflow/dbt-venv \
    && PIP_USER=false /opt/airflow/dbt-venv/bin/pip install --no-cache-dir -r /tmp/requirements-dbt.txt \
    && /opt/airflow/dbt-venv/bin/python -c "import duckdb; duckdb.sql('INSTALL httpfs'); duckdb.sql('INSTALL iceberg')"

COPY docker/spark/requirements.txt /tmp/requirements-spark.txt
RUN python -m venv /opt/airflow/spark-venv \
    && PIP_USER=false /opt/airflow/spark-venv/bin/pip install --no-cache-dir -r /tmp/requirements-spark.txt
