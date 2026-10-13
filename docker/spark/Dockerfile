# Imagem dos jobs Spark bronze -> silver quando rodam como pods no Kubernetes. Contexto de build: raiz do repositório.
FROM python:3.12-slim-bookworm

RUN apt-get update \
    && apt-get install -y --no-install-recommends default-jre-headless curl ca-certificates \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/default-java

COPY docker/spark/fetch-jars.sh /tmp/fetch-jars.sh
RUN sh /tmp/fetch-jars.sh /opt/spark-jars

COPY docker/spark/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

RUN useradd --uid 10001 --create-home spark
COPY src /opt/lakehouse/src
ENV PYTHONPATH=/opt/lakehouse/src \
    SPARK_JARS_DIR=/opt/spark-jars \
    GX_ANALYTICS_ENABLED=false

USER 10001
WORKDIR /home/spark
ENTRYPOINT ["python", "-m"]
