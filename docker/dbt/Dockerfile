# Imagem do dbt quando roda como pod no Kubernetes. Contexto de build: raiz do repositório.
# O arquivo do DuckDB é efêmero: os modelos de staging e intermediate são reconstruídos a partir da silver a
# cada execução e os marts são exportados para o bucket gold, então o pod não precisa de volume persistente.
FROM python:3.12-slim-bookworm

RUN useradd --uid 10001 --create-home dbt
COPY docker/dbt/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

USER 10001
# As extensões do DuckDB vêm embutidas na imagem, para o pod não precisar acessar o repositório de extensões.
RUN python -c "import duckdb; duckdb.sql('INSTALL httpfs'); duckdb.sql('INSTALL iceberg')"

COPY --chown=10001:10001 dbt_project /opt/dbt
WORKDIR /opt/dbt
ENV DBT_PROFILES_DIR=/opt/dbt \
    DBT_TARGET_PATH=/tmp/dbt/target \
    DBT_LOG_PATH=/tmp/dbt/logs \
    DBT_SEND_ANONYMOUS_USAGE_STATS=false \
    DUCKDB_PATH=/tmp/warehouse.duckdb
ENTRYPOINT ["dbt"]
