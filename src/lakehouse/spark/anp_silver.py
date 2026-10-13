"""Bronze -> silver dos preços de combustíveis da ANP: CSVs semestrais na tabela Iceberg lake.anp.fuel_prices."""

import argparse
import logging
import os
import re

import great_expectations.expectations as gxe
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from lakehouse.exceptions import EmptyDatasetError, SchemaMismatchError
from lakehouse.sources.anp import COLUMN_MAP, normalize_column
from lakehouse.spark.quality import validate
from lakehouse.spark.session import CATALOG, build_spark, detect_encoding, list_files, write_partitions

log = logging.getLogger(__name__)

TABLE = f"{CATALOG}.anp.fuel_prices"
# Particionada pelo período de origem: cada arquivo da bronze vira exatamente uma partição, então reprocessar
# um arquivo substitui as próprias linhas e nunca mexe em dados que vieram de outros arquivos.
DDL = f"""
CREATE TABLE IF NOT EXISTS {TABLE} (
    region STRING,
    state STRING,
    city STRING,
    reseller STRING,
    reseller_cnpj STRING,
    product STRING,
    collected_at DATE,
    sale_price DOUBLE,
    unit STRING,
    brand STRING,
    period_year INT,
    period_semester INT,
    _source_file STRING,
    _ingested_at TIMESTAMP
)
USING iceberg
PARTITIONED BY (period_year, period_semester)
"""

PERIOD_PATTERN = re.compile(r"year=(\d{4})/semester=([12])/")
BRAZIL_STATES = [
    "AC", "AL", "AM", "AP", "BA", "CE", "DF", "ES", "GO", "MA", "MG", "MS", "MT", "PA",
    "PB", "PE", "PI", "PR", "RJ", "RN", "RO", "RR", "RS", "SC", "SE", "SP", "TO",
]  # fmt: skip


def parse_prices(raw: DataFrame) -> DataFrame:
    df = raw.toDF(*[normalize_column(column) for column in raw.columns])
    missing = set(COLUMN_MAP) - set(df.columns)
    if missing:
        raise SchemaMismatchError(f"Colunas ausentes no arquivo da ANP: {sorted(missing)}")

    df = df.select([F.col(source).alias(target) for source, target in COLUMN_MAP.items()])
    return (
        df.withColumns({column: F.trim(column) for column in ("region", "state", "city", "reseller", "unit", "brand")})
        .withColumns(
            {
                "product": F.upper(F.trim("product")),
                "reseller_cnpj": F.regexp_replace("reseller_cnpj", r"\D", ""),
                "collected_at": F.try_to_timestamp(F.trim("collected_at"), F.lit("dd/MM/yyyy")).cast("date"),
                "sale_price": F.regexp_replace(F.trim("sale_price"), ",", ".").try_cast("double"),
            }
        )
        .dropna(subset=["collected_at", "sale_price", "product", "state"])
        .filter(F.col("sale_price") > 0)
    )


def expectations() -> list:
    return [
        gxe.ExpectTableRowCountToBeBetween(min_value=1),
        *[gxe.ExpectColumnValuesToNotBeNull(column=c) for c in ("collected_at", "sale_price", "product", "state")],
        gxe.ExpectColumnValuesToBeInSet(column="state", value_set=BRAZIL_STATES),
        gxe.ExpectColumnValuesToBeBetween(column="sale_price", min_value=0.5, max_value=50, mostly=0.999),
        gxe.ExpectColumnValueLengthsToEqual(column="reseller_cnpj", value=14, mostly=0.99),
    ]


def source_period(path: str) -> tuple[int, int]:
    match = PERIOD_PATTERN.search(path)
    if not match:
        raise ValueError(f"Não foi possível identificar o período de {path}")
    return int(match.group(1)), int(match.group(2))


def processed_periods(spark: SparkSession) -> set[tuple[int, int]]:
    if not spark.catalog.tableExists(TABLE):
        return set()
    # Lê apenas os metadados do Iceberg; nenhum arquivo de dados é lido.
    rows = spark.sql(f"SELECT partition.period_year, partition.period_semester FROM {TABLE}.partitions").collect()
    return {(row.period_year, row.period_semester) for row in rows}


def read_period(spark: SparkSession, path: str) -> DataFrame:
    year, semester = source_period(path)
    raw = spark.read.options(header=True, sep=";", encoding=detect_encoding(spark, path)).csv(path)
    return parse_prices(raw).withColumns(
        {
            "period_year": F.lit(year),
            "period_semester": F.lit(semester),
            "_source_file": F.lit(path),
        }
    )


def run(spark: SparkSession, bucket: str, full_refresh: bool = False) -> int:
    files = list_files(spark, f"s3a://{bucket}/anp/fuel_prices/year=*/semester=*/*.csv.gz")
    done = set() if full_refresh else processed_periods(spark)
    pending = [path for path in files if source_period(path) not in done]
    log.info("%s arquivos na bronze, %s já na silver, %s a processar", len(files), len(done), len(pending))
    if not pending:
        return 0

    frames = [read_period(spark, path) for path in pending]
    silver = frames[0]
    for frame in frames[1:]:
        silver = silver.unionByName(frame)
    silver = silver.withColumn("_ingested_at", F.current_timestamp()).cache()
    if silver.isEmpty():
        raise EmptyDatasetError(f"Nenhuma linha lida de {pending}")

    validate(silver, "anp_fuel_prices", expectations())
    write_partitions(silver, TABLE, DDL)
    rows = silver.count()
    log.info("%s linhas gravadas em %s", rows, TABLE)
    return rows


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--full-refresh", action="store_true", help="Reprocessa todos os arquivos da bronze")
    args = parser.parse_args()
    # No Kubernetes o parâmetro da DAG chega como variável de ambiente renderizada pelo pod operator.
    full_refresh = args.full_refresh or os.getenv("FULL_REFRESH", "false").strip().lower() == "true"

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")
    spark = build_spark("anp_bronze_to_silver", driver_memory="2g")
    try:
        run(spark, os.getenv("BRONZE_BUCKET", "bronze"), full_refresh=full_refresh)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
