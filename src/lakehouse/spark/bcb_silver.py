"""Bronze -> silver das séries do BCB: o snapshot JSON mais recente de cada série na tabela Iceberg lake.bcb.series."""

import logging
import os

import great_expectations.expectations as gxe
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from lakehouse.config import BCB_SERIES
from lakehouse.exceptions import EmptyDatasetError, SchemaMismatchError
from lakehouse.spark.quality import validate
from lakehouse.spark.session import CATALOG, build_spark, list_files, write_partitions

log = logging.getLogger(__name__)

TABLE = f"{CATALOG}.bcb.series"
DDL = f"""
CREATE TABLE IF NOT EXISTS {TABLE} (
    series_name STRING,
    series_code BIGINT,
    reference_date DATE,
    value DOUBLE,
    _source_file STRING,
    _ingested_at TIMESTAMP
)
USING iceberg
PARTITIONED BY (series_name)
"""

# Faixas plausíveis por série; valores fora delas costumam indicar problema de unidade ou de leitura na origem.
VALUE_BOUNDS = {
    "selic_target": (0.0, 100.0),
    "ipca_monthly": (-10.0, 10.0),
    "usd_brl": (0.5, 20.0),
}


def parse_series(raw: DataFrame, series_name: str, series_code: int) -> DataFrame:
    if not {"data", "valor"} <= set(raw.columns):
        if raw.isEmpty():
            raise EmptyDatasetError(f"Nenhuma linha para a série {series_name} do BCB")
        raise SchemaMismatchError(f"Campos ausentes na resposta do BCB: {sorted({'data', 'valor'} - set(raw.columns))}")

    return (
        raw.select(
            F.try_to_timestamp(F.col("data"), F.lit("dd/MM/yyyy")).cast("date").alias("reference_date"),
            F.col("valor").try_cast("double").alias("value"),
        )
        .dropna()
        .dropDuplicates(["reference_date"])
        .withColumn("series_name", F.lit(series_name))
        .withColumn("series_code", F.lit(series_code).cast("bigint"))
    )


def expectations(series_name: str) -> list:
    low, high = VALUE_BOUNDS[series_name]
    return [
        gxe.ExpectTableRowCountToBeBetween(min_value=1),
        gxe.ExpectColumnValuesToNotBeNull(column="reference_date"),
        gxe.ExpectColumnValuesToNotBeNull(column="value"),
        gxe.ExpectColumnValuesToBeUnique(column="reference_date"),
        gxe.ExpectColumnValuesToBeBetween(column="value", min_value=low, max_value=high),
    ]


def latest_snapshot(spark: SparkSession, bucket: str, series_name: str) -> str:
    # Datas ISO no nome da partição ordenam cronologicamente, então o último caminho é o snapshot mais recente.
    snapshots = list_files(spark, f"s3a://{bucket}/bcb/{series_name}/ingestion_date=*/{series_name}.json")
    if not snapshots:
        raise EmptyDatasetError(f"Nenhum snapshot na bronze para a série {series_name} do BCB")
    return snapshots[-1]


def run(spark: SparkSession, bucket: str) -> int:
    frames = []
    for series_name, series_code in BCB_SERIES.items():
        path = latest_snapshot(spark, bucket, series_name)
        df = parse_series(spark.read.option("multiLine", True).json(path), series_name, series_code)
        validate(df, f"bcb_{series_name}", expectations(series_name))
        frames.append(df.withColumn("_source_file", F.lit(path)))

    silver = frames[0]
    for frame in frames[1:]:
        silver = silver.unionByName(frame)
    silver = silver.withColumn("_ingested_at", F.current_timestamp()).cache()

    write_partitions(silver, TABLE, DDL)
    rows = silver.count()
    log.info("%s linhas gravadas em %s", rows, TABLE)
    return rows


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")
    spark = build_spark("bcb_bronze_to_silver")
    try:
        run(spark, os.getenv("BRONZE_BUCKET", "bronze"))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
