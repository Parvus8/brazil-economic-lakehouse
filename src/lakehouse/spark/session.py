import os
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

from lakehouse.config import required_env

CATALOG = "lake"
REPLACEMENT_CHAR = "\ufffd"


def build_spark(app_name: str, driver_memory: str = "1g") -> SparkSession:
    # O S3A lê as credenciais do ambiente, então elas nunca aparecem na configuração do Spark.
    os.environ["AWS_ACCESS_KEY_ID"] = required_env("S3_ACCESS_KEY")
    os.environ["AWS_SECRET_ACCESS_KEY"] = required_env("S3_SECRET_KEY")
    endpoint = os.getenv("S3_ENDPOINT_URL", "http://seaweedfs:8333")
    jars = sorted(Path(os.getenv("SPARK_JARS_DIR", "/opt/spark-jars")).glob("*.jar"))

    return (
        SparkSession.builder.appName(app_name)
        .master(os.getenv("SPARK_MASTER", "local[2]"))
        .config("spark.driver.memory", os.getenv("SPARK_DRIVER_MEMORY", driver_memory))
        .config("spark.jars", ",".join(str(jar) for jar in jars))
        .config("spark.ui.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.sql.shuffle.partitions", os.getenv("SPARK_SHUFFLE_PARTITIONS", "8"))
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config(f"spark.sql.catalog.{CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{CATALOG}.type", "hadoop")
        .config(f"spark.sql.catalog.{CATALOG}.warehouse", os.getenv("ICEBERG_WAREHOUSE", "s3a://silver/warehouse"))
        .config("spark.hadoop.fs.s3a.endpoint", endpoint)
        .config("spark.hadoop.fs.s3a.endpoint.region", "us-east-1")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", str(endpoint.startswith("https")).lower())
        .getOrCreate()
    )


def list_files(spark: SparkSession, pattern: str) -> list[str]:
    jvm_path = spark._jvm.org.apache.hadoop.fs.Path(pattern)
    fs = jvm_path.getFileSystem(spark._jsc.hadoopConfiguration())
    statuses = fs.globStatus(jvm_path) or []
    return sorted(status.getPath().toString() for status in statuses if status.isFile())


def detect_encoding(spark: SparkSession, path: str, sample_lines: int = 5000) -> str:
    # O Spark decodifica texto como UTF-8 e troca bytes inválidos, então caracteres de substituição indicam Latin-1.
    sample = spark.read.text(path).limit(sample_lines).collect()
    return "ISO-8859-1" if any(REPLACEMENT_CHAR in row.value for row in sample) else "UTF-8"


def write_partitions(df: DataFrame, table: str, ddl: str) -> None:
    namespace = table.rsplit(".", 1)[0]
    df.sparkSession.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace}")
    df.sparkSession.sql(ddl)
    # Sobrescrita dinâmica: só as partições presentes em df são substituídas, o que torna as reexecuções idempotentes.
    df.writeTo(table).overwritePartitions()
