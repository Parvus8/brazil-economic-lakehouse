import datetime as dt

import pytest
from pyspark.sql import Row

from lakehouse.exceptions import DataQualityError, SchemaMismatchError
from lakehouse.spark.anp_silver import expectations, parse_prices, source_period
from lakehouse.spark.quality import validate
from lakehouse.spark.session import detect_encoding


@pytest.fixture
def anp_path(fixtures_dir) -> str:
    return str(fixtures_dir / "anp_sample.csv")


@pytest.fixture
def anp_raw(spark, anp_path):
    return spark.read.options(header=True, sep=";", encoding=detect_encoding(spark, anp_path)).csv(anp_path)


def test_detect_encoding_finds_latin1(spark, anp_path):
    assert detect_encoding(spark, anp_path) == "ISO-8859-1"


def test_detect_encoding_defaults_to_utf8(spark, tmp_path):
    path = tmp_path / "utf8.csv"
    path.write_text("Município;Valor\nSão Paulo;1,0\n", encoding="utf-8")
    assert detect_encoding(spark, str(path)) == "UTF-8"


def test_parse_prices_cleans_and_types(anp_raw):
    df = parse_prices(anp_raw)
    rows = {row.product: row for row in df.collect()}

    assert df.count() == 4
    assert dict(df.dtypes)["sale_price"] == "double"
    assert dict(df.dtypes)["collected_at"] == "date"
    assert "GASOLINA ADITIVADA" in rows
    assert rows["GASOLINA"].sale_price == pytest.approx(5.79)
    assert rows["GASOLINA"].collected_at == dt.date(2024, 1, 2)
    assert {len(row.reseller_cnpj) for row in rows.values()} == {14}


def test_parse_prices_rejects_unknown_layout(spark):
    with pytest.raises(SchemaMismatchError):
        parse_prices(spark.createDataFrame([("1", "2", "3")], ["a", "b", "c"]))


def test_quality_gate_passes_on_clean_data(anp_raw):
    summary = validate(parse_prices(anp_raw), "anp_test", expectations())
    assert summary["success"]


def test_quality_gate_blocks_invalid_states(spark):
    df = spark.createDataFrame(
        [
            Row(
                state="XX",
                product="GASOLINA",
                collected_at=dt.date(2024, 1, 2),
                sale_price=5.5,
                reseller_cnpj="12345678000190",
            )
        ]
    )
    with pytest.raises(DataQualityError, match="expect_column_values_to_be_in_set"):
        validate(df, "anp_invalid", expectations())


@pytest.mark.parametrize(
    ("path", "expected"),
    [
        ("s3a://bronze/anp/fuel_prices/year=2024/semester=1/ca-2024-01.csv", (2024, 1)),
        ("s3a://bronze/anp/fuel_prices/year=2023/semester=2/ca-2023-02.csv", (2023, 2)),
    ],
)
def test_source_period(path, expected):
    assert source_period(path) == expected
