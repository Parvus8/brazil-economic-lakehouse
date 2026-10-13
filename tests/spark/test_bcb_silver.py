import pytest

from lakehouse.exceptions import DataQualityError, EmptyDatasetError, SchemaMismatchError
from lakehouse.spark.bcb_silver import expectations, parse_series
from lakehouse.spark.quality import validate


@pytest.fixture
def bcb_raw(spark, fixtures_dir):
    return spark.read.option("multiLine", True).json(str(fixtures_dir / "bcb_usd_brl.json"))


def test_parse_series_types_and_deduplication(bcb_raw):
    df = parse_series(bcb_raw, "usd_brl", 1)
    assert df.count() == 3
    assert dict(df.dtypes) == {
        "reference_date": "date",
        "value": "double",
        "series_name": "string",
        "series_code": "bigint",
    }
    assert {row.series_name for row in df.collect()} == {"usd_brl"}


def test_parse_series_empty_payload(spark, tmp_path):
    path = tmp_path / "empty.json"
    path.write_text("[]")
    with pytest.raises(EmptyDatasetError):
        parse_series(spark.read.option("multiLine", True).json(str(path)), "usd_brl", 1)


def test_parse_series_rejects_unexpected_payload(spark):
    with pytest.raises(SchemaMismatchError):
        parse_series(spark.createDataFrame([("2024-01-01", 1.0)], ["date", "value"]), "usd_brl", 1)


def test_quality_gate_passes_on_clean_data(bcb_raw):
    assert validate(parse_series(bcb_raw, "usd_brl", 1), "bcb_test", expectations("usd_brl"))["success"]


def test_quality_gate_blocks_out_of_range_values(spark):
    raw = spark.createDataFrame([("02/01/2024", "489.16")], ["data", "valor"])
    with pytest.raises(DataQualityError, match="expect_column_values_to_be_between"):
        validate(parse_series(raw, "usd_brl", 1), "bcb_out_of_range", expectations("usd_brl"))
