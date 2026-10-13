import gzip
import json
import shutil
import tempfile
from datetime import date

from lakehouse.config import ANP_URL_OVERRIDES, BCB_SERIES, Settings, get_settings
from lakehouse.http import build_session
from lakehouse.sources import anp, bcb
from lakehouse.storage import ObjectStore


def spooled_file() -> tempfile.SpooledTemporaryFile:
    return tempfile.SpooledTemporaryFile(max_size=64 * 1024 * 1024)


def bcb_bronze_key(series_name: str, run_date: date) -> str:
    return f"bcb/{series_name}/ingestion_date={run_date.isoformat()}/{series_name}.json"


def anp_bronze_key(year: int, semester: int) -> str:
    return f"anp/fuel_prices/year={year}/semester={semester}/ca-{year}-{semester:02d}.csv.gz"


def extract_bcb_series(series_name: str, run_date: date, settings: Settings | None = None) -> dict:
    settings = settings or get_settings()
    store = ObjectStore(settings)
    records = bcb.fetch_series(
        build_session(),
        BCB_SERIES[series_name],
        date(settings.start_year, 1, 1),
        run_date,
        settings.http_timeout,
    )
    key = bcb_bronze_key(series_name, run_date)
    store.put_bytes(settings.bronze_bucket, key, json.dumps(records).encode("utf-8"))
    return {"series_name": series_name, "bronze_key": key, "records": len(records)}


def extract_anp_period(period: dict, settings: Settings | None = None) -> dict:
    settings = settings or get_settings()
    store = ObjectStore(settings)
    year, semester = period["year"], period["semester"]
    key = anp_bronze_key(year, semester)
    downloaded = False
    if not store.exists(settings.bronze_bucket, key):
        # Os buffers passam para o disco acima de 64 MB, então arquivos grandes nunca ficam inteiros na memória.
        with spooled_file() as raw, spooled_file() as packed:
            file_format = anp.download_period(
                build_session(),
                settings.anp_url_template,
                year,
                semester,
                raw,
                settings.anp_max_download_bytes,
                settings.http_timeout,
                ANP_URL_OVERRIDES,
            )
            raw.seek(0)
            # A bronze guarda o CSV da fonte byte a byte, compactado em gzip: o Spark lê gzip, mas não zip.
            with gzip.GzipFile(fileobj=packed, mode="wb", mtime=0) as csv_gz:
                if file_format == "zip":
                    anp.unpack_csv(raw, csv_gz, settings.anp_max_download_bytes)
                else:
                    shutil.copyfileobj(raw, csv_gz)
            packed.seek(0)
            store.put_fileobj(settings.bronze_bucket, key, packed)
        downloaded = True
    return {"year": year, "semester": semester, "bronze_key": key, "downloaded": downloaded}
