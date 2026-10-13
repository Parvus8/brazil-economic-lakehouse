import io
import zipfile
from datetime import date

import pytest
import requests

from lakehouse.exceptions import (
    DownloadInterruptedError,
    DownloadTooLargeError,
    SchemaMismatchError,
    SourceUnavailableError,
)
from lakehouse.sources.anp import (
    build_url,
    download,
    download_period,
    list_periods,
    normalize_column,
    unpack_csv,
)


class FakeResponse:
    def __init__(self, chunks, headers=None, status_code=200):
        self.chunks = chunks
        self.headers = headers or {"Content-Type": "text/csv"}
        self.status_code = status_code

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def raise_for_status(self):
        pass

    def iter_content(self, chunk_size):
        for chunk in self.chunks:
            if isinstance(chunk, Exception):
                raise chunk
            yield chunk


class FakeSession:
    def __init__(self, response=None, by_suffix=None, sequence=None):
        self.response = response
        self.by_suffix = by_suffix or {}
        self.sequence = list(sequence or [])
        self.urls = []
        self.headers = []

    def get(self, url, **kwargs):
        self.urls.append(url)
        self.headers.append(kwargs.get("headers") or {})
        if self.sequence:
            return self.sequence.pop(0)
        for suffix, response in self.by_suffix.items():
            if url.endswith(suffix):
                return response
        return self.response or FakeResponse([], status_code=404)


def make_zip(members: dict[str, bytes]) -> io.BytesIO:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for name, data in members.items():
            zf.writestr(name, data)
    buffer.seek(0)
    return buffer


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("Regiao - Sigla", "regiao_sigla"),
        ("\ufeffRegião - Sigla", "regiao_sigla"),
        ("Município", "municipio"),
        ("CNPJ da Revenda", "cnpj_da_revenda"),
    ],
)
def test_normalize_column(raw, expected):
    assert normalize_column(raw) == expected


def test_list_periods_until_current_semester():
    periods = list_periods(2024, date(2025, 8, 10))
    assert periods == [
        {"year": 2024, "semester": 1},
        {"year": 2024, "semester": 2},
        {"year": 2025, "semester": 1},
        {"year": 2025, "semester": 2},
    ]


def test_list_periods_first_semester():
    assert list_periods(2026, date(2026, 3, 1)) == [{"year": 2026, "semester": 1}]


def test_build_url_pads_semester():
    assert build_url("https://x/ca-{year}-{semester:02d}.{ext}", 2024, 1, "zip") == "https://x/ca-2024-01.zip"


def test_download_streams_into_destination():
    dest = io.BytesIO()
    written = download(FakeSession(FakeResponse([b"a;b\n", b"1;2\n"])), "https://x/f.csv", dest, max_bytes=100)
    assert written == 8
    assert dest.getvalue() == b"a;b\n1;2\n"


def test_download_rejects_declared_size_above_limit():
    response = FakeResponse([b"x"], headers={"Content-Type": "text/csv", "Content-Length": "1000"})
    with pytest.raises(DownloadTooLargeError):
        download(FakeSession(response), "https://x/f.csv", io.BytesIO(), max_bytes=10)


def test_download_rejects_stream_above_limit_without_content_length():
    with pytest.raises(DownloadTooLargeError):
        download(FakeSession(FakeResponse([b"x" * 6, b"x" * 6])), "https://x/f.csv", io.BytesIO(), max_bytes=10)


def test_unpack_csv_streams_the_single_csv_member():
    dest = io.BytesIO()
    assert unpack_csv(make_zip({"precos.csv": b"a;b\n1;2\n"}), dest, max_bytes=100) == 8
    assert dest.getvalue() == b"a;b\n1;2\n"


def test_unpack_csv_rejects_archives_without_exactly_one_csv():
    with pytest.raises(SchemaMismatchError):
        unpack_csv(make_zip({"a.csv": b"1", "b.csv": b"2"}), io.BytesIO(), max_bytes=100)
    with pytest.raises(SchemaMismatchError):
        unpack_csv(make_zip({"readme.txt": b"1"}), io.BytesIO(), max_bytes=100)


def test_unpack_csv_caps_uncompressed_size():
    # 1 MB de zeros compacta para cerca de 1 KB: o limite precisa valer para o tamanho descompactado.
    with pytest.raises(DownloadTooLargeError):
        unpack_csv(make_zip({"bomb.csv": b"0" * 1024 * 1024}), io.BytesIO(), max_bytes=1000)


def test_download_period_prefers_zip():
    session = FakeSession(by_suffix={".zip": FakeResponse([b"PK"], {"Content-Type": "application/zip"})})
    dest = io.BytesIO()
    assert download_period(session, "https://x/ca-{year}-{semester:02d}.{ext}", 2024, 1, dest, 100) == "zip"
    assert dest.getvalue() == b"PK"


def test_download_period_falls_back_to_csv():
    session = FakeSession(by_suffix={".csv": FakeResponse([b"a;b\n"])})
    dest = io.BytesIO()
    assert download_period(session, "https://x/ca-{year}-{semester:02d}.{ext}", 2021, 2, dest, 100) == "csv"
    assert session.urls == ["https://x/ca-2021-02.zip", "https://x/ca-2021-02.csv"]
    assert dest.getvalue() == b"a;b\n"


def test_download_period_raises_when_no_format_is_published():
    with pytest.raises(SourceUnavailableError):
        download_period(FakeSession(), "https://x/ca-{year}-{semester:02d}.{ext}", 2027, 1, io.BytesIO(), 100)


def test_download_period_uses_known_override():
    session = FakeSession(
        by_suffix={"precos-semestrais-ca.zip": FakeResponse([b"PK"], {"Content-Type": "application/zip"})}
    )
    dest = io.BytesIO()
    file_format = download_period(
        session,
        "https://x/ca-{year}-{semester:02d}.{ext}",
        2022,
        1,
        dest,
        100,
        overrides={(2022, 1): "https://x/precos-semestrais-ca.zip"},
    )
    assert file_format == "zip"
    assert session.urls == ["https://x/precos-semestrais-ca.zip"]
    assert dest.getvalue() == b"PK"


def test_download_period_ignores_overrides_for_other_periods():
    session = FakeSession(by_suffix={".zip": FakeResponse([b"PK"], {"Content-Type": "application/zip"})})
    overrides = {(2022, 1): "https://x/precos-semestrais-ca.zip"}
    download_period(
        session, "https://x/ca-{year}-{semester:02d}.{ext}", 2023, 1, io.BytesIO(), 100, overrides=overrides
    )
    assert session.urls == ["https://x/ca-2023-01.zip"]


def dropped() -> requests.exceptions.ChunkedEncodingError:
    return requests.exceptions.ChunkedEncodingError("Connection broken: IncompleteRead")


def test_download_resumes_with_range_after_a_dropped_connection():
    first = FakeResponse([b"abc", dropped()], {"Content-Type": "text/csv", "Content-Length": "6"})
    resumed = FakeResponse([b"def"], {"Content-Range": "bytes 3-5/6"}, status_code=206)
    session = FakeSession(sequence=[first, resumed])
    dest = io.BytesIO()

    assert download(session, "https://x/f.csv", dest, max_bytes=100) == 6
    assert dest.getvalue() == b"abcdef"
    assert "Range" not in session.headers[0]
    assert session.headers[1]["Range"] == "bytes=3-"
    assert session.headers[1]["Accept-Encoding"] == "identity"


def test_download_resumes_when_the_body_ends_early():
    first = FakeResponse([b"abc"], {"Content-Type": "text/csv", "Content-Length": "6"})
    resumed = FakeResponse([b"def"], {"Content-Range": "bytes 3-5/6"}, status_code=206)
    dest = io.BytesIO()
    assert download(FakeSession(sequence=[first, resumed]), "https://x/f.csv", dest, max_bytes=100) == 6
    assert dest.getvalue() == b"abcdef"


def test_download_rejects_a_resume_at_the_wrong_offset():
    first = FakeResponse([b"abc", dropped()], {"Content-Type": "text/csv", "Content-Length": "6"})
    wrong = FakeResponse([b"xdef"], {"Content-Range": "bytes 2-5/6"}, status_code=206)
    with pytest.raises(DownloadInterruptedError, match="esperado 3"):
        download(FakeSession(sequence=[first, wrong]), "https://x/f.csv", io.BytesIO(), max_bytes=100)


def test_download_rejects_a_server_that_ignores_range():
    first = FakeResponse([b"abc", dropped()], {"Content-Type": "text/csv", "Content-Length": "6"})
    full = FakeResponse([b"abcdef"], {"Content-Type": "text/csv"}, status_code=200)
    with pytest.raises(DownloadInterruptedError, match="ignorou o pedido de Range"):
        download(FakeSession(sequence=[first, full]), "https://x/f.csv", io.BytesIO(), max_bytes=100)


def test_download_gives_up_after_max_resumes():
    responses = [FakeResponse([b"a", dropped()], {"Content-Type": "text/csv", "Content-Length": "9"})] + [
        FakeResponse([b"a", dropped()], {"Content-Range": f"bytes {i}-8/9"}, status_code=206) for i in range(1, 4)
    ]
    with pytest.raises(DownloadInterruptedError, match="continuou caindo"):
        download(FakeSession(sequence=responses), "https://x/f.csv", io.BytesIO(), max_bytes=100, max_resumes=3)
