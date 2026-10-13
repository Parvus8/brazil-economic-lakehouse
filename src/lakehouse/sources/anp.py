from __future__ import annotations

import logging
import re
import unicodedata
import zipfile
from collections.abc import Iterable
from datetime import date
from typing import BinaryIO

import requests

from lakehouse.exceptions import (
    DownloadInterruptedError,
    DownloadTooLargeError,
    SchemaMismatchError,
    SourceUnavailableError,
)

log = logging.getLogger(__name__)

# A ANP trocou os CSVs por arquivos ZIP em 2022, então os dois formatos são tentados, o mais novo primeiro.
FILE_FORMATS = ("zip", "csv")
CHUNK_SIZE = 1024 * 1024
MAX_RESUMES = 5
CONTENT_RANGE = re.compile(r"bytes (\d+)-\d+/(\d+|\*)")

COLUMN_MAP = {
    "regiao_sigla": "region",
    "estado_sigla": "state",
    "municipio": "city",
    "revenda": "reseller",
    "cnpj_da_revenda": "reseller_cnpj",
    "produto": "product",
    "data_da_coleta": "collected_at",
    "valor_de_venda": "sale_price",
    "unidade_de_medida": "unit",
    "bandeira": "brand",
}


def list_periods(start_year: int, today: date) -> list[dict]:
    current_semester = 1 if today.month <= 6 else 2
    periods = []
    for year in range(start_year, today.year + 1):
        for semester in (1, 2):
            if (year, semester) > (today.year, current_semester):
                break
            periods.append({"year": year, "semester": semester})
    return periods


def build_url(template: str, year: int, semester: int, file_format: str) -> str:
    return template.format(year=year, semester=semester, ext=file_format)


def copy_limited(chunks: Iterable[bytes], dest: BinaryIO, max_bytes: int, label: str) -> int:
    written = 0
    for chunk in chunks:
        written += len(chunk)
        if written > max_bytes:
            raise DownloadTooLargeError(f"{label} passou do limite de {max_bytes} bytes")
        dest.write(chunk)
    return written


def _resume_offset(response: requests.Response, url: str) -> tuple[int, int | None]:
    """Converte `Content-Range: bytes <início>-<fim>/<total>` em (início, total)."""
    match = CONTENT_RANGE.fullmatch(response.headers.get("Content-Range", "").strip())
    if not match:
        raise DownloadInterruptedError(f"O servidor ignorou o pedido de Range: {url}")
    return int(match.group(1)), int(match.group(2)) if match.group(2).isdigit() else None


def download(
    session: requests.Session,
    url: str,
    dest: BinaryIO,
    max_bytes: int,
    timeout: int = 120,
    chunk_size: int = CHUNK_SIZE,
    max_resumes: int = MAX_RESUMES,
) -> int:
    """Baixa url em streaming para dest. Conexões que caem são retomadas com HTTP Range a partir do último
    byte gravado, porque o servidor encerra no meio as transferências longas dos CSVs antigos da ANP."""
    start = dest.tell()
    total = None
    for attempt in range(max_resumes + 1):
        written = dest.tell() - start
        # identity: as posições do Range precisam se referir ao próprio arquivo, não a uma transferência compactada.
        headers = {"Accept-Encoding": "identity"}
        if written:
            headers["Range"] = f"bytes={written}-"
        try:
            with session.get(url, timeout=timeout, stream=True, headers=headers) as response:
                if not written:
                    if response.status_code == 404:
                        raise SourceUnavailableError(f"Arquivo ainda não publicado: {url}")
                    response.raise_for_status()
                    if "text/html" in response.headers.get("Content-Type", ""):
                        raise SourceUnavailableError(f"Esperava um arquivo de dados, mas recebeu HTML: {url}")
                    content_length = response.headers.get("Content-Length", "")
                    total = int(content_length) if content_length.isdigit() else None
                else:
                    response.raise_for_status()
                    offset, total = _resume_offset(response, url)
                    if offset != written:
                        raise DownloadInterruptedError(f"O servidor retomou {url} no byte {offset}, esperado {written}")

                if total is not None and total > max_bytes:
                    raise DownloadTooLargeError(f"{url} tem {total} bytes, o limite é {max_bytes}")
                copy_limited(response.iter_content(chunk_size=chunk_size), dest, max_bytes - written, url)
        except (requests.exceptions.ChunkedEncodingError, requests.exceptions.ConnectionError) as exc:
            if attempt == max_resumes:
                raise DownloadInterruptedError(f"{url} continuou caindo após {max_resumes} retomadas") from exc
            log.warning("Download de %s interrompido em %s bytes, retomando (%s)", url, dest.tell() - start, exc)
            continue

        written = dest.tell() - start
        if total is None or written == total:
            return written
        log.warning("Download de %s terminou antes, em %s de %s bytes, retomando", url, written, total)
    raise DownloadInterruptedError(f"{url} continua incompleto após {max_resumes} retomadas")


def download_period(
    session: requests.Session,
    template: str,
    year: int,
    semester: int,
    dest: BinaryIO,
    max_bytes: int,
    timeout: int = 120,
    overrides: dict[tuple[int, int], str] | None = None,
) -> str:
    """Baixa o arquivo de um semestre no primeiro formato disponível e devolve esse formato."""
    override = (overrides or {}).get((year, semester))
    if override:
        download(session, override, dest, max_bytes, timeout)
        return override.rsplit(".", 1)[-1].lower()

    for file_format in FILE_FORMATS:
        dest.seek(0)
        dest.truncate()
        try:
            download(session, build_url(template, year, semester, file_format), dest, max_bytes, timeout)
            return file_format
        except SourceUnavailableError:
            continue
    raise SourceUnavailableError(
        f"Nenhum arquivo da ANP publicado ainda para {year}-S{semester} ({'/'.join(FILE_FORMATS)})"
    )


def unpack_csv(archive: BinaryIO, dest: BinaryIO, max_bytes: int) -> int:
    """Extrai em streaming o único CSV de um arquivo da ANP para dest, com limite no tamanho descompactado."""
    with zipfile.ZipFile(archive) as zf:
        members = [m for m in zf.infolist() if not m.is_dir() and m.filename.lower().endswith(".csv")]
        if len(members) != 1:
            names = [m.filename for m in members]
            raise SchemaMismatchError(f"Esperava exatamente um CSV no arquivo da ANP, encontrou {names}")
        member = members[0]
        if member.file_size > max_bytes:
            raise DownloadTooLargeError(f"{member.filename} tem {member.file_size} bytes, o limite é {max_bytes}")
        # O tamanho no cabeçalho do zip pode ser forjado (zip bomb), então o limite também vale durante a leitura.
        with zf.open(member) as source:
            return copy_limited(iter(lambda: source.read(CHUNK_SIZE), b""), dest, max_bytes, member.filename)


def normalize_column(name: str) -> str:
    name = name.replace("﻿", "").strip()
    name = unicodedata.normalize("NFKD", name).encode("ascii", "ignore").decode("ascii")
    name = re.sub(r"[^a-zA-Z0-9]+", "_", name.lower())
    return name.strip("_")
