from __future__ import annotations

from datetime import date
from typing import TYPE_CHECKING

from lakehouse.exceptions import SourceUnavailableError

if TYPE_CHECKING:
    import requests

BASE_URL = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.{code}/dados"
MAX_WINDOW_YEARS = 5


def date_windows(start: date, end: date, years: int = MAX_WINDOW_YEARS) -> list[tuple[date, date]]:
    windows = []
    current = start
    while current <= end:
        window_end = min(date(current.year + years - 1, 12, 31), end)
        windows.append((current, window_end))
        current = date.fromordinal(window_end.toordinal() + 1)
    return windows


def fetch_series(session: requests.Session, code: int, start: date, end: date, timeout: int = 60) -> list[dict]:
    records: list[dict] = []
    for window_start, window_end in date_windows(start, end):
        response = session.get(
            BASE_URL.format(code=code),
            params={
                "formato": "json",
                "dataInicial": window_start.strftime("%d/%m/%Y"),
                "dataFinal": window_end.strftime("%d/%m/%Y"),
            },
            timeout=timeout,
        )
        if response.status_code == 404:
            continue
        response.raise_for_status()
        try:
            records.extend(response.json())
        except ValueError as exc:
            # A API do SGS às vezes responde 200 com HTML ou corpo vazio quando está sobrecarregada.
            raise SourceUnavailableError(
                f"A série {code} do BCB devolveu uma resposta que não é JSON "
                f"({response.headers.get('Content-Type')}, {len(response.content)} bytes): {response.text[:120]!r}"
            ) from exc
    return records
