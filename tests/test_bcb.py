from datetime import date

import pytest

from lakehouse.exceptions import SourceUnavailableError
from lakehouse.sources.bcb import date_windows, fetch_series


def test_date_windows_split_long_ranges():
    windows = date_windows(date(2015, 1, 1), date(2026, 6, 30), years=5)
    assert windows == [
        (date(2015, 1, 1), date(2019, 12, 31)),
        (date(2020, 1, 1), date(2024, 12, 31)),
        (date(2025, 1, 1), date(2026, 6, 30)),
    ]


def test_date_windows_single_window():
    assert date_windows(date(2024, 1, 1), date(2024, 3, 1)) == [(date(2024, 1, 1), date(2024, 3, 1))]


class FakeResponse:
    status_code = 200
    headers = {"Content-Type": "text/html"}
    content = b"<html>busy</html>"
    text = "<html>busy</html>"

    def raise_for_status(self):
        pass

    def json(self):
        raise ValueError("Expecting value")


class FakeSession:
    def get(self, url, **kwargs):
        return FakeResponse()


def test_fetch_series_reports_non_json_responses():
    with pytest.raises(SourceUnavailableError, match="não é JSON"):
        fetch_series(FakeSession(), 1, date(2024, 1, 1), date(2024, 2, 1))
