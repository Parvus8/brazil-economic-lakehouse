from importlib.util import find_spec
from pathlib import Path

import pytest

# Os testes de Spark precisam de Java e do virtualenv do Spark (pyspark, great_expectations); fora dele são ignorados.
collect_ignore = [] if find_spec("pyspark") and find_spec("great_expectations") else ["spark"]


@pytest.fixture
def fixtures_dir() -> Path:
    return Path(__file__).parent / "fixtures"
