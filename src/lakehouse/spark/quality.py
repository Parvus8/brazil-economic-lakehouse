import logging
import os

# Precisa ser definido antes de importar o great_expectations, que senão envia dados anônimos de uso.
os.environ.setdefault("GX_ANALYTICS_ENABLED", "false")

import great_expectations as gx  # noqa: E402
from great_expectations.expectations import Expectation  # noqa: E402
from pyspark.sql import DataFrame  # noqa: E402

from lakehouse.exceptions import DataQualityError  # noqa: E402

log = logging.getLogger(__name__)


def validate(df: DataFrame, name: str, expectations: list[Expectation]) -> dict:
    """Barreira de qualidade: lança DataQualityError se alguma expectativa falhar, antes de gravar na silver."""
    context = gx.get_context(mode="ephemeral")
    batch_definition = (
        context.data_sources.add_spark(name=f"{name}_spark")
        .add_dataframe_asset(name=name)
        .add_batch_definition_whole_dataframe(f"{name}_batch")
    )
    suite = context.suites.add(gx.ExpectationSuite(name=f"{name}_suite", expectations=expectations))
    result = batch_definition.get_batch(batch_parameters={"dataframe": df}).validate(suite)

    summary = {
        "suite": name,
        "success": result.success,
        "evaluated": len(result.results),
        "failed": [
            {
                "expectation": item.expectation_config.type,
                "column": item.expectation_config.kwargs.get("column"),
                "unexpected_percent": item.result.get("unexpected_percent"),
                "observed_value": item.result.get("observed_value"),
            }
            for item in result.results
            if not item.success
        ],
    }
    log.info("Resumo da qualidade dos dados: %s", summary)
    if not result.success:
        raise DataQualityError(f"{len(summary['failed'])} expectativa(s) falharam em {name}: {summary['failed']}")
    return summary
