import os
from datetime import UTC, date, datetime, timedelta

from airflow.sdk import Param, dag, get_current_context, task
from airflow.sdk.exceptions import AirflowSkipException

from lakehouse import jobs
from lakehouse.config import BCB_SERIES, get_settings
from lakehouse.exceptions import SourceUnavailableError
from lakehouse.sources import anp

# "compose": Spark e dbt rodam dentro do worker do Airflow (uma imagem, modo local).
# "kubernetes": Spark e dbt rodam em pods próprios, com imagens próprias, via KubernetesPodOperator.
RUNTIME = os.getenv("LAKEHOUSE_RUNTIME", "compose")

DBT_BIN = "/opt/airflow/dbt-venv/bin/dbt"
DBT_DIR = "/opt/airflow/dbt"
SPARK_PYTHON = "/opt/airflow/spark-venv/bin/python"
# Pool de uma vaga criado pelo airflow-init: os drivers locais do Spark rodam um de cada vez, para limitar
# a memória. No Kubernetes, os requests de recursos dos pods fazem esse papel.
SPARK_POOL = "spark"


def run_date() -> date:
    logical_date = get_current_context().get("logical_date")
    return (logical_date or datetime.now(UTC)).date()


def pod_task(task_id: str, image: str, cmds: list[str], memory: str, env: dict | None = None, **kwargs):
    from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
    from kubernetes.client import models as k8s

    return KubernetesPodOperator(
        task_id=task_id,
        name=task_id.replace("_", "-"),
        namespace=os.environ["LAKEHOUSE_NAMESPACE"],
        image=image,
        image_pull_policy="IfNotPresent",
        cmds=cmds,
        env_vars={"S3_ENDPOINT_URL": os.environ["S3_ENDPOINT_URL"], **(env or {})},
        # As credenciais do S3 vêm de um Secret do Kubernetes, nunca da DAG nem de variáveis do Airflow.
        env_from=[k8s.V1EnvFromSource(secret_ref=k8s.V1SecretEnvSource(name="lakehouse-pipeline-s3"))],
        container_resources=k8s.V1ResourceRequirements(
            requests={"cpu": "500m", "memory": memory}, limits={"memory": memory}
        ),
        security_context=k8s.V1PodSecurityContext(run_as_non_root=True),
        container_security_context=k8s.V1SecurityContext(
            allow_privilege_escalation=False, capabilities=k8s.V1Capabilities(drop=["ALL"])
        ),
        get_logs=True,
        on_finish_action="delete_succeeded_pod",
        **kwargs,
    )


@dag(
    dag_id="economic_lakehouse",
    schedule="@weekly",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "data-engineering", "retries": 2, "retry_delay": timedelta(minutes=5)},
    params={"full_refresh": Param(False, type="boolean")},
    tags=["bcb", "anp", "lakehouse", "spark", "iceberg"],
)
def economic_lakehouse():
    @task
    def list_bcb_series() -> list[str]:
        return list(BCB_SERIES)

    @task(max_active_tis_per_dag=3)
    def extract_bcb(series_name: str) -> dict:
        return jobs.extract_bcb_series(series_name, run_date())

    @task
    def list_anp_periods() -> list[dict]:
        return anp.list_periods(get_settings().start_year, run_date())

    @task(max_active_tis_per_dag=2)
    def extract_anp(period: dict) -> dict:
        try:
            return jobs.extract_anp_period(period)
        except SourceUnavailableError as exc:
            raise AirflowSkipException(str(exc)) from exc

    if RUNTIME == "kubernetes":
        spark_image = os.environ["LAKEHOUSE_SPARK_IMAGE"]
        silver_bcb = pod_task("silver_bcb", spark_image, ["python", "-m", "lakehouse.spark.bcb_silver"], "2Gi")
        silver_anp = pod_task(
            "silver_anp",
            spark_image,
            ["python", "-m", "lakehouse.spark.anp_silver"],
            "3Gi",
            env={"FULL_REFRESH": "{{ params.full_refresh }}", "SPARK_DRIVER_MEMORY": "2g"},
            trigger_rule="none_failed",
        )
        dbt_build = pod_task(
            "dbt_build",
            os.environ["LAKEHOUSE_DBT_IMAGE"],
            ["dbt", "build", "--target", "docker"],
            "1Gi",
            env={"DUCKDB_S3_ENDPOINT": os.environ["S3_ENDPOINT_URL"].removeprefix("http://")},
            trigger_rule="none_failed",
        )
    else:

        @task.bash(pool=SPARK_POOL)
        def silver_bcb_local() -> str:
            return f"{SPARK_PYTHON} -m lakehouse.spark.bcb_silver"

        @task.bash(pool=SPARK_POOL, trigger_rule="none_failed")
        def silver_anp_local() -> str:
            full_refresh = bool(get_current_context()["params"]["full_refresh"])
            return f"{SPARK_PYTHON} -m lakehouse.spark.anp_silver" + (" --full-refresh" if full_refresh else "")

        @task.bash(trigger_rule="none_failed")
        def dbt_build_local() -> str:
            return f"{DBT_BIN} build --project-dir {DBT_DIR} --profiles-dir {DBT_DIR} --target docker"

        silver_bcb = silver_bcb_local.override(task_id="silver_bcb")()
        silver_anp = silver_anp_local.override(task_id="silver_anp")()
        dbt_build = dbt_build_local.override(task_id="dbt_build")()

    bcb_bronze = extract_bcb.expand(series_name=list_bcb_series())
    anp_bronze = extract_anp.expand(period=list_anp_periods())

    [bcb_bronze >> silver_bcb, anp_bronze >> silver_anp] >> dbt_build


economic_lakehouse()
