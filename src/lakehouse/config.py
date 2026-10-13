import os
from dataclasses import dataclass, field

BCB_SERIES = {
    "selic_target": 432,
    "ipca_monthly": 433,
    "usd_brl": 1,
}

ANP_BASE_URL = "https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/arquivos/shpc/dsas/ca"
ANP_URL_TEMPLATE = ANP_BASE_URL + "/ca-{year}-{semester:02d}.{ext}"
# Semestres publicados fora do padrão de nome (conferidos na página de dados abertos da ANP).
ANP_URL_OVERRIDES = {
    (2022, 1): ANP_BASE_URL + "/precos-semestrais-ca.zip",
}


def required_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"A variável de ambiente {name} precisa estar definida")
    return value


@dataclass(frozen=True)
class Settings:
    s3_endpoint_url: str = field(default_factory=lambda: os.getenv("S3_ENDPOINT_URL", "http://seaweedfs:8333"))
    s3_access_key: str = field(default_factory=lambda: required_env("S3_ACCESS_KEY"), repr=False)
    s3_secret_key: str = field(default_factory=lambda: required_env("S3_SECRET_KEY"), repr=False)
    bronze_bucket: str = field(default_factory=lambda: os.getenv("BRONZE_BUCKET", "bronze"))
    silver_bucket: str = field(default_factory=lambda: os.getenv("SILVER_BUCKET", "silver"))
    start_year: int = field(default_factory=lambda: int(os.getenv("PIPELINE_START_YEAR", "2018")))
    anp_url_template: str = field(default_factory=lambda: os.getenv("ANP_URL_TEMPLATE", ANP_URL_TEMPLATE))
    http_timeout: int = field(default_factory=lambda: int(os.getenv("HTTP_TIMEOUT_SECONDS", "120")))
    anp_max_download_bytes: int = field(
        default_factory=lambda: int(os.getenv("ANP_MAX_DOWNLOAD_MB", "1024")) * 1024 * 1024
    )


def get_settings() -> Settings:
    return Settings()
