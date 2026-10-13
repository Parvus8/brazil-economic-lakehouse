#!/bin/sh
# Gera o arquivo de identidades S3 a partir de variáveis de ambiente (nenhum segredo fica no repositório)
# e inicia o SeaweedFS (master + volume + filer + gateway S3) num único processo.
#
# - admin:     acesso total, para pessoas que inspecionam o lake.
# - pipeline:  menor privilégio, usada por Airflow, Spark e dbt. Só lista, lê, grava e apaga objetos nos
#              buckets bronze, silver e gold (apagar é necessário porque o S3A faz os renames dos commits do
#              Iceberg como cópia + exclusão). Não cria buckets nem acessa outros.
# - dashboard: somente leitura no bucket gold.
# - logs:      opcional (só no Kubernetes), leitura e escrita no bucket airflow-logs, usado nos logs remotos do Airflow.
set -eu
umask 077

logs_identity=""
if [ -n "${S3_LOGS_ACCESS_KEY:-}" ]; then
  logs_identity=$(cat <<EOF
,
    {
      "name": "logs",
      "credentials": [{"accessKey": "${S3_LOGS_ACCESS_KEY}", "secretKey": "${S3_LOGS_SECRET_KEY}"}],
      "actions": ["Read:airflow-logs", "Write:airflow-logs", "List:airflow-logs"]
    }
EOF
)
fi

config=/tmp/s3-identities.json
cat > "$config" <<EOF
{
  "identities": [
    {
      "name": "admin",
      "credentials": [{"accessKey": "${S3_ADMIN_ACCESS_KEY}", "secretKey": "${S3_ADMIN_SECRET_KEY}"}],
      "actions": ["Admin", "Read", "Write", "List", "Tagging"]
    },
    {
      "name": "pipeline",
      "credentials": [{"accessKey": "${S3_PIPELINE_ACCESS_KEY}", "secretKey": "${S3_PIPELINE_SECRET_KEY}"}],
      "actions": [
        "Read:bronze", "Write:bronze", "List:bronze",
        "Read:silver", "Write:silver", "List:silver",
        "Read:gold", "Write:gold", "List:gold"
      ]
    },
    {
      "name": "dashboard",
      "credentials": [{"accessKey": "${S3_DASHBOARD_ACCESS_KEY}", "secretKey": "${S3_DASHBOARD_SECRET_KEY}"}],
      "actions": ["Read:gold", "List:gold"]
    }${logs_identity}
  ]
}
EOF

# SEAWEEDFS_IP é o endereço que os componentes anunciam entre si. No Kubernetes é o IP do pod (downward API),
# para o tráfego interno não precisar voltar pelo Service.
exec weed server \
  -dir=/data \
  -ip="${SEAWEEDFS_IP:-seaweedfs}" \
  -ip.bind=0.0.0.0 \
  -volume.max=0 \
  -master.volumeSizeLimitMB=1024 \
  -s3 \
  -s3.config="$config"
