#!/bin/sh
# Cria ou atualiza os Secrets do Kubernetes a partir do .env local, então nenhuma credencial é commitada.
# Os valores passam por arquivos temporários (modo 600) em vez de argumentos de linha de comando, que outros
# usuários locais poderiam ver na lista de processos. Valores gerados (chave Fernet, senha do admin) são
# mantidos nas reexecuções.
set -eu

NS=${K8S_NAMESPACE:-lakehouse}
[ -f .env ] || { echo ".env não encontrado: rode 'make init' primeiro" >&2; exit 1; }
set -a
. ./.env
set +a

umask 077
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT

apply_secret() {
  name=$1
  shift
  kubectl -n "$NS" create secret generic "$name" "$@" --dry-run=client -o yaml | kubectl apply -f - >/dev/null
  echo "secret/$name configurado"
}

existing_key() {
  kubectl -n "$NS" get secret "$1" -o "jsonpath={.data.$2}" 2>/dev/null | base64 -d 2>/dev/null || true
}

kubectl apply -f k8s/base/namespace.yaml >/dev/null

# Identidades do object storage (vão para o arquivo de identidades do SeaweedFS na inicialização).
cat > "$tmp/s3.env" <<EOF
S3_ADMIN_ACCESS_KEY=$S3_ADMIN_ACCESS_KEY
S3_ADMIN_SECRET_KEY=$S3_ADMIN_SECRET_KEY
S3_PIPELINE_ACCESS_KEY=$S3_PIPELINE_ACCESS_KEY
S3_PIPELINE_SECRET_KEY=$S3_PIPELINE_SECRET_KEY
S3_DASHBOARD_ACCESS_KEY=$S3_DASHBOARD_ACCESS_KEY
S3_DASHBOARD_SECRET_KEY=$S3_DASHBOARD_SECRET_KEY
S3_LOGS_ACCESS_KEY=$S3_LOGS_ACCESS_KEY
S3_LOGS_SECRET_KEY=$S3_LOGS_SECRET_KEY
EOF
apply_secret lakehouse-s3 --from-env-file="$tmp/s3.env"

# Credenciais por consumidor: cada workload recebe só a própria identidade.
printf 'S3_ACCESS_KEY=%s\nS3_SECRET_KEY=%s\n' "$S3_PIPELINE_ACCESS_KEY" "$S3_PIPELINE_SECRET_KEY" > "$tmp/pipeline.env"
apply_secret lakehouse-pipeline-s3 --from-env-file="$tmp/pipeline.env"
printf 'S3_ACCESS_KEY=%s\nS3_SECRET_KEY=%s\n' "$S3_DASHBOARD_ACCESS_KEY" "$S3_DASHBOARD_SECRET_KEY" > "$tmp/dashboard.env"
apply_secret lakehouse-dashboard-s3 --from-env-file="$tmp/dashboard.env"

# Banco de metadados do Airflow.
printf 'POSTGRES_PASSWORD=%s\n' "$POSTGRES_PASSWORD" > "$tmp/postgres.env"
apply_secret postgres --from-env-file="$tmp/postgres.env"
printf '%s' "postgresql+psycopg2://airflow:${POSTGRES_PASSWORD}@postgres/airflow" > "$tmp/connection"
apply_secret airflow-metadata --from-file=connection="$tmp/connection"

# Chaves do Airflow. A chave Fernet criptografa as conexões guardadas no banco, então é gerada uma única vez.
fernet=$(existing_key airflow-secrets fernet-key)
[ -n "$fernet" ] || fernet=$(openssl rand -base64 32 | tr '+/' '-_')
printf '%s' "$fernet" > "$tmp/fernet-key"
printf '%s' "$AIRFLOW_JWT_SECRET" > "$tmp/jwt-secret"
printf '%s' "$AIRFLOW_API_SECRET_KEY" > "$tmp/api-secret-key"
apply_secret airflow-secrets \
  --from-file=fernet-key="$tmp/fernet-key" \
  --from-file=jwt-secret="$tmp/jwt-secret" \
  --from-file=api-secret-key="$tmp/api-secret-key"

# Senha do admin da interface do Airflow (simple auth manager); consulte com `make k8s-password`.
passwords=$(existing_key airflow-auth passwords\\.json)
[ -n "$passwords" ] || passwords="{\"admin\": \"$(openssl rand -hex 16)\"}"
printf '%s' "$passwords" > "$tmp/passwords.json"
apply_secret airflow-auth --from-file=passwords.json="$tmp/passwords.json"

# Conexão dos logs remotos (o Airflow grava os logs das tarefas no bucket airflow-logs com a identidade logs).
cat > "$tmp/logs-connection" <<EOF
{"conn_type": "aws", "login": "$S3_LOGS_ACCESS_KEY", "password": "$S3_LOGS_SECRET_KEY", "extra": {"endpoint_url": "http://seaweedfs:8333", "region_name": "us-east-1"}}
EOF
apply_secret airflow-logs-connection --from-file=connection="$tmp/logs-connection"

kubectl -n "$NS" create configmap seaweedfs-scripts --from-file=docker/seaweedfs \
  --dry-run=client -o yaml | kubectl apply -f - >/dev/null
echo "configmap/seaweedfs-scripts configurado"
