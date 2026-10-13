.PHONY: init up down logs trigger test test-spark lint format dbt-local query dashboard clean \
	k8s-up k8s-cluster k8s-images k8s-load k8s-secrets k8s-deploy k8s-airflow k8s-dashboard k8s-password \
	k8s-trigger k8s-status k8s-down

init:
	@test -f .env || (umask 077 && cp .env.example .env && \
		sed -i.bak "s/^AIRFLOW_UID=.*/AIRFLOW_UID=$$(id -u)/" .env && \
		sed -i.bak "s/^AIRFLOW_JWT_SECRET=.*/AIRFLOW_JWT_SECRET=$$(openssl rand -hex 32)/" .env && \
		sed -i.bak "s/^AIRFLOW_API_SECRET_KEY=.*/AIRFLOW_API_SECRET_KEY=$$(openssl rand -hex 32)/" .env && \
		sed -i.bak "s/^POSTGRES_PASSWORD=.*/POSTGRES_PASSWORD=$$(openssl rand -hex 24)/" .env && \
		sed -i.bak "s/^S3_ADMIN_SECRET_KEY=.*/S3_ADMIN_SECRET_KEY=$$(openssl rand -hex 24)/" .env && \
		sed -i.bak "s/^S3_PIPELINE_SECRET_KEY=.*/S3_PIPELINE_SECRET_KEY=$$(openssl rand -hex 24)/" .env && \
		sed -i.bak "s/^S3_DASHBOARD_SECRET_KEY=.*/S3_DASHBOARD_SECRET_KEY=$$(openssl rand -hex 24)/" .env && \
		sed -i.bak "s/^S3_LOGS_SECRET_KEY=.*/S3_LOGS_SECRET_KEY=$$(openssl rand -hex 24)/" .env && \
		chmod 600 .env && rm -f .env.bak && echo ".env criado")

up: init
	docker compose up -d --build

down:
	docker compose down

dashboard:
	@echo "Painel: http://localhost:$$(grep -E '^DASHBOARD_PORT=' .env | cut -d= -f2 || echo 8501)"

logs:
	docker compose logs -f airflow-scheduler

trigger:
	docker compose exec airflow-scheduler airflow dags unpause economic_lakehouse
	docker compose exec airflow-scheduler airflow dags trigger economic_lakehouse

test:
	pytest

# Os testes de Spark precisam de Java, então rodam dentro da imagem do Airflow (o pytest é instalado numa pasta temporária).
test-spark:
	docker compose run --rm --no-deps -v "$(CURDIR):/project:ro" -e PIP_USER=false --entrypoint bash airflow-scheduler -c \
		"/opt/airflow/spark-venv/bin/pip install -q --target /tmp/pytest pytest==9.1.1 && cd /project && \
		PYTHONPATH=/tmp/pytest /opt/airflow/spark-venv/bin/python -m pytest -p no:cacheprovider tests/spark"

lint:
	ruff check .
	ruff format --check .

format:
	ruff check . --fix
	ruff format .

dbt-local:
	set -a && . ./.env && set +a && \
		export S3_ACCESS_KEY="$$S3_PIPELINE_ACCESS_KEY" S3_SECRET_KEY="$$S3_PIPELINE_SECRET_KEY" && \
		cd dbt_project && dbt build --profiles-dir . --target local

# Lê o mart da camada gold que o dbt exporta para o object storage.
query:
	docker compose exec airflow-scheduler /opt/airflow/dbt-venv/bin/python -c \
		"import duckdb, os; c = duckdb.connect(); \
		c.sql(f\"create secret (type s3, key_id '{os.environ['S3_ACCESS_KEY']}', secret '{os.environ['S3_SECRET_KEY']}', endpoint 'seaweedfs:8333', url_style 'path', use_ssl false)\"); \
		print(c.sql(\"select month, product, avg_price, real_avg_price, fuel_price_index, ipca_index, usd_brl_index from 's3://gold/marts/mart_fuel_vs_macro.parquet' order by product, month desc limit 20\"))"

clean:
	docker compose down -v
	rm -rf logs/* warehouse/*.duckdb* dbt_project/target dbt_project/logs

# ---------------------------------------------------------------------------------------------------------
# Kubernetes (Fase 3): cluster local com kind, Helm chart oficial do Airflow, Spark e dbt como pods.
# Requer kind, kubectl e helm.
# ---------------------------------------------------------------------------------------------------------
K8S_NS := lakehouse
KIND_CLUSTER := lakehouse
IMAGE_TAG := dev
AIRFLOW_CHART_VERSION := 1.22.0
IMAGES := airflow-k8s spark dbt dashboard

k8s-up: init k8s-cluster k8s-images k8s-load k8s-secrets k8s-deploy
	@echo "Pronto. Airflow: make k8s-airflow (senha: make k8s-password) | Painel: make k8s-dashboard"

k8s-cluster:
	@kind get clusters | grep -qx $(KIND_CLUSTER) || kind create cluster --config k8s/kind-config.yaml

k8s-images:
	docker build -f docker/airflow/Dockerfile --target kubernetes -t brazil-economic-lakehouse/airflow-k8s:$(IMAGE_TAG) .
	docker build -f docker/spark/Dockerfile -t brazil-economic-lakehouse/spark:$(IMAGE_TAG) .
	docker build -f docker/dbt/Dockerfile -t brazil-economic-lakehouse/dbt:$(IMAGE_TAG) .
	docker build -f docker/dashboard/Dockerfile -t brazil-economic-lakehouse/dashboard:$(IMAGE_TAG) dashboard

k8s-load:
	for image in $(IMAGES); do \
		kind load docker-image brazil-economic-lakehouse/$$image:$(IMAGE_TAG) --name $(KIND_CLUSTER) || exit 1; \
	done

k8s-secrets:
	sh k8s/create-secrets.sh

k8s-deploy:
	kubectl apply -k k8s/base
	kubectl -n $(K8S_NS) rollout status deployment/postgres deployment/seaweedfs --timeout=300s
	kubectl -n $(K8S_NS) wait --for=condition=complete job/seaweedfs-create-buckets --timeout=300s
	helm repo add apache-airflow https://airflow.apache.org --force-update
	helm upgrade --install airflow apache-airflow/airflow --version $(AIRFLOW_CHART_VERSION) \
		--namespace $(K8S_NS) -f k8s/airflow-values.yaml --wait --timeout 15m

k8s-airflow:
	kubectl -n $(K8S_NS) port-forward svc/airflow-api-server 8080:8080

k8s-dashboard:
	kubectl -n $(K8S_NS) port-forward svc/dashboard 8501:8501

k8s-password:
	@kubectl -n $(K8S_NS) get secret airflow-auth -o 'jsonpath={.data.passwords\.json}' | base64 -d; echo

k8s-trigger:
	kubectl -n $(K8S_NS) exec deploy/airflow-scheduler -- airflow dags unpause economic_lakehouse
	kubectl -n $(K8S_NS) exec deploy/airflow-scheduler -- airflow dags trigger economic_lakehouse

k8s-status:
	kubectl -n $(K8S_NS) get pods

k8s-down:
	kind delete cluster --name $(KIND_CLUSTER)
