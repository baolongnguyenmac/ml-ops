AIRFLOW_COMPOSE_CMD = docker compose --env-file .env_file/airflow.env -f airflow-docker-compose.yml
INFRA_COMPOSE_CMD = docker compose --env-file .env_file/infra.env -f infra-docker-compose.yml

# -a: relate to Airflow
# -i: relate to Infrastructure
# -v: relate to containers' volume

# airflow
up-a:
	$(AIRFLOW_COMPOSE_CMD) up -d

stop-a:
	$(AIRFLOW_COMPOSE_CMD) stop

down-a:
	$(AIRFLOW_COMPOSE_CMD) down

down-av:
	$(AIRFLOW_COMPOSE_CMD) down -v

restart-a:
	$(AIRFLOW_COMPOSE_CMD) restart

# infra
up-i:
	$(INFRA_COMPOSE_CMD) up -d

stop-i:
	$(INFRA_COMPOSE_CMD) stop

down-i:
	$(INFRA_COMPOSE_CMD) down

down-iv:
	$(INFRA_COMPOSE_CMD) down -v

restart-i:
	$(INFRA_COMPOSE_CMD) restart

## infra - batch processing
up-ib:
	$(INFRA_COMPOSE_CMD) up lh-metastore trino company_db -d

## infra - stream processing
up-is:
	$(INFRA_COMPOSE_CMD) up company_db broker schema-registry connect control-center ksqldb-server rest-proxy -d

# dependencies
compile:
	uv pip compile requirements.in -o requirements.txt

compile-u:
	uv pip compile requirements.in -o requirements.txt --upgrade

install:
	uv pip install -r requirements.txt