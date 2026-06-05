AIRFLOW_COMPOSE_CMD = docker compose --env-file .env_file/airflow.env -f airflow-docker-compose.yml
INFRA_COMPOSE_CMD = docker compose --env-file .env_file/infra.env -f infra-docker-compose.yml

# -a: relate to Airflow
# -i: relate to Infrastructure
# -v: relate to containers' volume

up-a:
	$(AIRFLOW_COMPOSE_CMD) up -d

down-a:
	$(AIRFLOW_COMPOSE_CMD) down

down-av:
	$(AIRFLOW_COMPOSE_CMD) down -v

restart-a:
	$(AIRFLOW_COMPOSE_CMD) restart

up-i:
	$(INFRA_COMPOSE_CMD) up -d

down-i:
	$(INFRA_COMPOSE_CMD) down

down-iv:
	$(INFRA_COMPOSE_CMD) down -v

restart-i:
	$(INFRA_COMPOSE_CMD) restart

compile:
	uv pip compile requirements.in -o requirements.txt

compile-u:
	uv pip compile requirements.in -o requirements.txt --upgrade

install:
	uv pip install -r requirements.txt