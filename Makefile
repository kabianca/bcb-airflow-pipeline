AIRFLOW_VERSION ?= 3.3.1
COMPOSE_URL = https://airflow.apache.org/docs/apache-airflow/$(AIRFLOW_VERSION)/docker-compose.yaml

.DEFAULT_GOAL := help
.PHONY: help init up down logs test lint backfill psql clean reset

help:  ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
	 | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'

init:  ## Download the official Airflow compose file and prepare folders
	@test -f docker-compose.yaml || curl -fsSL "$(COMPOSE_URL)" -o docker-compose.yaml
	@mkdir -p dags include sql data logs plugins
	@test -f .env || (cp .env.example .env && \
	  echo "AIRFLOW_UID=$$(id -u)" >> .env && \
	  echo "created .env (AIRFLOW_UID=$$(id -u))")
	@echo "ready — now run: make up"

up: init  ## Start Airflow + warehouse
	docker compose up -d
	@echo "Airflow UI: http://localhost:8080"

down:  ## Stop everything (keeps volumes)
	docker compose down

logs:  ## Tail the scheduler
	docker compose logs -f airflow-scheduler

test:  ## Run the test suite (no Docker, no network needed)
	python -m pytest -q

lint:  ## Static checks
	python -m ruff check . || true
	python -m ruff format --check . || true

backfill:  ## Backfill a window: make backfill FROM=2026-08-01 TO=2026-08-31
	docker compose run --rm airflow-cli airflow backfill create \
	  --dag-id bcb_series_ingest --from-date $(FROM) --to-date $(TO)

psql:  ## Open a shell on the warehouse
	docker compose exec warehouse psql -U warehouse -d warehouse

clean:  ## Remove landed files and logs
	rm -rf data/raw logs/*

reset: down  ## Destroy volumes and start clean (DELETES the warehouse)
	docker compose down -v
	rm -rf data/raw logs/*