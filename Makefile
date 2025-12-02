DBT := docker compose exec -T dbt dbt --profile pycon25_duckdb
PY := docker compose exec -T dbt python

.PHONY: help build setup seed run test clean up down demo-late-data demo-01 demo-02 demo-03 dbt-shell

help: ## Show this help
	@echo "Usage: make <target>"
	@echo
	@echo "Targets:"
	@awk -F':|##' '/^[a-zA-Z0-9_.-]+:.*##/ { printf "  %-20s %s\n", $$1, $$3 }' $(MAKEFILE_LIST) | sort

build: ## Build container images
	@echo "[docker] Building images"
	docker compose build

setup: build ## Build images and prepare runtime dirs
	@echo "[setup] Creating runtime dirs"
	@mkdir -p data/warehouse data/raw data/partitioned
	@chmod -R 777 data/warehouse data/raw data/partitioned || true

up: ## Start dbt container
	@echo "[docker] Starting dbt container"
	docker compose up -d dbt
	@echo "[docker] dbt container ready"

down: ## Stop dbt container
	@echo "[docker] Stopping dbt container"
	docker compose stop dbt

download-data: ## Download and repartition NYC taxi data (default: Sept-Oct 2025) using DuckDB
	@echo "[download] Downloading and repartitioning NYC Yellow Taxi data"
	$(PY) scripts/download_and_repartition.py --raw-dir /data/raw --partitioned-dir /data/partitioned

run: ## Run dbt models
	@echo "[dbt] Running models"
	$(DBT) run

test: ## Run dbt tests
	@echo "[dbt] Testing models"
	$(DBT) test

clean: ## Clean dbt artifacts and local logs
	@echo "[clean] Removing build artifacts"
	$(DBT) clean || true
	rm -rf target logs

dbt-shell: ## Open a shell into the dbt container
	@echo "[dbt] Opening shell"
	docker compose exec dbt bash

partition-data: ## Partition raw Parquet files by trip_date using dbt model (defaults to SQL version)
	@echo "[partition] Partitioning data by trip_date using dbt model"
	$(DBT) run --select partition_trips_v1

partition-data-sql: ## Partition raw Parquet files using SQL dbt model
	@echo "[partition] Partitioning data using SQL model"
	$(DBT) run --select partition_trips_v1

partition-data-python: ## Partition raw Parquet files using Python dbt model
	@echo "[partition] Partitioning data using Python model"
	$(DBT) run --select partition_trips_v2

demo-01: ## Run Demo v1: Full Batch Processing
	@echo "[demo-v1] Running full batch models"
	$(DBT) run --select stg_trips agg_daily_revenue_v1
	@echo "[demo-v1] Demo complete!"

demo-02: ## Run Demo v2: Incremental Event Processing
	@echo "[demo-v2] Running incremental event models"
	$(DBT) run --select stg_trips_v2 agg_daily_revenue_v2
	@echo "[demo-v2] Demo complete!"

demo-03: ## Run Demo v3: Incremental Aggregation with Partition Overwrite
	@echo "[demo-v3] Running incremental aggregation models"
	$(DBT) run --select stg_trips agg_daily_revenue_v3
	@echo "[demo-v3] Demo complete!"
