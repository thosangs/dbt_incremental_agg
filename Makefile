DBT := docker compose exec -T dbt dbt --profile pycon25_spark
PY := docker compose exec -T dbt python

.PHONY: help build setup seed run test clean spark-up spark-down spark-logs demo-late-data demo-01 demo-02 demo-03 dbt-shell

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

download-data: ## Download NYC taxi data (default: 2022-2023) and partition by date
	@echo "[download] Downloading NYC Yellow Taxi data"
	$(PY) scripts/download_nyc_taxi_data.py --output-dir /data/raw
	@echo "[partition] Partitioning data by trip_date using dbt model"
	$(DBT) run --select partition_trips

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

spark-up: ## Start Spark cluster (master, worker, thrift) and dbt containers
	@echo "[spark] Starting Spark cluster"
	docker compose up -d spark-master spark-worker spark-thrift dbt
	@echo "[spark] Waiting for Spark Thrift server to be ready..."
	@sleep 10
	@echo "[spark] Spark cluster ready. Thrift server at localhost:10000"

spark-down: ## Stop Spark cluster containers
	@echo "[spark] Stopping Spark cluster"
	docker compose stop spark-master spark-worker spark-thrift

spark-logs: ## Tail Spark Thrift server logs
	@echo "[spark] Tailing Spark Thrift logs (Ctrl-C to stop)"
	docker compose logs -f spark-thrift | cat

partition-data: ## Partition raw Parquet files by trip_date using dbt model (defaults to SQL version)
	@echo "[partition] Partitioning data by trip_date using dbt model"
	$(DBT) run --select partition_trips

partition-data-sql: ## Partition raw Parquet files using SQL dbt model
	@echo "[partition] Partitioning data using SQL model"
	$(DBT) run --select partition_trips.sql

partition-data-python: ## Partition raw Parquet files using Python dbt model
	@echo "[partition] Partitioning data using Python model"
	$(DBT) run --select partition_trips.py

demo-late-data: ## Download additional older data to simulate late-arriving trips
	@echo "[demo] Downloading older data to simulate late-arriving trips"
	$(PY) scripts/download_nyc_taxi_data.py --start-year 2021 --start-month 1 --end-year 2021 --end-month 3 --output-dir /data/raw --skip-existing
	@echo "[partition] Partitioning late-arriving data using dbt model"
	$(DBT) run --select partition_trips
	@echo "[demo] Late-arriving data downloaded and partitioned. Re-run 'make run' to process."

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
