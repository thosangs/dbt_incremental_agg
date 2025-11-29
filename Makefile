DBT := docker compose exec -T dbt dbt --profiles-dir profiles
PY := docker compose exec -T dbt python

.PHONY: help build setup seed run test clean spark-up spark-down spark-logs sqlpad-up sqlpad-down sqlpad-logs demo-late-data demo-01 demo-02 demo-03 dbt-shell

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
	@mkdir -p data/warehouse data/raw

download-data: ## Download NYC taxi data (default: 2022-2023)
	@echo "[download] Downloading NYC Yellow Taxi data"
	$(PY) scripts/download_nyc_taxi_data.py --output-dir /data/raw

download-data-late: ## Download additional older data for late-arriving demo
	@echo "[download] Downloading older NYC taxi data for late-arriving demo"
	$(PY) scripts/download_nyc_taxi_data.py --start-year 2021 --start-month 1 --end-year 2021 --end-month 3 --output-dir /data/raw

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

sqlpad-up: spark-up ## Start SQLPad (depends on Spark)
	@echo "[sqlpad] Starting SQLPad"
	docker compose up -d sqlpad
	@echo "[sqlpad] SQLPad ready at http://localhost:3000"
	@echo "[sqlpad] Login: admin@example.com / changeme"

sqlpad-down: ## Stop SQLPad container
	@echo "[sqlpad] Stopping SQLPad"
	docker compose stop sqlpad

sqlpad-logs: ## Tail SQLPad logs
	@echo "[sqlpad] Tailing logs (Ctrl-C to stop)"
	docker compose logs -f sqlpad | cat

demo-late-data: ## Download additional older data to simulate late-arriving trips
	@echo "[demo] Downloading older data to simulate late-arriving trips"
	$(PY) scripts/download_nyc_taxi_data.py --start-year 2021 --start-month 1 --end-year 2021 --end-month 3 --output-dir /data/raw --skip-existing
	@echo "[demo] Late-arriving data downloaded. Re-run 'make run' to process."

demo-01: ## Run Demo 01: Full Batch Processing
	@echo "[demo-01] Setting up full batch models"
	@cp demos/01_full_batch/models/stg_trips.sql models/staging/stg_trips.sql
	@cp demos/01_full_batch/models/agg_daily_revenue.sql models/metrics/agg_daily_revenue.sql
	@echo "[demo-01] Running full batch models"
	$(DBT) run --select stg_trips agg_daily_revenue
	@echo "[demo-01] Demo complete! Check SQLPad at http://localhost:3000"

demo-02: ## Run Demo 02: Incremental Event Processing
	@echo "[demo-02] Setting up incremental event models"
	@cp demos/02_incremental_events/models/stg_trips.sql models/staging/stg_trips.sql
	@cp demos/02_incremental_events/models/agg_daily_revenue.sql models/metrics/agg_daily_revenue.sql
	@echo "[demo-02] Running incremental event models"
	$(DBT) run --select stg_trips agg_daily_revenue
	@echo "[demo-02] Demo complete! Check SQLPad at http://localhost:3000"

demo-03: ## Run Demo 03: Incremental Aggregation with Partition Overwrite
	@echo "[demo-03] Setting up incremental aggregation models"
	@cp demos/03_incremental_aggregation/models/stg_trips.sql models/staging/stg_trips.sql
	@cp demos/03_incremental_aggregation/models/agg_daily_revenue.sql models/metrics/agg_daily_revenue.sql
	@echo "[demo-03] Running incremental aggregation models"
	$(DBT) run --select stg_trips agg_daily_revenue
	@echo "[demo-03] Demo complete! Check SQLPad at http://localhost:3000"
