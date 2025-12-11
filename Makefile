DBT := docker compose exec -T dbt dbt --profile pycon25_duckdb
PY := docker compose exec -T dbt python

.PHONY: help run test clean up down demo-01 demo-02 demo-03 dbt-shell dbt-docs

help: ## Show this help
	@echo "Usage: make <target>"
	@echo
	@echo "Targets:"
	@awk -F':|##' '/^[a-zA-Z0-9_.-]+:.*##/ { printf "  %-20s %s\n", $$1, $$3 }' $(MAKEFILE_LIST) | sort

up: ## Start dbt, marimo, and dbt-docs containers
	@echo "[up-viz] Starting all services"
	@mkdir -p data/warehouse data/partitioned
	@chmod -R 777 data/warehouse data/partitioned || true
	docker compose up -d
	@echo "[docker] All containers ready"
	@echo "[generate] Generating store transaction data"
	$(PY) scripts/generate_store_transactions.py --partitioned-dir /data/partitioned
	@echo "[marimo] Visualization notebook available at http://localhost:8080"
	@echo "[dbt-docs] Documentation available at http://localhost:8081"

down: ## Stop all containers
	@echo "[docker] Stopping all containers"
	docker compose stop

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

demo-01: ## Run Demo v1: Full Batch Processing
	@echo "[demo-v1] Running full batch models"
	$(DBT) run --select +agg_daily_revenue_v1
	@echo "[demo-v1] Demo complete!"

demo-02: ## Run Demo v2: Incremental Event Processing
	@echo "[demo-v2] Running incremental event models"
	$(DBT) run --select +agg_daily_revenue_v2
	@echo "[demo-v2] Demo complete!"

demo-03: ## Run Demo v3: Incremental Aggregation with Sliding Window
	@echo "[demo-v3] Running incremental aggregation models"
	$(DBT) run --select +agg_daily_revenue_v3
	@echo "[demo-v3] Demo complete!"

demo-01-py: ## Run Demo v1: Full Batch Processing
	@echo "[demo-v1] Running full batch models"
	$(DBT) run --select +agg_daily_revenue_py_v1
	@echo "[demo-v1] Demo complete!"

demo-02-py: ## Run Demo v2: Incremental Event Processing
	@echo "[demo-v2] Running incremental event models"
	$(DBT) run --select +agg_daily_revenue_py_v2
	@echo "[demo-v2] Demo complete!"

demo-03-py: ## Run Demo v3: Incremental Aggregation with Sliding Window
	@echo "[demo-v3] Running incremental aggregation models"
	$(DBT) run --select +agg_daily_revenue_py_v3
	@echo "[demo-v3] Demo complete!"

dbt-docs: ## Regenerate and restart dbt docs server
	@echo "[dbt-docs] Regenerating documentation and restarting server"
	$(DBT) docs generate
	docker compose restart dbt-docs
	@echo "[dbt-docs] Documentation available at http://localhost:8081"
