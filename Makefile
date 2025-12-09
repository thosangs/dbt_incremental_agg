DBT := docker compose exec -T dbt dbt --profile pycon25_duckdb
PY := docker compose exec -T dbt python

.PHONY: help build setup seed run test clean up down demo-late-data demo-01 demo-02 demo-03 dbt-shell

help: ## Show this help
	@echo "Usage: make <target>"
	@echo
	@echo "Targets:"
	@awk -F':|##' '/^[a-zA-Z0-9_.-]+:.*##/ { printf "  %-20s %s\n", $$1, $$3 }' $(MAKEFILE_LIST) | sort

up: ## Start dbt container
	@echo "[up] Creating runtime dirs"
	@mkdir -p data/warehouse data/partitioned
	@chmod -R 777 data/warehouse data/partitioned || true
	@echo "[docker] Starting dbt container"
	docker compose up -d dbt
	@echo "[docker] dbt container ready"
	@echo "[generate] Generating store transaction data"
	$(PY) scripts/generate_store_transactions.py --partitioned-dir /data/partitioned

down: ## Stop dbt container
	@echo "[docker] Stopping dbt container"
	docker compose stop dbt

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
