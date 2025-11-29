### Incremental Aggregations Journey: dbt + Spark + SQLPad (PyCon 2025 Talk Demo)

This project demonstrates a progressive journey through incremental data processing patterns using dbt, Apache Spark (with Parquet), and SQLPad. It showcases three increasingly sophisticated approaches: full batch processing, incremental event processing, and incremental aggregation with partition overwrite.

The repo is optimized for live demos: everything runs locally in Docker, requires no external cloud credentials, and can be reset quickly.

---

### What's inside

- **dbt project** targeting Spark with staging and incremental aggregation models
- **Three progressive demos** showing the evolution from full batch to sophisticated incremental patterns
- **NYC Yellow Taxi Trip Data** from the NYC Taxi & Limousine Commission (TLC) - real-world time-series data
- **Data download script** to fetch parquet files from TLC's public data repository
- **Spark Standalone cluster** with Thrift server for JDBC connectivity
- **SQLPad** web UI for querying and visualizing results
- **Docker Compose** orchestration for the entire stack

---

### The Journey: Three Progressive Demos

#### Demo 01: Full Batch Processing

**Pattern**: Full table refresh on every run  
**Use case**: Small datasets, infrequent updates  
**Trade-off**: Simple but inefficient for large datasets

#### Demo 02: Incremental Event Processing

**Pattern**: Incremental event ingestion with full aggregation refresh  
**Use case**: Event streams with continuous new data  
**Trade-off**: Efficient event processing but doesn't handle late-arriving events well

#### Demo 03: Incremental Aggregation with Partition Overwrite

**Pattern**: Incremental aggregation using Spark's `insert_overwrite` strategy  
**Use case**: Time-series aggregations with late-arriving events  
**Trade-off**: Most sophisticated, handles late data correctly, leverages Spark's partition overwrite

Each demo includes its own models and README explaining the pattern, trade-offs, and when to use it.

---

### Requirements

- Docker (Desktop or compatible)
- Docker Compose
- `make` command-line utility
- macOS/Linux (tested), Windows WSL works too

---

### Quick start

1. **Clone and enter the project directory**

```bash
git clone <your-fork-or-repo-url> pycon25
cd pycon25
```

2. **Build images and prepare runtime directories**

```bash
make setup
```

3. **Start Spark cluster and SQLPad**

```bash
make sqlpad-up
```

This starts:

- Spark master (port 8080 for web UI)
- Spark worker
- Spark Thrift server (port 10000 for JDBC)
- SQLPad (port 3000 for web UI)
- dbt container

Wait a few seconds for Spark Thrift server to be ready.

4. **Download NYC taxi data and run a demo**

```bash
# Download NYC Yellow Taxi data (default: 2024, ~1GB)
make download-data

# Run Demo 01: Full Batch
make demo-01

# Or run Demo 02: Incremental Events
make demo-02

# Or run Demo 03: Incremental Aggregation (recommended)
make demo-03
```

5. **Explore in SQLPad**

- Open SQLPad at `http://localhost:3000`
- Login: `admin@example.com` / `changeme`
- Add a new connection:
  - **Name**: Spark
  - **Driver**: SparkSQL
  - **Host**: `spark-thrift`
  - **Port**: `10000`
  - **Database**: `analytics`
- Query your models:
  ```sql
  SELECT * FROM analytics.stg_trips LIMIT 100;
  SELECT * FROM analytics.agg_daily_revenue_v3 ORDER BY trip_date;
  SELECT trip_date, daily_revenue, daily_trips FROM analytics.agg_daily_revenue_v3 ORDER BY trip_date DESC LIMIT 30;
  ```

---

### Demo workflow: The Journey

#### Step 1: Start with Full Batch (Version 1)

```bash
make demo-01
```

**What happens**: Every run rebuilds tables from scratch. Simple but inefficient.

#### Step 2: Move to Incremental Events (Version 2)

```bash
make demo-02
```

**What happens**: Only new events are processed incrementally. More efficient, but late-arriving events are missed.

#### Step 3: Advanced Incremental Aggregation (Version 3)

```bash
# First run - downloads and processes 2022-2023 data
make download-data
make demo-03

# Download older data to simulate late-arriving trips
make demo-late-data

# Re-run - only affected partitions are reprocessed
make run
```

**What happens**: Uses Spark's `insert_overwrite` to efficiently reprocess only affected date partitions, handling late-arriving events correctly. When you download older months (e.g., 2021 Q1), those trips will update the historical partitions.

---

### Project structure

```text
.
├── Dockerfile                    # Python container for dbt + Spark dependencies
├── Makefile                      # Orchestrates dockerized dbt, Spark, and SQLPad
├── docker-compose.yml            # Spark cluster + SQLPad + dbt services
├── dbt_project.yml              # dbt project configuration
├── profiles/
│   └── profiles.yml              # dbt profile for Spark connection
├── models/
│   ├── schema.yml                # Model documentation and tests
│   ├── sources.yml               # Source definitions for parquet files
│   ├── staging/
│   │   ├── stg_trips.sql        # Staging model (view - used by v1 & v3)
│   │   └── stg_trips_v2.sql     # Incremental staging (used by v2)
│   └── metrics/
│       ├── agg_daily_revenue_v1.sql # Version 1: Full batch
│       ├── agg_daily_revenue_v2.sql # Version 2: Incremental events
│       └── agg_daily_revenue_v3.sql # Version 3: Incremental aggregation (recommended)
├── scripts/
│   └── download_nyc_taxi_data.py # Script to download NYC taxi parquet files
├── data/                         # Created at runtime
│   ├── raw/                      # Downloaded parquet files (NYC taxi data)
│   └── warehouse/                # dbt output (tables/views)
├── requirements.txt              # Python dependencies
└── README.md
```

---

### dbt + Spark details

- **Profiles**: Stored locally under `profiles/profiles.yml` for portability
- **Storage**: Parquet files in `data/warehouse/` directory
- **Schema**: `analytics` (default)
- **Connection**: JDBC via Spark Thrift server (`spark-thrift:10000`)
- **Incremental Strategy**: `insert_overwrite` with partition overwrite (Version 3)

---

### Common commands

```bash
# Build images and prepare runtime dirs
make setup

# Start Spark cluster and SQLPad
make sqlpad-up

# Download NYC taxi data (default: 2024, ~1GB)
make download-data

# Build models
make run

# Run specific demo version
make demo-01  # Version 1: Full batch (stg_trips + agg_daily_revenue_v1)
make demo-02  # Version 2: Incremental events (stg_trips_v2 + agg_daily_revenue_v2)
make demo-03  # Version 3: Incremental aggregation (stg_trips + agg_daily_revenue_v3)

# Simulate late-arriving data and re-run
make demo-late-data  # Downloads older months (2021 Q1)
make run

# Run dbt tests
make test

# Run specific model
docker compose exec -T dbt dbt --profiles-dir profiles run --select metrics.agg_daily_revenue_v3

# View logs
make spark-logs
make sqlpad-logs

# Stop services
make spark-down
make sqlpad-down

# Clean build artifacts
make clean
```

---

### Spark-specific features

This project leverages Spark's capabilities:

- **Partition Overwrite**: Unlike DuckDB, Spark supports efficient partition-level overwrites via `insert_overwrite` strategy
- **Parquet Storage**: Columnar format for efficient analytics queries
- **Thrift Server**: JDBC connectivity for SQLPad and other SQL clients
- **Distributed Processing**: Spark Standalone cluster for parallel processing

---

### Notes for the live demo

1. **Start with Version 1** (`make demo-01`) to show the simplest approach - full batch processing
2. **Progress to Version 2** (`make demo-02`) to introduce incremental event processing concepts
3. **Finish with Version 3** (`make demo-03`) to showcase Spark's partition overwrite capabilities
4. **Use `make demo-late-data`** to download older months and show how Version 3 handles late-arriving trips correctly
5. **Query in SQLPad** to visualize results and demonstrate the SQL interface:
   ```sql
   SELECT * FROM analytics.agg_daily_revenue_v3 ORDER BY trip_date DESC LIMIT 30;
   ```
6. **Emphasize**:
   - How `is_incremental()` limits work to changed partitions
   - Spark's `insert_overwrite` strategy vs. simpler incremental approaches
   - The trade-offs between simplicity and efficiency
   - Real-world NYC taxi data demonstrates natural late-arriving patterns (data published ~2 months after collection)
   - All versions are available side-by-side for easy comparison

---

### Data Source

This project uses **NYC Yellow Taxi Trip Data** from the [NYC Taxi & Limousine Commission (TLC)](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page). The data is publicly available in Parquet format and includes:

- **Pickup/dropoff timestamps**: Perfect for time-series aggregation
- **Fare amounts**: Revenue metrics for aggregation
- **Vendor IDs**: Categorical dimensions
- **Trip distances, passenger counts**: Additional metrics
- **Monthly files**: Natural partitioning for incremental processing

The data is published monthly with a ~2-month delay, making it ideal for demonstrating late-arriving data patterns.

### Troubleshooting

- **Spark Thrift server not ready**: Wait 10-15 seconds after `make sqlpad-up` before running dbt commands
- **Connection errors**: Ensure Spark Thrift server is running (`make spark-logs` to check)
- **SQLPad can't connect**: Verify Spark connection settings (host: `spark-thrift`, port: `10000`, database: `analytics`)
- **Parquet files not found**: Run `make download-data` to download NYC taxi data
- **Download fails**: Check internet connection. Files are ~50-100MB each. Ensure sufficient disk space (~2GB for default range)
- **Memory issues**: Ensure Docker has at least 4GB RAM allocated (Spark needs memory)
- **Port conflicts**: Ensure ports 3000 (SQLPad), 8080 (Spark UI), and 10000 (Thrift) are available

---

### Why Spark instead of DuckDB?

- **Partition Overwrite**: Spark's `insert_overwrite` strategy efficiently handles partition-level updates, which DuckDB doesn't support natively
- **Scalability**: Spark is designed for distributed processing and larger datasets
- **Production-ready**: Spark is commonly used in production data pipelines
- **JDBC Support**: Better integration with SQL tools like SQLPad via Thrift server

---

### License

This project is licensed under the MIT License.
