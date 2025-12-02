### Incremental Aggregations Journey: dbt + DuckDB (PyCon 2025 Talk Demo)

This project demonstrates a progressive journey through incremental data processing patterns using dbt and DuckDB. It showcases three increasingly sophisticated approaches: full batch processing, incremental event processing, and incremental aggregation with sliding window using DuckDB's merge strategy.

The repo is optimized for live demos: everything runs locally in Docker, requires no external cloud credentials, and can be reset quickly.

---

### What's inside

- **dbt project** targeting DuckDB with staging and incremental aggregation models
- **Python models** demonstrating DuckDB Python API integration with holidays package
- **Three progressive demos** showing the evolution from full batch to sophisticated incremental patterns
- **NYC Yellow Taxi Trip Data** from the NYC Taxi & Limousine Commission (TLC) - real-world time-series data
- **Data download script** to fetch parquet files from TLC's public data repository (Sept-Oct 2025)
- **DuckDB** embedded database for fast analytical queries
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

#### Demo 03: Incremental Aggregation with Sliding Window

**Pattern**: Incremental aggregation using DuckDB's `merge` strategy with sliding window  
**Use case**: Time-series aggregations with late-arriving events  
**Trade-off**: Most sophisticated, handles late data correctly, leverages DuckDB's merge capabilities

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

3. **Start dbt container**

```bash
make up
```

This starts the dbt container with DuckDB embedded. No external services needed!

4. **Download NYC taxi data and run a demo**

```bash
# Download NYC Yellow Taxi data (default: Sept-Oct 2025)
make download-data

# Run Demo 01: Full Batch
make demo-01

# Or run Demo 02: Incremental Events
make demo-02

# Or run Demo 03: Incremental Aggregation (recommended)
make demo-03
```

5. **Query your models**

You can query your models using DuckDB CLI or any SQL client that supports DuckDB:

- **Database file**: `data/warehouse/analytics.duckdb`
- **Schema**: `analytics`

Example queries using DuckDB CLI:

```bash
docker compose exec dbt duckdb /data/warehouse/analytics.duckdb
```

Then run SQL:

```sql
SELECT * FROM analytics.stg_trips_v1 LIMIT 100;
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
# First run - downloads and processes Sept-Oct 2025 data
make download-data
make demo-03

# Download older data to simulate late-arriving trips
make demo-late-data

# Re-run - only affected date ranges are reprocessed using sliding window
make run
```

**What happens**: Uses DuckDB's `merge` strategy with a sliding window to efficiently reprocess only affected date ranges, handling late-arriving events correctly. When you download older months (e.g., August 2025), those trips will update the historical aggregations within the sliding window.

---

### Project structure

```text
.
├── Dockerfile                    # Python container for dbt + DuckDB dependencies
├── Makefile                      # Orchestrates dockerized dbt and DuckDB
├── docker-compose.yml            # dbt service
├── dbt_project.yml              # dbt project configuration
├── profiles/
│   └── profiles.yml              # dbt profile for DuckDB connection
├── models/
│   ├── schema.yml                # Model documentation and tests
│   ├── sources.yml               # Source definitions for parquet files
│   ├── staging/
│   │   ├── partition_trips_v1.sql # Partition raw data by trip_date
│   │   ├── partition_trips_v2.py   # Python version of partition model
│   │   ├── stg_trips_v1.sql        # Staging model (view - used by v1 & v3)
│   │   └── stg_trips_v2.sql        # Incremental staging (used by v2)
│   └── metrics/
│       ├── agg_daily_revenue_v1.sql              # Version 1: Full batch
│       ├── agg_daily_revenue_v2.sql              # Version 2: Incremental events
│       ├── agg_daily_revenue_v3.sql              # Version 3: Incremental aggregation (recommended)
│       └── agg_daily_revenue_with_holidays.py    # Python model with holidays
├── scripts/
│   └── download_nyc_taxi_data.py # Script to download NYC taxi parquet files
├── data/                         # Created at runtime
│   ├── raw/                      # Downloaded parquet files (NYC taxi data)
│   ├── partitioned/              # Partitioned data (optional)
│   └── warehouse/                # DuckDB database file (analytics.duckdb)
├── requirements.txt              # Python dependencies
└── README.md
```

---

### dbt + DuckDB details

- **Profiles**: Stored locally under `profiles/profiles.yml` for portability
- **Storage**: DuckDB database file at `data/warehouse/analytics.duckdb`
- **Schema**: `analytics` (default)
- **Connection**: File-based DuckDB database (embedded, no external service)
- **Incremental Strategy**: `merge` with sliding window (Version 3)
- **Python Models**: Supported natively - Python models execute in the same process as dbt

---

### Common commands

```bash
# Build images and prepare runtime dirs
make setup

# Start dbt container
make up

# Download NYC taxi data (default: Sept-Oct 2025)
make download-data

# Build models
make run

# Run specific demo version
make demo-01  # Version 1: Full batch (stg_trips_v1 + agg_daily_revenue_v1)
make demo-02  # Version 2: Incremental events (stg_trips_v2 + agg_daily_revenue_v2)
make demo-03  # Version 3: Incremental aggregation (stg_trips_v1 + agg_daily_revenue_v3)

# Simulate late-arriving data and re-run
make demo-late-data  # Downloads August 2025
make run

# Run dbt tests
make test

# Run specific model
docker compose exec -T dbt dbt --profiles-dir profiles --profile pycon25_duckdb run --select metrics.agg_daily_revenue_v3

# Stop container
make down

# Clean build artifacts
make clean
```

---

### DuckDB-specific features

This project leverages DuckDB's capabilities:

- **Merge Strategy**: DuckDB's `merge` statement efficiently handles incremental updates with sliding window
- **Parquet Reading**: Native support for reading Parquet files directly with `read_parquet()` function
- **Python Models**: Native support for Python models that execute in the same process as dbt
- **Embedded Database**: No external services needed - DuckDB runs embedded in dbt
- **Fast Analytics**: Optimized for analytical queries on columnar data

---

### Notes for the live demo

1. **Start with Version 1** (`make demo-01`) to show the simplest approach - full batch processing
2. **Progress to Version 2** (`make demo-02`) to introduce incremental event processing concepts
3. **Finish with Version 3** (`make demo-03`) to showcase DuckDB's merge strategy with sliding window
4. **Show Python models** (`agg_daily_revenue_with_holidays.py`) to demonstrate DuckDB's Python API support
5. **Use `make demo-late-data`** to download older months and show how Version 3 handles late-arriving trips correctly
6. **Query results** using DuckDB CLI:
   ```bash
   docker compose exec dbt duckdb /data/warehouse/analytics.duckdb
   ```
   Then:
   ```sql
   SELECT * FROM analytics.agg_daily_revenue_v3 ORDER BY trip_date DESC LIMIT 30;
   ```
7. **Emphasize**:
   - How `is_incremental()` limits work to changed date ranges in sliding window
   - DuckDB's `merge` strategy vs. simpler incremental approaches
   - The trade-offs between simplicity and efficiency
   - Real-world NYC taxi data demonstrates natural late-arriving patterns (data published ~2 months after collection)
   - All versions are available side-by-side for easy comparison
   - Python models work seamlessly with DuckDB (no external cluster needed)

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

- **DuckDB database locked**: Ensure only one dbt process is accessing the database at a time
- **Parquet files not found**: Run `make download-data` to download NYC taxi data
- **Download fails**: Check internet connection. Files are ~50-100MB each. Ensure sufficient disk space
- **Python model errors**: Ensure all required packages are in `requirements.txt` (holidays, pandas)
- **Connection errors**: Ensure dbt container is running (`make up`)

---

### Why DuckDB?

- **Simplicity**: No external services needed - DuckDB runs embedded in dbt
- **Python Models**: Native support for Python models without external clusters
- **Fast Analytics**: Optimized for analytical queries on columnar data
- **Merge Strategy**: Efficient incremental updates with sliding window support
- **Local Development**: Perfect for local development and demos
- **Parquet Native**: Direct support for reading Parquet files without external tools

---

### License

This project is licensed under the MIT License.
