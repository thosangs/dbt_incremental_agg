# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "marimo[sql]",
#     "duckdb>=0.10.0",
#     "matplotlib>=3.7.0",
#     "pandas>=2.0.0",
# ]
# ///

import marimo

__generated_with = "0.18.4"
app = marimo.App()


@app.cell
def _():
    import marimo as mo

    return (mo,)


@app.cell
def _():
    from datetime import datetime

    import duckdb
    import matplotlib.dates as mdates
    import matplotlib.pyplot as plt
    import pandas as pd

    return duckdb, mdates, plt


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    # 📊 Revenue Analytics Dashboard

    This notebook visualizes daily revenue metrics from the DuckDB warehouse.

    **Data Source**: `analytics.agg_daily_revenue_v3` (Incremental Aggregation with Sliding Window)
    """)
    return


@app.cell
def _(duckdb):
    # Connect to DuckDB warehouse
    db_path = "/data/warehouse/analytics.duckdb"
    conn = duckdb.connect(database=db_path)
    return (conn,)


@app.cell
def _(conn):
    # Query daily revenue data
    query = """
    SELECT 
        order_date,
        daily_revenue,
        daily_orders,
        daily_buyers,
        running_revenue
    FROM analytics.analytics.agg_daily_revenue_v3
    ORDER BY order_date
    """
    df = conn.execute(query).df()
    return (df,)


@app.cell
def _(conn, mo):
    _df = mo.sql(
        f"""
        SELECT
            order_date,
            SUM(revenue) AS daily_revenue,
            SUM(revenue) AS running_revenue,
            COUNT(DISTINCT order_id) AS daily_orders,
            COUNT(DISTINCT buyer_id) AS daily_buyers
        FROM "analytics"."analytics"."stg_orders_v2"
        WHERE order_date >= '2025-11-26'
        GROUP BY 1
        ORDER BY 1
        """,
        engine=conn,
    )
    return


@app.cell
def _(conn, mo):
    _df = mo.sql(
        f"""
        SELECT
            order_date,
            daily_revenue,
            daily_orders,
            daily_buyers,
            running_revenue
        FROM "analytics"."analytics"."agg_daily_revenue_v3"
        WHERE order_date = DATE('2025-11-26') - INTERVAL '1' DAY
        """,
        engine=conn,
    )
    return


@app.cell
def _(conn, mo):
    _df = mo.sql(
        f"""
        WITH new_aggregates AS (
            SELECT
                order_date,
                SUM(revenue) AS daily_revenue,
                SUM(revenue) AS running_revenue,
                COUNT(DISTINCT order_id) AS daily_orders,
                COUNT(DISTINCT buyer_id) AS daily_buyers
            FROM "analytics"."analytics"."stg_orders_v2"
            WHERE order_date >= '2025-11-26'
            GROUP BY 1
        ),

        existing_data AS (
            SELECT
                order_date,
                daily_revenue,
                daily_orders,
                daily_buyers,
                running_revenue
            FROM "analytics"."analytics"."agg_daily_revenue_v3"
            WHERE order_date = DATE('2025-11-26') - INTERVAL '1' DAY
        )

        SELECT order_date, daily_revenue, running_revenue FROM existing_data
        UNION ALL BY NAME
        SELECT order_date, daily_revenue, running_revenue FROM new_aggregates
        """,
        engine=conn,
    )
    return


@app.cell
def _(conn, mo):
    _df = mo.sql(
        f"""
        WITH new_aggregates AS (
            SELECT
                order_date,
                SUM(revenue) AS daily_revenue,
                SUM(revenue) AS running_revenue,
                COUNT(DISTINCT order_id) AS daily_orders,
                COUNT(DISTINCT buyer_id) AS daily_buyers
            FROM "analytics"."analytics"."stg_orders_v2"
            WHERE order_date >= '2025-11-26'
            GROUP BY 1
        ),

        existing_data AS (
            SELECT
                order_date,
                daily_revenue,
                daily_orders,
                daily_buyers,
                running_revenue
            FROM "analytics"."analytics"."agg_daily_revenue_v3"
            WHERE order_date = DATE('2025-11-26') - INTERVAL '1' DAY
        ),

        combined AS (
            SELECT * FROM new_aggregates
            UNION ALL BY NAME
            SELECT * FROM existing_data
        )

        SELECT
            order_date,
            daily_revenue,
            daily_orders,
            daily_buyers,
            SUM(running_revenue) OVER (
                ORDER BY order_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS running_revenue
        FROM combined
        ORDER BY order_date
        """,
        engine=conn,
    )
    return


@app.cell
def _(conn, mo):
    _df = mo.sql(
        f"""
        SELECT
            order_date,
            running_revenue
        FROM "analytics"."analytics"."agg_daily_revenue_v3"
        """,
        engine=conn,
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ## Data Overview

    Let's first check the data we're working with:
    """)
    return


@app.cell
def _(df, mo):
    # Display data summary
    mo.md(f"""
    **Total Records**: {len(df)}  
    **Date Range**: {df["order_date"].min()} to {df["order_date"].max()}  
    **Total Revenue**: ${df["daily_revenue"].sum():,.2f}  
    **Total Orders**: {df["daily_orders"].sum():,}
    """)
    return


@app.cell
def _(df):
    # Display first few rows
    df.head(10)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ## Daily Revenue Trend
    """)
    return


@app.cell
def _(df, mdates, plt):
    # Plot daily revenue over time
    fig1, ax1 = plt.subplots(figsize=(14, 6))

    ax1.plot(
        df["order_date"],
        df["daily_revenue"],
        linewidth=2,
        color="#2ecc71",
        label="Daily Revenue",
    )
    ax1.fill_between(df["order_date"], df["daily_revenue"], alpha=0.3, color="#2ecc71")

    ax1.set_xlabel("Date", fontsize=12)
    ax1.set_ylabel("Revenue ($)", fontsize=12)
    ax1.set_title("Daily Revenue Trend", fontsize=14, fontweight="bold")
    ax1.grid(True, alpha=0.3)
    ax1.legend()

    # Format x-axis dates
    ax1.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m-%d"))
    ax1.xaxis.set_major_locator(mdates.WeekdayLocator(interval=2))
    plt.xticks(rotation=45)

    plt.tight_layout()
    ax1
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ## Running Revenue (Cumulative)
    """)
    return


@app.cell
def _(df, mdates, plt):
    # Plot running revenue
    fig2, ax2 = plt.subplots(figsize=(14, 6))

    ax2.plot(
        df["order_date"],
        df["running_revenue"],
        linewidth=2.5,
        color="#3498db",
        label="Running Revenue",
    )

    ax2.set_xlabel("Date", fontsize=12)
    ax2.set_ylabel("Cumulative Revenue ($)", fontsize=12)
    ax2.set_title("Running Revenue (Cumulative)", fontsize=14, fontweight="bold")
    ax2.grid(True, alpha=0.3)
    ax2.legend()

    # Format x-axis dates
    ax2.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m-%d"))
    ax2.xaxis.set_major_locator(mdates.WeekdayLocator(interval=2))
    plt.xticks(rotation=45)

    plt.tight_layout()
    ax2
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ## Daily Orders and Buyers
    """)
    return


@app.cell
def _(df, mdates, plt):
    # Plot daily orders and buyers
    fig3, (ax3, ax4) = plt.subplots(2, 1, figsize=(14, 10), sharex=True)

    # Daily Orders
    ax3.plot(
        df["order_date"],
        df["daily_orders"],
        linewidth=2,
        color="#e74c3c",
        label="Daily Orders",
    )
    ax3.fill_between(df["order_date"], df["daily_orders"], alpha=0.3, color="#e74c3c")
    ax3.set_ylabel("Number of Orders", fontsize=12)
    ax3.set_title("Daily Orders", fontsize=14, fontweight="bold")
    ax3.grid(True, alpha=0.3)
    ax3.legend()

    # Daily Buyers
    ax4.plot(
        df["order_date"],
        df["daily_buyers"],
        linewidth=2,
        color="#9b59b6",
        label="Daily Buyers",
    )
    ax4.fill_between(df["order_date"], df["daily_buyers"], alpha=0.3, color="#9b59b6")
    ax4.set_xlabel("Date", fontsize=12)
    ax4.set_ylabel("Number of Buyers", fontsize=12)
    ax4.set_title("Daily Buyers", fontsize=14, fontweight="bold")
    ax4.grid(True, alpha=0.3)
    ax4.legend()

    # Format x-axis dates
    for _ax in [ax3, ax4]:
        _ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m-%d"))
        _ax.xaxis.set_major_locator(mdates.WeekdayLocator(interval=2))

    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ## Combined Metrics Dashboard
    """)
    return


@app.cell
def _(df, mdates, plt):
    # Create a comprehensive dashboard
    fig4, axes_dashboard = plt.subplots(2, 2, figsize=(16, 10))

    # 1. Daily Revenue
    axes_dashboard[0, 0].plot(
        df["order_date"], df["daily_revenue"], linewidth=2, color="#2ecc71"
    )
    axes_dashboard[0, 0].fill_between(
        df["order_date"], df["daily_revenue"], alpha=0.3, color="#2ecc71"
    )
    axes_dashboard[0, 0].set_title("Daily Revenue", fontweight="bold")
    axes_dashboard[0, 0].set_ylabel("Revenue ($)")
    axes_dashboard[0, 0].grid(True, alpha=0.3)
    axes_dashboard[0, 0].xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m-%d"))
    axes_dashboard[0, 0].tick_params(axis="x", rotation=45)

    # 2. Running Revenue
    axes_dashboard[0, 1].plot(
        df["order_date"], df["running_revenue"], linewidth=2, color="#3498db"
    )
    axes_dashboard[0, 1].set_title("Running Revenue (Cumulative)", fontweight="bold")
    axes_dashboard[0, 1].set_ylabel("Cumulative Revenue ($)")
    axes_dashboard[0, 1].grid(True, alpha=0.3)
    axes_dashboard[0, 1].xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m-%d"))
    axes_dashboard[0, 1].tick_params(axis="x", rotation=45)

    # 3. Daily Orders
    axes_dashboard[1, 0].plot(
        df["order_date"], df["daily_orders"], linewidth=2, color="#e74c3c"
    )
    axes_dashboard[1, 0].fill_between(
        df["order_date"], df["daily_orders"], alpha=0.3, color="#e74c3c"
    )
    axes_dashboard[1, 0].set_title("Daily Orders", fontweight="bold")
    axes_dashboard[1, 0].set_xlabel("Date")
    axes_dashboard[1, 0].set_ylabel("Number of Orders")
    axes_dashboard[1, 0].grid(True, alpha=0.3)
    axes_dashboard[1, 0].xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m-%d"))
    axes_dashboard[1, 0].tick_params(axis="x", rotation=45)

    # 4. Daily Buyers
    axes_dashboard[1, 1].plot(
        df["order_date"], df["daily_buyers"], linewidth=2, color="#9b59b6"
    )
    axes_dashboard[1, 1].fill_between(
        df["order_date"], df["daily_buyers"], alpha=0.3, color="#9b59b6"
    )
    axes_dashboard[1, 1].set_title("Daily Buyers", fontweight="bold")
    axes_dashboard[1, 1].set_xlabel("Date")
    axes_dashboard[1, 1].set_ylabel("Number of Buyers")
    axes_dashboard[1, 1].grid(True, alpha=0.3)
    axes_dashboard[1, 1].xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m-%d"))
    axes_dashboard[1, 1].tick_params(axis="x", rotation=45)

    plt.tight_layout()
    plt.show()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ## Summary Statistics
    """)
    return


@app.cell
def _(df, mo):
    # Display summary statistics
    summary_stats = df[["daily_revenue", "daily_orders", "daily_buyers"]].describe()
    mo.md(f"""
    ### Revenue Statistics
    - **Mean Daily Revenue**: ${df["daily_revenue"].mean():,.2f}
    - **Median Daily Revenue**: ${df["daily_revenue"].median():,.2f}
    - **Max Daily Revenue**: ${df["daily_revenue"].max():,.2f}
    - **Min Daily Revenue**: ${df["daily_revenue"].min():,.2f}

    ### Order Statistics
    - **Mean Daily Orders**: {df["daily_orders"].mean():,.0f}
    - **Median Daily Orders**: {df["daily_orders"].median():,.0f}
    - **Max Daily Orders**: {df["daily_orders"].max():,}
    - **Min Daily Orders**: {df["daily_orders"].min():,}

    ### Buyer Statistics
    - **Mean Daily Buyers**: {df["daily_buyers"].mean():,.0f}
    - **Median Daily Buyers**: {df["daily_buyers"].median():,.0f}
    - **Max Daily Buyers**: {df["daily_buyers"].max():,}
    - **Min Daily Buyers**: {df["daily_buyers"].min():,}
    """)
    return


if __name__ == "__main__":
    app.run()
