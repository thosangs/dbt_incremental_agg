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

__generated_with = "0.17.4"
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

    return (duckdb, datetime, mdates, pd, plt)


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    # 📊 Revenue Analytics Dashboard
    
    This notebook visualizes daily revenue metrics from the DuckDB warehouse.
    
    **Data Source**: `analytics.agg_daily_revenue_v3` (Incremental Aggregation with Sliding Window)
    """)
    return


@app.cell
def _():
    # Connect to DuckDB warehouse
    db_path = "/data/warehouse/analytics.duckdb"
    conn = duckdb.connect(db_path)
    return (conn, db_path)


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
    FROM analytics.agg_daily_revenue_v3
    ORDER BY order_date
    """
    df = conn.execute(query).df()
    return (df, query)


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
    fig, ax = plt.subplots(figsize=(14, 6))

    ax.plot(
        df["order_date"],
        df["daily_revenue"],
        linewidth=2,
        color="#2ecc71",
        label="Daily Revenue",
    )
    ax.fill_between(df["order_date"], df["daily_revenue"], alpha=0.3, color="#2ecc71")

    ax.set_xlabel("Date", fontsize=12)
    ax.set_ylabel("Revenue ($)", fontsize=12)
    ax.set_title("Daily Revenue Trend", fontsize=14, fontweight="bold")
    ax.grid(True, alpha=0.3)
    ax.legend()

    # Format x-axis dates
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m-%d"))
    ax.xaxis.set_major_locator(mdates.WeekdayLocator(interval=2))
    plt.xticks(rotation=45)

    plt.tight_layout()
    ax
    return (ax, fig)


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ## Running Revenue (Cumulative)
    """)
    return


@app.cell
def _(df, mdates, plt):
    # Plot running revenue
    fig, ax = plt.subplots(figsize=(14, 6))

    ax.plot(
        df["order_date"],
        df["running_revenue"],
        linewidth=2.5,
        color="#3498db",
        label="Running Revenue",
    )

    ax.set_xlabel("Date", fontsize=12)
    ax.set_ylabel("Cumulative Revenue ($)", fontsize=12)
    ax.set_title("Running Revenue (Cumulative)", fontsize=14, fontweight="bold")
    ax.grid(True, alpha=0.3)
    ax.legend()

    # Format x-axis dates
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m-%d"))
    ax.xaxis.set_major_locator(mdates.WeekdayLocator(interval=2))
    plt.xticks(rotation=45)

    plt.tight_layout()
    ax
    return (ax, fig)


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ## Daily Orders and Buyers
    """)
    return


@app.cell
def _(df, mdates, plt):
    # Plot daily orders and buyers
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 10), sharex=True)

    # Daily Orders
    ax1.plot(
        df["order_date"],
        df["daily_orders"],
        linewidth=2,
        color="#e74c3c",
        label="Daily Orders",
    )
    ax1.fill_between(df["order_date"], df["daily_orders"], alpha=0.3, color="#e74c3c")
    ax1.set_ylabel("Number of Orders", fontsize=12)
    ax1.set_title("Daily Orders", fontsize=14, fontweight="bold")
    ax1.grid(True, alpha=0.3)
    ax1.legend()

    # Daily Buyers
    ax2.plot(
        df["order_date"],
        df["daily_buyers"],
        linewidth=2,
        color="#9b59b6",
        label="Daily Buyers",
    )
    ax2.fill_between(df["order_date"], df["daily_buyers"], alpha=0.3, color="#9b59b6")
    ax2.set_xlabel("Date", fontsize=12)
    ax2.set_ylabel("Number of Buyers", fontsize=12)
    ax2.set_title("Daily Buyers", fontsize=14, fontweight="bold")
    ax2.grid(True, alpha=0.3)
    ax2.legend()

    # Format x-axis dates
    for ax in [ax1, ax2]:
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m-%d"))
        ax.xaxis.set_major_locator(mdates.WeekdayLocator(interval=2))

    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()
    return (ax1, ax2, fig)


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ## Combined Metrics Dashboard
    """)
    return


@app.cell
def _(df, mdates, plt):
    # Create a comprehensive dashboard
    fig, axes = plt.subplots(2, 2, figsize=(16, 10))

    # 1. Daily Revenue
    axes[0, 0].plot(df["order_date"], df["daily_revenue"], linewidth=2, color="#2ecc71")
    axes[0, 0].fill_between(
        df["order_date"], df["daily_revenue"], alpha=0.3, color="#2ecc71"
    )
    axes[0, 0].set_title("Daily Revenue", fontweight="bold")
    axes[0, 0].set_ylabel("Revenue ($)")
    axes[0, 0].grid(True, alpha=0.3)
    axes[0, 0].xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m-%d"))
    axes[0, 0].tick_params(axis="x", rotation=45)

    # 2. Running Revenue
    axes[0, 1].plot(
        df["order_date"], df["running_revenue"], linewidth=2, color="#3498db"
    )
    axes[0, 1].set_title("Running Revenue (Cumulative)", fontweight="bold")
    axes[0, 1].set_ylabel("Cumulative Revenue ($)")
    axes[0, 1].grid(True, alpha=0.3)
    axes[0, 1].xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m-%d"))
    axes[0, 1].tick_params(axis="x", rotation=45)

    # 3. Daily Orders
    axes[1, 0].plot(df["order_date"], df["daily_orders"], linewidth=2, color="#e74c3c")
    axes[1, 0].fill_between(
        df["order_date"], df["daily_orders"], alpha=0.3, color="#e74c3c"
    )
    axes[1, 0].set_title("Daily Orders", fontweight="bold")
    axes[1, 0].set_xlabel("Date")
    axes[1, 0].set_ylabel("Number of Orders")
    axes[1, 0].grid(True, alpha=0.3)
    axes[1, 0].xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m-%d"))
    axes[1, 0].tick_params(axis="x", rotation=45)

    # 4. Daily Buyers
    axes[1, 1].plot(df["order_date"], df["daily_buyers"], linewidth=2, color="#9b59b6")
    axes[1, 1].fill_between(
        df["order_date"], df["daily_buyers"], alpha=0.3, color="#9b59b6"
    )
    axes[1, 1].set_title("Daily Buyers", fontweight="bold")
    axes[1, 1].set_xlabel("Date")
    axes[1, 1].set_ylabel("Number of Buyers")
    axes[1, 1].grid(True, alpha=0.3)
    axes[1, 1].xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m-%d"))
    axes[1, 1].tick_params(axis="x", rotation=45)

    plt.tight_layout()
    plt.show()
    return (axes, fig)


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
    return (summary_stats,)


if __name__ == "__main__":
    app.run()
