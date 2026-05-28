from dotenv import load_dotenv
load_dotenv()
"""
collect_metrics.py
------------------
Pulls row counts, null rates, and freshness timestamps for all
registered tables across Bronze / Silver / Gold layers.

Uses Databricks SQL Statement Execution REST API — same pattern
as your ecommerce-pipeline Phase 5 setup.
"""

import os
import time
import requests
from datetime import datetime, timezone
from databricks.sdk import WorkspaceClient


# ---------------------------------------------------------------------------
# Table Registry — all monitored tables with layer labels
# Update table names here if yours differ slightly
# ---------------------------------------------------------------------------
TABLE_REGISTRY = [
    # Bronze
    {"layer": "bronze", "table": "workspace.ecommerce.bronze_customers"},
    {"layer": "bronze", "table": "workspace.ecommerce.bronze_orders"},
    {"layer": "bronze", "table": "workspace.ecommerce.bronze_products"},
    {"layer": "bronze", "table": "workspace.ecommerce.bronze_order_items"},
    {"layer": "bronze", "table": "workspace.ecommerce.bronze_orders_stream"},

    # Silver
    {"layer": "silver", "table": "workspace.ecommerce.silver_customers_enriched"},
    {"layer": "silver", "table": "workspace.ecommerce.silver_order_items"},

    # Gold
    {"layer": "gold", "table": "workspace.ecommerce.gold_customer_segments"},
    {"layer": "gold", "table": "workspace.ecommerce.gold_monthly_order_trends"},
    {"layer": "gold", "table": "workspace.ecommerce.gold_return_analysis"},
    {"layer": "gold", "table": "workspace.ecommerce.gold_revenue_by_category"},
    {"layer": "gold", "table": "workspace.ecommerce.gold_stream_anomalies"},
    {"layer": "gold", "table": "workspace.ecommerce.gold_top_customers"},

    # Pipeline health (meta-monitoring)
    {"layer": "pipeline", "table": "workspace.ecommerce.pipeline_runs"},
]

# Columns to check for nulls — table-specific, expand as needed
NULL_CHECK_COLUMNS = {
    "bronze_customers":           ["customer_id", "email"],
    "bronze_orders":              ["order_id", "customer_id", "order_date"],
    "bronze_products":            ["product_id", "base_price"],
    "bronze_order_items":         ["order_id", "product_id", "quantity"],
    "bronze_orders_stream":       ["event_id", "customer_id", "status"],
    "silver_customers_enriched":  ["customer_id", "email", "country"],
    "silver_order_items":         ["order_id", "product_id", "quantity", "unit_price"],
    "gold_customer_segments":     ["customer_id", "tenure_segment"],
    "gold_monthly_order_trends":  ["year", "month", "total_revenue", "order_count"],
    "gold_return_analysis":       ["category", "return_rate", "total_revenue_lost"],
    "gold_revenue_by_category":   ["category", "total_revenue"],
    "gold_stream_anomalies":      ["event_id", "deviation"],
    "gold_top_customers":         ["customer_id", "total_spend", "order_count"],
    "pipeline_runs":              ["run_id", "status", "layer_reached"],
}


# ---------------------------------------------------------------------------
# Databricks SQL execution helpers
# ---------------------------------------------------------------------------

def _get_client():
    """Auto-detect credentials via Databricks SDK (same as Phase 5)."""
    return WorkspaceClient()


def _execute_sql(client, sql: str, warehouse_id: str) -> list[dict]:
    """
    Run a SQL statement via REST API and return rows as list of dicts.
    Polls until the statement completes.
    """
    host = client.config.host
    token = client.config.token
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }

    # Submit statement
    resp = requests.post(
        f"{host}/api/2.0/sql/statements",
        headers=headers,
        json={
            "statement": sql,
            "warehouse_id": warehouse_id,
            "wait_timeout": "30s",
            "on_wait_timeout": "CONTINUE",
        },
    )
    resp.raise_for_status()
    payload = resp.json()
    statement_id = payload["statement_id"]

    # Poll until done
    for _ in range(30):
        status = payload.get("status", {}).get("state", "")
        if status in ("SUCCEEDED", "FAILED", "CANCELED", "CLOSED"):
            break
        time.sleep(2)
        poll = requests.get(
            f"{host}/api/2.0/sql/statements/{statement_id}",
            headers=headers,
        )
        poll.raise_for_status()
        payload = poll.json()

    state = payload.get("status", {}).get("state")
    if state != "SUCCEEDED":
        error = payload.get("status", {}).get("error", {})
        raise RuntimeError(f"SQL failed [{state}]: {error.get('message', 'unknown error')}")

    # Parse results into list of dicts
    result = payload.get("result", {})
    schema = payload.get("manifest", {}).get("schema", {}).get("columns", [])
    col_names = [c["name"] for c in schema]
    rows = result.get("data_array", [])

    return [dict(zip(col_names, row)) for row in rows]


def _get_warehouse_id(client) -> str:
    http_path = os.environ.get("DATABRICKS_HTTP_PATH", "")
    if http_path:
        # Extract warehouse ID from HTTP path
        # e.g. /sql/1.0/warehouses/abc123 -> abc123
        return http_path.strip("/").split("/")[-1]
    warehouses = client.warehouses.list()
    for wh in warehouses:
        if wh.state.value in ("RUNNING", "IDLE"):
            return wh.id
    raise RuntimeError("No running SQL warehouse found. Start one in your Databricks workspace.")

# ---------------------------------------------------------------------------
# Metric collectors
# ---------------------------------------------------------------------------

def get_row_count(client, warehouse_id: str, table: str) -> int:
    sql = f"SELECT COUNT(*) AS row_count FROM {table}"
    rows = _execute_sql(client, sql, warehouse_id)
    return int(rows[0]["row_count"]) if rows else 0


def get_null_rates(client, warehouse_id, table):
    """Return null rate (0.0–1.0) for each monitored column in the table."""
    short_name = table.split(".")[-1]
    columns = NULL_CHECK_COLUMNS.get(short_name, [])
    if not columns:
        return {}

    null_exprs = ", ".join(
        f"ROUND(SUM(CASE WHEN {col} IS NULL THEN 1 ELSE 0 END) / COUNT(*), 4) AS {col}_null_rate"
        for col in columns
    )
    sql = f"SELECT {null_exprs} FROM {table}"
    rows = _execute_sql(client, sql, warehouse_id)
    return rows[0] if rows else {}


def get_freshness(client, warehouse_id, table):
    """
    Return the latest modification timestamp from Delta table history.
    Falls back gracefully if DESCRIBE HISTORY isn't available.
    """
    try:
        sql = f"DESCRIBE HISTORY {table} LIMIT 1"
        rows = _execute_sql(client, sql, warehouse_id)
        if rows:
            ts = rows[0].get("timestamp", None)
            return {
                "last_modified": str(ts),
                "hours_since_modified": _hours_since(str(ts)) if ts else None,
            }
    except Exception:
        pass
    return {"last_modified": None, "hours_since_modified": None}


def _hours_since(ts_str):
    """Calculate hours elapsed since a timestamp string."""
    try:
        # Handle both 'YYYY-MM-DD HH:MM:SS' and ISO formats
        ts_str = ts_str.replace("T", " ").split(".")[0]
        dt = datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S")
        dt = dt.replace(tzinfo=timezone.utc)
        delta = datetime.now(timezone.utc) - dt
        return round(delta.total_seconds() / 3600, 2)
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def collect_all_metrics() -> dict:
    """
    Collect metrics for all registered tables.
    Returns a structured dict ready to pass to the agent.
    """
    print("🔌 Connecting to Databricks...")
    client = _get_client()
    warehouse_id = _get_warehouse_id(client)
    print(f"✅ Connected. Using warehouse: {warehouse_id}\n")

    metrics = {
        "collected_at": datetime.now(timezone.utc).isoformat(),
        "tables": [],
    }

    for entry in TABLE_REGISTRY:
        layer = entry["layer"]
        table = entry["table"]
        short_name = table.split(".")[-1]

        print(f"📊 Collecting metrics for {short_name} [{layer}]...")

        try:
            row_count = get_row_count(client, warehouse_id, table)
            null_rates = get_null_rates(client, warehouse_id, table)
            freshness = get_freshness(client, warehouse_id, table)

            metrics["tables"].append({
                "layer": layer,
                "table": table,
                "short_name": short_name,
                "row_count": row_count,
                "null_rates": null_rates,
                "freshness": freshness,
                "status": "ok",
            })
            print(f"   ✅ rows={row_count:,} | last_modified={freshness.get('last_modified', 'N/A')}")

        except Exception as e:
            print(f"   ⚠️  Failed to collect metrics for {short_name}: {e}")
            metrics["tables"].append({
                "layer": layer,
                "table": table,
                "short_name": short_name,
                "row_count": None,
                "null_rates": {},
                "freshness": {},
                "status": "error",
                "error": str(e),
            })

    print(f"\n✅ Metrics collected for {len(metrics['tables'])} tables.")
    return metrics


if __name__ == "__main__":
    import json
    results = collect_all_metrics()
    print("\n--- RAW METRICS SNAPSHOT ---")
    print(json.dumps(results, indent=2))
