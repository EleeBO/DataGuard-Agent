from dotenv import load_dotenv
load_dotenv()

"""
agent.py
--------
The BRAIN of the DataGuard-Agent.

Implements a 3-turn agentic loop:
  Turn 1 — Observe:     Feed all 14 table metrics to Claude. Ask what looks suspicious.
  Turn 2 — Investigate: Run targeted SQL on flagged tables. Feed results back to Claude.
  Turn 3 — Report:      Claude produces a structured incident report with severity,
                        root cause, and recommended action.

Unlike a scripted monitor (smoke detector), this agent REASONS about what it sees
and DECIDES whether something is worth escalating — no hardcoded thresholds.
"""

import os
import json
import requests
import time
from datetime import datetime, timezone

from collect_metrics import collect_all_metrics, _get_client, _get_warehouse_id, _execute_sql

ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY")
CLAUDE_MODEL = "claude-sonnet-4-20250514"
MAX_TOKENS = 1000

# ---------------------------------------------------------------------------
# Claude API call helper
# ---------------------------------------------------------------------------

def _call_claude(messages: list, system: str) -> str:
    """Send a message list to Claude and return the text response."""
    response = requests.post(
        "https://api.anthropic.com/v1/messages",
        headers={
            "x-api-key": ANTHROPIC_API_KEY,
            "anthropic-version": "2023-06-01",
            "content-type": "application/json",
        },
        json={
            "model": CLAUDE_MODEL,
            "max_tokens": MAX_TOKENS,
            "system": system,
            "messages": messages,
        },
    )
    response.raise_for_status()
    return response.json()["content"][0]["text"]


# ---------------------------------------------------------------------------
# Targeted investigation queries — fired only when Claude flags a table
# ---------------------------------------------------------------------------

INVESTIGATION_QUERIES = {
    "bronze_customers": """
        SELECT country, COUNT(*) as count
        FROM workspace.ecommerce.bronze_customers
        GROUP BY country ORDER BY count DESC LIMIT 5
    """,
    "bronze_orders": """
        SELECT DATE(order_date) as day, COUNT(*) as orders
        FROM workspace.ecommerce.bronze_orders
        GROUP BY day ORDER BY day DESC LIMIT 7
    """,
    "bronze_products": """
        SELECT category, COUNT(*) as count, ROUND(AVG(base_price),2) as avg_price
        FROM workspace.ecommerce.bronze_products
        GROUP BY category
    """,
    "bronze_order_items": """
        SELECT COUNT(*) as total, SUM(CASE WHEN quantity <= 0 THEN 1 ELSE 0 END) as bad_qty
        FROM workspace.ecommerce.bronze_order_items
    """,
    "bronze_orders_stream": """
        SELECT status, COUNT(*) as count
        FROM workspace.ecommerce.bronze_orders_stream
        GROUP BY status ORDER BY count DESC
    """,
    "silver_customers_enriched": """
        SELECT tenure_segment, COUNT(*) as count
        FROM workspace.ecommerce.silver_customers_enriched
        GROUP BY tenure_segment
    """,
    "silver_order_items": """
        SELECT COUNT(*) as total,
               ROUND(AVG(unit_price), 2) as avg_price,
               SUM(CASE WHEN unit_price <= 0 THEN 1 ELSE 0 END) as zero_price_count
        FROM workspace.ecommerce.silver_order_items
    """,
    "gold_customer_segments": """
        SELECT tenure_segment, COUNT(*) as count, ROUND(AVG(total_spend),2) as avg_spend
        FROM workspace.ecommerce.gold_customer_segments
        GROUP BY tenure_segment
    """,
    "gold_monthly_order_trends": """
        SELECT year, month, month_label, total_revenue, order_count
        FROM workspace.ecommerce.gold_monthly_order_trends
        ORDER BY year DESC, month DESC LIMIT 3
    """,
    "gold_return_analysis": """
        SELECT category, return_rate, total_revenue_lost
        FROM workspace.ecommerce.gold_return_analysis
        ORDER BY return_rate DESC LIMIT 5
    """,
    "gold_revenue_by_category": """
        SELECT category, total_revenue
        FROM workspace.ecommerce.gold_revenue_by_category
        ORDER BY total_revenue DESC
    """,
    "gold_stream_anomalies": """
        SELECT event_id, deviation, status
        FROM workspace.ecommerce.gold_stream_anomalies
        ORDER BY deviation DESC LIMIT 5
    """,
    "gold_top_customers": """
        SELECT customer_id, total_spend, order_count
        FROM workspace.ecommerce.gold_top_customers
        ORDER BY total_spend DESC LIMIT 5
    """,
    "pipeline_runs": """
        SELECT status, layer_reached, failed_checks, duration_seconds
        FROM workspace.ecommerce.pipeline_runs
        ORDER BY run_timestamp DESC LIMIT 5
    """,
}


# ---------------------------------------------------------------------------
# Turn 1 — Observe
# ---------------------------------------------------------------------------

def turn1_observe(metrics: dict) -> tuple[str, list]:
    """
    Feed all table metrics to Claude.
    Ask it to identify suspicious tables and explain why.
    Returns Claude's reasoning text and the conversation history.
    """
    print("\n🔍 Turn 1 — Observe: Feeding metrics to Claude...")

    system = """You are an expert data reliability engineer monitoring a Databricks lakehouse.
You will be given metrics (row counts, null rates, freshness) for tables across Bronze, Silver, and Gold layers.

Your job:
1. Identify which tables look suspicious or concerning
2. For each suspicious table, explain WHY it looks wrong
3. Consider: unusual row counts, high null rates, stale data (many hours since last update), 
   layer inconsistencies (e.g. Silver has fewer rows than expected vs Bronze)
4. Be specific — reference actual numbers from the metrics

Respond in this JSON format only:
{
  "flagged_tables": [
    {
      "table": "short_table_name",
      "reason": "specific explanation referencing actual metric values",
      "severity": "HIGH | MEDIUM | LOW"
    }
  ],
  "summary": "one sentence overview of overall lakehouse health"
}

If everything looks healthy, return an empty flagged_tables list.
Return JSON only — no preamble, no markdown fences."""

    metrics_text = json.dumps(metrics, indent=2)
    user_message = f"""Here are the current lakehouse metrics snapshot:

{metrics_text}

Analyze these metrics and identify any tables that look suspicious or concerning."""

    messages = [{"role": "user", "content": user_message}]
    response_text = _call_claude(messages, system)

    # Append assistant response to conversation history
    messages.append({"role": "assistant", "content": response_text})

    print(f"   ✅ Claude identified issues. Parsing response...")
    return response_text, messages


# ---------------------------------------------------------------------------
# Turn 2 — Investigate
# ---------------------------------------------------------------------------

def turn2_investigate(flagged_tables: list, messages: list) -> tuple[str, list]:
    """
    For each flagged table, run a targeted SQL query to get deeper context.
    Feed all investigation results back to Claude for deeper reasoning.
    Returns Claude's deeper analysis and updated conversation history.
    """
    print("\n🔬 Turn 2 — Investigate: Running targeted queries on flagged tables...")

    client = _get_client()
    warehouse_id = _get_warehouse_id(client)

    investigation_results = []

    for item in flagged_tables:
        table_name = item.get("table", "")
        query = INVESTIGATION_QUERIES.get(table_name)

        if not query:
            print(f"   ⚠️  No investigation query defined for {table_name}, skipping.")
            continue

        print(f"   🔎 Investigating {table_name}...")
        try:
            rows = _execute_sql(client, query.strip(), warehouse_id)
            investigation_results.append({
                "table": table_name,
                "severity": item.get("severity"),
                "initial_reason": item.get("reason"),
                "investigation_data": rows,
            })
            print(f"   ✅ Got {len(rows)} rows of investigation data for {table_name}")
        except Exception as e:
            print(f"   ⚠️  Investigation query failed for {table_name}: {e}")
            investigation_results.append({
                "table": table_name,
                "severity": item.get("severity"),
                "initial_reason": item.get("reason"),
                "investigation_data": f"Query failed: {str(e)}",
            })

    if not investigation_results:
        print("   ℹ️  No investigation results to feed back.")
        return "No investigation data available.", messages

    # Feed investigation results back to Claude
    investigation_text = json.dumps(investigation_results, indent=2)
    user_message = f"""I ran deeper investigation queries on the tables you flagged.
Here are the results:

{investigation_text}

Based on this deeper data, refine your analysis:
1. Confirm or adjust severity for each flagged table
2. Identify the most likely root cause
3. Suggest a specific recommended action for each issue

Respond in this JSON format only:
{{
  "incidents": [
    {{
      "table": "short_table_name",
      "severity": "HIGH | MEDIUM | LOW",
      "observation": "what the metrics show",
      "likely_cause": "your best assessment of root cause",
      "recommended_action": "specific steps to investigate or fix"
    }}
  ]
}}

Return JSON only — no preamble, no markdown fences."""

    messages.append({"role": "user", "content": user_message})
    response_text = _call_claude(messages, system="You are an expert data reliability engineer. Respond only in the JSON format requested.")
    messages.append({"role": "assistant", "content": response_text})

    print(f"   ✅ Claude completed deeper analysis.")
    return response_text, messages


# ---------------------------------------------------------------------------
# Turn 3 — Report
# ---------------------------------------------------------------------------

def turn3_report(incidents_text: str, metrics: dict, messages: list) -> dict:
    """
    Ask Claude to produce the final structured incident report.
    Returns a clean incident report dict ready for report_writer.py.
    """
    print("\n📋 Turn 3 — Report: Generating final incident report...")

    user_message = """Based on your full analysis, produce the final incident report.

Respond in this JSON format only:
{
  "severity": "HIGH | MEDIUM | LOW | CLEAR",
  "incident_count": <number of issues found>,
  "overall_health": "one sentence assessment of overall lakehouse health",
  "incidents": [
    {
      "table": "short_table_name",
      "layer": "bronze | silver | gold | pipeline",
      "severity": "HIGH | MEDIUM | LOW",
      "observation": "what the metrics show",
      "likely_cause": "root cause assessment",
      "recommended_action": "specific remediation steps"
    }
  ],
  "immediate_actions": ["top 1-3 things to do right now, in priority order"]
}

If no incidents found, set severity to CLEAR and incidents to empty list.
Return JSON only — no preamble, no markdown fences."""

    messages.append({"role": "user", "content": user_message})
    response_text = _call_claude(messages, system="You are an expert data reliability engineer. Respond only in the JSON format requested.")

    # Parse and enrich with run metadata
    try:
        clean = response_text.strip().replace("```json", "").replace("```", "")
        report = json.loads(clean)
    except json.JSONDecodeError:
        print("   ⚠️  Could not parse Claude's report as JSON. Using raw text.")
        report = {"raw_response": response_text}

    report["run_id"] = f"incident-{datetime.now(timezone.utc).strftime('%Y%m%d-%H%M%S')}"
    report["generated_at"] = datetime.now(timezone.utc).isoformat()
    report["tables_monitored"] = len(metrics.get("tables", []))
    report["metrics_collected_at"] = metrics.get("collected_at")

    print(f"   ✅ Incident report generated. Severity: {report.get('severity', 'UNKNOWN')}")
    return report


# ---------------------------------------------------------------------------
# Main agentic loop
# ---------------------------------------------------------------------------

def run_agent() -> dict:
    """
    Orchestrates the full 3-turn agentic loop:
      Eyes (collect_metrics) → Brain Turn 1 (observe) →
      Brain Turn 2 (investigate) → Brain Turn 3 (report)
    """
    print("=" * 60)
    print("DataGuard-Agent — Starting agentic loop")
    print("=" * 60)

    # ── Eyes: collect metrics ──────────────────────────────────────
    metrics = collect_all_metrics()

    # ── Brain Turn 1: Observe ──────────────────────────────────────
    turn1_response, messages = turn1_observe(metrics)

    try:
        clean = turn1_response.strip().replace("```json", "").replace("```", "")
        turn1_data = json.loads(clean)
    except json.JSONDecodeError:
        print("⚠️  Could not parse Turn 1 response. Aborting.")
        return {"error": "Turn 1 parse failure", "raw": turn1_response}

    flagged_tables = turn1_data.get("flagged_tables", [])
    summary = turn1_data.get("summary", "")
    print(f"\n   📌 Overall: {summary}")
    print(f"   📌 Flagged tables: {len(flagged_tables)}")

    # ── Brain Turn 2: Investigate ──────────────────────────────────
    if flagged_tables:
        incidents_text, messages = turn2_investigate(flagged_tables, messages)
    else:
        print("\n✅ No suspicious tables found. Lakehouse looks healthy.")
        incidents_text = "{}"

    # ── Brain Turn 3: Report ───────────────────────────────────────
    report = turn3_report(incidents_text, metrics, messages)

    print("\n" + "=" * 60)
    print("🏁 AGENT COMPLETE")
    print(f"   Run ID  : {report.get('run_id')}")
    print(f"   Severity: {report.get('severity')}")
    print(f"   Incidents: {report.get('incident_count', 0)}")
    print("=" * 60)

    return report


if __name__ == "__main__":
    report = run_agent()
    print("\n--- FINAL INCIDENT REPORT ---")
    print(json.dumps(report, indent=2))
