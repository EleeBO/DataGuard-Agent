# DataGuard-Agent

> *Autonomous AI agent that monitors a multi-layer Databricks lakehouse, reasons about anomalies using Claude API, and files structured incident reports — without hardcoded rules or expensive observability tooling.*

---

## Business Impact

> Bad data costs organizations an estimated $12.9M annually on average (Gartner). This agent catches anomalies before they propagate from Bronze to Gold — protecting downstream business decisions from corrupted inputs.

Data pipelines break silently. A streaming table stops refreshing. A Silver enrichment job quietly fails. Downstream Gold tables go stale. Nobody knows until a business user asks why their dashboard hasn't updated — and that conversation happens at 9 AM in a Slack channel, in front of everyone.

Traditional monitoring tools solve this with rules: *"alert if row count drops below X."* That works — until your data changes shape, your pipeline evolves, or the threshold you set last quarter is no longer meaningful.

This agent takes a different approach.

---

## The Approach: A Fire Investigator, Not a Smoke Detector

> A traditional monitoring script is like a **smoke detector** — it trips when a hardcoded threshold is crossed.
> This agent is like a **fire investigator** — it looks at the same signals, decides if they're worth investigating, traces the root cause, and writes a report with prioritized recommendations.

Same data. Completely different intelligence.

The key distinction: **no hardcoded thresholds anywhere in this codebase.** The reasoning about what looks wrong, what to investigate deeper, and what to recommend lives entirely in the LLM — not in if/else logic.

---

## Architecture: Eyes → Brain → Hands

```
┌─────────────────────────────────────────────────────────┐
│                     run_agent.py                        │
│                  (single entrypoint)                    │
└──────────┬──────────────────┬──────────────────┬────────┘
           │                  │                  │
           ▼                  ▼                  ▼
  collect_metrics.py       agent.py        report_writer.py
       [Eyes]               [Brain]            [Hands]

  "What does the        "What does it       "File the incident"
   lakehouse look        mean? Is anything
   like right now?"      wrong?"
```

| Module | Role | What it does |
|---|---|---|
| `collect_metrics.py` | 👁️ Eyes | Queries all 14 tables via Databricks REST API — row counts, null rates, freshness timestamps |
| `agent.py` | 🧠 Brain | 3-turn Claude API reasoning loop — observe, investigate, report |
| `report_writer.py` | 🤝 Hands | Persists structured incident report to local JSON file + Databricks Delta table |
| `run_agent.py` | 🎯 Entrypoint | Single CLI command that orchestrates the full pipeline |

---

## The Agentic Loop (What Makes This Different)

This is not a script. It is an agent. The difference matters.

A **script** executes a fixed sequence of steps regardless of what it finds.
An **agent** observes, makes decisions, and adapts its next action based on what it learned.

Here is how the 3-turn reasoning loop works:

```
Turn 1 — Observe
  Feed all 14 table metrics to Claude
  Claude identifies suspicious tables and explains WHY
  No thresholds. Pure LLM judgment.
        ↓
Turn 2 — Investigate
  Agent fires targeted SQL queries ONLY on flagged tables
  Feeds deeper data back to Claude
  Claude refines its diagnosis — confirms or adjusts severity
        ↓
Turn 3 — Report
  Claude produces structured incident JSON
  Severity, root cause, recommended actions, priority order
  Agent enriches with run metadata and persists
```

The decision of *which* tables to investigate, *how* to interpret the deeper data, and *what* to recommend is made by Claude — not by the engineer who wrote the code.

---

## What the Agent Monitors

14 tables across Bronze / Silver / Gold layers of a Medallion Architecture lakehouse:

| Layer | Tables |
|---|---|
| 🥉 Bronze | `bronze_customers`, `bronze_orders`, `bronze_products`, `bronze_order_items`, `bronze_orders_stream` |
| 🥈 Silver | `silver_customers_enriched`, `silver_order_items` |
| 🥇 Gold | `gold_customer_segments`, `gold_monthly_order_trends`, `gold_return_analysis`, `gold_revenue_by_category`, `gold_stream_anomalies`, `gold_top_customers` |
| 🔧 Pipeline | `pipeline_runs` (meta-monitoring — is the pipeline itself healthy?) |

For each table the agent collects:
- **Row count** — volume anomalies, unexpected drops
- **Null rates** — data quality degradation on key columns
- **Freshness** — hours since last Delta table modification

---

## Sample Output

```
============================================================
  🔴  OVERALL SEVERITY: HIGH
  📊 Tables monitored : 14
  🚨 Incidents found  : 4
  🕐 Run ID           : incident-20260422-022803
============================================================

  Core transactional pipelines healthy. Streaming and customer
  enrichment workflows broken for ~8 days.

  INCIDENTS:

  1. 🔴 [BRONZE] bronze_orders_stream
     Observation : Stale 7.7 days — 402 completed, 48 returned orders
     Likely cause: Streaming ingestion failure — connector stopped processing
     Action      : Check streaming service, verify source connectivity, restart job

  2. 🟡 [SILVER] silver_customers_enriched
     Observation : Stale 7.9 days — 10 records missing vs bronze layer
     Likely cause: ETL job failure or external enrichment API timeout
     Action      : Investigate 10 missing records, check API status, trigger manually

  3. 🟡 [GOLD] gold_customer_segments
     Likely cause: Downstream dependency on stale silver_customers_enriched
     Action      : Fix Silver first — Gold will auto-resolve

  4. 🔴 [GOLD] gold_stream_anomalies
     Likely cause: Depends on bronze_orders_stream — cannot process new events
     Action      : Fix Bronze first, then backfill 7.7-day anomaly gap

  IMMEDIATE ACTIONS:
  1. Restart streaming ingestion pipeline for bronze_orders_stream
  2. Check enrichment API and restart silver_customers_enriched ETL
  3. Backfill anomaly detection for 8-day gap once streaming restored
```

Notice what the agent did here — it identified **two root causes** in a four-incident fire and prioritized them correctly. Fix Bronze streaming → Gold anomalies auto-resolves. Fix Silver enrichment → Gold segments auto-resolves. A rules-based monitor would have filed 4 equal alerts and left the on-call engineer to figure out the dependency chain at 2 AM.

---

## Incident Reports as Data

Every agent run produces two outputs:

**1. Local JSON file** (`reports/incident-<run-id>.json`)
Zero-friction, always readable, great for CLI workflows and debugging.

**2. Databricks Delta table** (`workspace.ecommerce.incident_reports`)
Because incident reports *are* data — and data engineers store things as data.

Over time this table becomes a queryable history:
```sql
-- How often does bronze_orders_stream go stale?
SELECT generated_at, severity, incident_count
FROM workspace.ecommerce.incident_reports
ORDER BY generated_at DESC
```
That is observability you can chart, trend, and build alerts on. Not just a log file.

---

## Setup

### Prerequisites
- Python 3.8+
- Databricks workspace with Unity Catalog
- Anthropic API key ([console.anthropic.com](https://console.anthropic.com))

### Installation

```bash
git clone https://github.com/YOUR_USERNAME/data-incident-agent.git
cd data-incident-agent
python3 -m venv venv
source venv/bin/activate
pip install databricks-sdk requests anthropic python-dotenv
```

### Configuration

Create a `.env` file in the project root:

```
DATABRICKS_HOST=https://your-workspace.azuredatabricks.net
DATABRICKS_TOKEN=your-personal-access-token
DATABRICKS_HTTP_PATH=/sql/1.0/warehouses/your-warehouse-id
ANTHROPIC_API_KEY=your-anthropic-api-key
```

> ⚠️ `.env` is in `.gitignore` — your secrets will never be committed.

---

## Usage

```bash
# Full run — collect metrics, reason, write report
python run_agent.py

# Dry run — Eyes + Brain only, skip writing report
python run_agent.py --dry-run

# Output raw JSON (useful for piping to other tools)
python run_agent.py --json
```

### Exit codes
| Code | Meaning |
|---|---|
| `0` | CLEAR / LOW / MEDIUM severity |
| `2` | HIGH severity — use this to trigger Databricks Job failure alerts |

---

## Tech Stack

- **Databricks** — lakehouse platform, Delta tables, SQL warehouses, REST API
- **Claude API (Anthropic)** — LLM reasoning engine for the agentic loop
- **Databricks SDK** — auto-credential detection, warehouse discovery
- **Python** — `requests`, `python-dotenv`, `databricks-sdk`

---