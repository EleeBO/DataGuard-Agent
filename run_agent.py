from dotenv import load_dotenv
load_dotenv()

"""
run_agent.py
------------
The single entrypoint for the data-incident-agent.

Orchestrates the full pipeline:
  Eyes   → collect_metrics.py   (what does the lakehouse look like?)
  Brain  → agent.py             (what does it mean? is anything wrong?)
  Hands  → report_writer.py     (file the incident report)

Usage:
  python run_agent.py              # full run
  python run_agent.py --dry-run    # collect metrics + agent reasoning, skip writing report
"""

import argparse
import json
import sys
from datetime import datetime, timezone

from agent import run_agent
from report_writer import write_report


def print_banner():
    print()
    print("=" * 60)
    print("  DataGuard-Agent")
    print(f"  {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print("=" * 60)
    print()


def print_summary(report: dict):
    """Print a human-readable summary after the full run."""
    severity = report.get("severity", "UNKNOWN")
    severity_emoji = {
        "HIGH": "🔴",
        "MEDIUM": "🟡",
        "LOW": "🟢",
        "CLEAR": "✅",
    }.get(severity, "⚪")

    print()
    print("=" * 60)
    print(f"  {severity_emoji}  OVERALL SEVERITY: {severity}")
    print(f"  📊 Tables monitored : {report.get('tables_monitored', 0)}")
    print(f"  🚨 Incidents found  : {report.get('incident_count', 0)}")
    print(f"  🕐 Run ID           : {report.get('run_id')}")
    print("=" * 60)

    health = report.get("overall_health", "")
    if health:
        print(f"\n  {health}")

    incidents = report.get("incidents", [])
    if incidents:
        print(f"\n  INCIDENTS:")
        for i, incident in enumerate(incidents, 1):
            emoji = {"HIGH": "🔴", "MEDIUM": "🟡", "LOW": "🟢"}.get(incident.get("severity"), "⚪")
            print(f"\n  {i}. {emoji} [{incident.get('layer', '').upper()}] {incident.get('table')}")
            print(f"     Observation : {incident.get('observation')}")
            print(f"     Likely cause: {incident.get('likely_cause')}")
            print(f"     Action      : {incident.get('recommended_action')}")

    actions = report.get("immediate_actions", [])
    if actions:
        print(f"\n  IMMEDIATE ACTIONS:")
        for i, action in enumerate(actions, 1):
            print(f"  {i}. {action}")

    print()


def main():
    parser = argparse.ArgumentParser(
        description="DataGuard-Agent — autonomous lakehouse monitoring powered by Claude API"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Run the agent but skip writing the report (useful for testing)"
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Output the final report as raw JSON (useful for piping to other tools)"
    )
    args = parser.parse_args()

    print_banner()

    # ── Eyes + Brain: run the full agentic loop ────────────────────
    try:
        report = run_agent()
    except Exception as e:
        print(f"\n❌ Agent failed: {e}")
        sys.exit(1)

    # ── Hands: persist the report ──────────────────────────────────
    if args.dry_run:
        print("\n⚠️  Dry run mode — skipping report write.")
    else:
        try:
            write_report(report)
        except Exception as e:
            print(f"\n⚠️  Report write failed: {e}")

    # ── Output ─────────────────────────────────────────────────────
    if args.json:
        print(json.dumps(report, indent=2))
    else:
        print_summary(report)

    # Exit with non-zero code if HIGH severity — useful for CI/CD pipelines
    if report.get("severity") == "HIGH":
        sys.exit(2)

    sys.exit(0)


if __name__ == "__main__":
    main()
