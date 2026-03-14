# anomaly/explainer.py

import logging
import os
import json
from datetime import datetime, timezone

import duckdb
import anthropic
from dotenv import load_dotenv

load_dotenv()

# ------------------------------------------------------------------
# Config
# ------------------------------------------------------------------

DB_PATH = "data/finsight_dev.duckdb"
EXPLAIN_SEVERITIES = ["HIGH", "CRITICAL"]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


# ------------------------------------------------------------------
# Context builder
# ------------------------------------------------------------------

def build_company_context(con, ticker: str, metric: str, period_end: str) -> dict:
    """
    Build full financial context for one anomaly to send to Claude.
    Includes the anomalous value plus surrounding quarters for comparison.
    """

    # Get the anomalous quarter
    anomaly = con.execute("""
        SELECT
            ticker, company_name, metric, period_end,
            value_usd, qoq_pct_change, yoy_pct_change,
            z_score, if_score, severity_score, severity
        FROM anomaly_results
        WHERE ticker = ?
        AND metric = ?
        AND CAST(period_end AS VARCHAR) LIKE ?
        ORDER BY severity_score DESC
        LIMIT 1
    """, [ticker, metric, f"{str(period_end)[:10]}%"]).df()

    if anomaly.empty:
        return {}

    row = anomaly.iloc[0]

    # Get last 8 quarters of this metric for context
    history = con.execute("""
        SELECT period_end, value_usd, qoq_pct_change, yoy_pct_change
        FROM main_marts.fct_financials
        WHERE ticker = ?
        AND metric = ?
        ORDER BY period_end DESC
        LIMIT 8
    """, [ticker, metric]).fetchall()

    # Get all metrics for the anomalous quarter for broader context
    quarter_snapshot = con.execute("""
        SELECT metric, value_usd, qoq_pct_change, severity
        FROM anomaly_results
        WHERE ticker = ?
        AND CAST(period_end AS VARCHAR) LIKE ?
        ORDER BY severity_score DESC
    """, [ticker, f"{str(period_end)[:10]}%"]).fetchall()

    return {
        "ticker": ticker,
        "company_name": row["company_name"],
        "anomalous_metric": metric,
        "anomalous_period": str(period_end)[:10],
        "anomalous_value": float(row["value_usd"]),
        "qoq_pct_change": float(row["qoq_pct_change"]) if row["qoq_pct_change"] else None,
        "yoy_pct_change": float(row["yoy_pct_change"]) if row["yoy_pct_change"] else None,
        "z_score": float(row["z_score"]) if row["z_score"] else None,
        "severity": row["severity"],
        "severity_score": float(row["severity_score"]),
        "metric_history": [
            {
                "period": str(h[0])[:10],
                "value": float(h[1]),
                "qoq_pct": float(h[2]) if h[2] else None,
                "yoy_pct": float(h[3]) if h[3] else None,
            }
            for h in history
        ],
        "quarter_snapshot": [
            {
                "metric": q[0],
                "value": float(q[1]),
                "qoq_pct": float(q[2]) if q[2] else None,
                "severity": q[3],
            }
            for q in quarter_snapshot
        ],
    }


# ------------------------------------------------------------------
# Claude API call
# ------------------------------------------------------------------

def generate_explanation(client: anthropic.Anthropic, context: dict) -> dict:
    """
    Call Claude API with full financial context to generate
    a plain-English explanation of the anomaly.
    """

    prompt = f"""You are a financial data analyst reviewing an anomaly detected in SEC EDGAR filing data.

Company: {context['company_name']} ({context['ticker']})
Anomalous metric: {context['anomalous_metric']}
Period: {context['anomalous_period']}
Value: ${context['anomalous_value']:,.0f}
QoQ change: {context['qoq_pct_change']}%
YoY change: {context['yoy_pct_change']}%
Z-score: {context['z_score']} (threshold: 3.0)
Severity: {context['severity']} (score: {context['severity_score']}/100)

Recent history for this metric (most recent first):
{json.dumps(context['metric_history'], indent=2)}

All metrics for this quarter:
{json.dumps(context['quarter_snapshot'], indent=2)}

Please provide a concise anomaly explanation in this exact JSON format:
{{
    "headline": "One sentence summary of what happened (max 15 words)",
    "likely_cause": "2-3 sentences explaining the most probable business reason for this anomaly",
    "supporting_evidence": "1-2 sentences pointing to other metrics in the quarter snapshot that support your explanation",
    "recommended_action": "One specific action a data analyst or finance lead should take to investigate or respond",
    "confidence": "HIGH, MEDIUM, or LOW — how confident are you in this explanation"
}}

Return only valid JSON, no other text."""

    response = client.messages.create(
        model="claude-sonnet-4-20250514",
        max_tokens=1000,
        messages=[{"role": "user", "content": prompt}]
    )

    raw = response.content[0].text.strip()

    # Parse JSON response
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        # Strip markdown fences if present
        clean = raw.replace("```json", "").replace("```", "").strip()
        return json.loads(clean)


# ------------------------------------------------------------------
# Main
# ------------------------------------------------------------------

def main():
    logger.info("=" * 50)
    logger.info("FinSight AI Explainer — starting run")
    logger.info("=" * 50 + "\n")

    client = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))
    con = duckdb.connect(DB_PATH)

    # Get all HIGH and CRITICAL anomalies
    anomalies = con.execute("""
        SELECT DISTINCT ticker, metric, period_end, severity, severity_score
        FROM anomaly_results
        WHERE severity IN ('HIGH', 'CRITICAL')
        ORDER BY severity_score DESC
    """).fetchall()

    logger.info(f"Found {len(anomalies)} HIGH/CRITICAL anomalies to explain\n")

    explanations = []

    for i, (ticker, metric, period_end, severity, score) in enumerate(anomalies):
        logger.info(f"[{i+1}/{len(anomalies)}] {severity} — {ticker} {metric} {str(period_end)[:10]}")

        # Build context
        context = build_company_context(con, ticker, metric, period_end)
        if not context:
            logger.warning(f"  Could not build context, skipping")
            continue

        # Call Claude
        try:
            explanation = generate_explanation(client, context)
            explanation["ticker"] = ticker
            explanation["metric"] = metric
            explanation["period_end"] = str(period_end)[:10]
            explanation["severity"] = severity
            explanation["severity_score"] = score
            explanations.append(explanation)

            logger.info(f"  ✓ {explanation['headline']}")

        except Exception as e:
            logger.error(f"  ✗ Failed: {e}")
            continue

    # Save explanations to DuckDB
    if explanations:
        import pandas as pd
        df = pd.DataFrame(explanations)
        df["generated_at"] = datetime.now(timezone.utc).isoformat()

        con.execute("DROP TABLE IF EXISTS anomaly_explanations")
        con.execute("""
            CREATE TABLE anomaly_explanations AS
            SELECT * FROM df
        """)

        logger.info(f"\nSaved {len(df)} explanations to 'anomaly_explanations'")

        # Preview
        logger.info("\n=== Sample Explanations ===")
        for exp in explanations[:3]:
            logger.info(f"\n{exp['severity']} — {exp['ticker']} {exp['metric']} {exp['period_end']}")
            logger.info(f"  Headline: {exp['headline']}")
            logger.info(f"  Cause: {exp['likely_cause']}")
            logger.info(f"  Action: {exp['recommended_action']}")
            logger.info(f"  Confidence: {exp['confidence']}")

    con.close()
    logger.info("\nDone!")


if __name__ == "__main__":
    main()