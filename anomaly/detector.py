# anomaly/detector.py
import logging
from datetime import datetime, timezone

import duckdb
import numpy as np
import pandas as pd
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler

# ------------------------------------------------------------------
# Config
# ------------------------------------------------------------------

DB_PATH = "data/finsight_dev.duckdb"
RESULTS_TABLE = "anomaly_results"

# Minimum quarters needed before we run anomaly detection
MIN_QUARTERS = 4

# Z-score threshold for flagging
ZSCORE_THRESHOLD = 3.0

# Isolation Forest contamination — expected % of anomalies
CONTAMINATION = 0.1

# Severity buckets
SEVERITY_THRESHOLDS = {
    "CRITICAL": 75,
    "HIGH": 50,
    "MEDIUM": 25,
    "LOW": 0,
}

METRICS = [
    "Revenues",
    "NetIncomeLoss",
    "Assets",
    "Liabilities",
    "StockholdersEquity",
    "OperatingIncomeLoss",
    "CashAndCashEquivalentsAtCarryingValue",
    "NetCashProvidedByUsedInOperatingActivities",
]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


# ------------------------------------------------------------------
# Layer 1: Rolling Z-score
# ------------------------------------------------------------------

def compute_zscore(series: pd.Series, window: int = 8) -> pd.Series:
    """
    Compute rolling Z-score for a time series.

    For each value, calculates how many standard deviations it is
    from the rolling mean of the previous `window` quarters.
    """
    rolling_mean = series.shift(1).rolling(window=window, min_periods=MIN_QUARTERS).mean()
    rolling_std = series.shift(1).rolling(window=window, min_periods=MIN_QUARTERS).std()

    # Avoid division by zero
    zscore = (series - rolling_mean) / rolling_std.replace(0, np.nan)
    return zscore.abs()


def run_zscore_layer(df: pd.DataFrame) -> pd.DataFrame:
    """
    Apply rolling Z-score to each ticker/metric combination.
    Returns dataframe with z_score column added.
    """
    results = []

    for (ticker, metric), group in df.groupby(["ticker", "metric"]):
        group = group.sort_values("period_end").copy()

        if len(group) < MIN_QUARTERS:
            logger.debug(f"[{ticker}][{metric}] Skipping — only {len(group)} quarters")
            group["z_score"] = np.nan
        else:
            group["z_score"] = compute_zscore(group["value_usd"])

        results.append(group)

    return pd.concat(results, ignore_index=True)


# ------------------------------------------------------------------
# Layer 2: Isolation Forest
# ------------------------------------------------------------------

def run_isolation_forest(df: pd.DataFrame) -> pd.DataFrame:
    """
    Run Isolation Forest per ticker across all metrics simultaneously.

    Pivots data so each row is one quarter with all metrics as columns,
    then fits the model and assigns anomaly scores.
    """
    all_results = []

    for ticker, group in df.groupby("ticker"):
        # Pivot: rows = quarters, columns = metrics
        pivoted = group.pivot_table(
            index="period_end",
            columns="metric",
            values="value_usd",
            aggfunc="first"
        )

        # Need at least MIN_QUARTERS rows with enough metrics
        if len(pivoted) < MIN_QUARTERS or pivoted.shape[1] < 2:
            logger.debug(f"[{ticker}] Skipping Isolation Forest — insufficient data")
            group["if_score"] = np.nan
            all_results.append(group)
            continue

        # Fill missing metrics with column median
        pivoted = pivoted.fillna(pivoted.median())

        # Scale features
        scaler = StandardScaler()
        scaled = scaler.fit_transform(pivoted)

        # Fit Isolation Forest
        clf = IsolationForest(
            contamination=CONTAMINATION,
            random_state=42,
            n_estimators=100
        )
        clf.fit(scaled)

        # Get anomaly scores — more negative = more anomalous
        # We flip and normalize to 0-1 range where 1 = most anomalous
        raw_scores = clf.score_samples(scaled)
        normalized = (raw_scores - raw_scores.max()) / (raw_scores.min() - raw_scores.max() + 1e-10)

        score_map = dict(zip(pivoted.index, normalized))

        # Map scores back to original dataframe rows
        group = group.copy()
        group["if_score"] = group["period_end"].map(score_map)
        all_results.append(group)

        logger.info(f"[{ticker}] Isolation Forest complete — {len(pivoted)} quarters analyzed")

    return pd.concat(all_results, ignore_index=True)


# ------------------------------------------------------------------
# Layer 3: Severity scoring
# ------------------------------------------------------------------

def compute_severity_score(z_score: float, if_score: float) -> float:
    """
    Combine Z-score and Isolation Forest score into a 0-100 severity score.

    Weights:
    - Z-score contributes 60% — more interpretable, metric-specific
    - Isolation Forest contributes 40% — catches multivariate anomalies
    """
    z_component = 0.0
    if_component = 0.0

    if not np.isnan(z_score):
        # Cap Z-score at 6 for normalization
        z_normalized = min(z_score / 6.0, 1.0)
        z_component = z_normalized * 60

    if not np.isnan(if_score):
        if_component = if_score * 40

    return round(z_component + if_component, 2)


def assign_severity_label(score: float) -> str:
    """Bucket a 0-100 severity score into LOW/MEDIUM/HIGH/CRITICAL."""
    if score >= SEVERITY_THRESHOLDS["CRITICAL"]:
        return "CRITICAL"
    elif score >= SEVERITY_THRESHOLDS["HIGH"]:
        return "HIGH"
    elif score >= SEVERITY_THRESHOLDS["MEDIUM"]:
        return "MEDIUM"
    else:
        return "LOW"


def run_severity_layer(df: pd.DataFrame) -> pd.DataFrame:
    """Apply severity scoring to all rows."""
    df = df.copy()

    df["severity_score"] = df.apply(
        lambda row: compute_severity_score(
            row.get("z_score", np.nan),
            row.get("if_score", np.nan)
        ),
        axis=1
    )

    df["severity"] = df["severity_score"].apply(assign_severity_label)

    return df


# ------------------------------------------------------------------
# Main
# ------------------------------------------------------------------

def main():
    logger.info("=" * 50)
    logger.info("FinSight Anomaly Detection — starting run")
    logger.info("=" * 50 + "\n")

    # Load data from mart
    logger.info("Loading data from fct_financials...")
    con = duckdb.connect(DB_PATH)
    df = con.execute("""
        SELECT
            ticker,
            company_name,
            metric,
            period_end,
            value_usd,
            qoq_pct_change,
            yoy_pct_change,
            filed_date,
            form
        FROM main_marts.fct_financials
        ORDER BY ticker, metric, period_end
    """).df()
    logger.info(f"Loaded {len(df):,} rows\n")

    # Layer 1: Z-score
    logger.info("Layer 1: Computing rolling Z-scores...")
    df = run_zscore_layer(df)
    flagged_z = df[df["z_score"] > ZSCORE_THRESHOLD]
    logger.info(f"  Z-score flags: {len(flagged_z)} anomalies detected\n")

    # Layer 2: Isolation Forest
    logger.info("Layer 2: Running Isolation Forest...")
    df = run_isolation_forest(df)
    logger.info("")

    # Layer 3: Severity scoring
    logger.info("Layer 3: Computing severity scores...")
    df = run_severity_layer(df)

    # Summary
    severity_counts = df["severity"].value_counts()
    logger.info("  Severity distribution:")
    for severity in ["CRITICAL", "HIGH", "MEDIUM", "LOW"]:
        count = severity_counts.get(severity, 0)
        logger.info(f"    {severity:<10} {count:>4} anomalies")

    # Save results to DuckDB
    logger.info("\nSaving results to DuckDB...")
    con.execute(f"DROP TABLE IF EXISTS {RESULTS_TABLE}")
    con.execute(f"""
        CREATE TABLE {RESULTS_TABLE} AS
        SELECT
            ticker,
            company_name,
            metric,
            period_end,
            value_usd,
            qoq_pct_change,
            yoy_pct_change,
            filed_date,
            z_score,
            if_score,
            severity_score,
            severity,
            current_timestamp as detected_at
        FROM df
        WHERE z_score IS NOT NULL
           OR if_score IS NOT NULL
        ORDER BY severity_score DESC
    """)

    count = con.execute(f"SELECT COUNT(*) FROM {RESULTS_TABLE}").fetchone()[0]
    logger.info(f"  Saved {count:,} rows to '{RESULTS_TABLE}'")

    # Preview top anomalies
    logger.info("\n=== Top 10 Anomalies ===")
    top = con.execute(f"""
        SELECT ticker, company_name, metric, period_end,
               value_usd, severity_score, severity
        FROM {RESULTS_TABLE}
        WHERE severity IN ('CRITICAL', 'HIGH')
        ORDER BY severity_score DESC
        LIMIT 10
    """).fetchall()

    for row in top:
        logger.info(
            f"  {row[6]:<10} {row[0]:<6} {row[3]}  "
            f"{row[4]:>18,.0f}  {row[2]}"
        )

    con.close()
    logger.info("\nDone!")


if __name__ == "__main__":
    main()