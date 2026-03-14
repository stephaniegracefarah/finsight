# finsight

AI-powered financial data quality monitor and market intelligence tool for the live events industry.

finsight ingests SEC EDGAR financial filings, runs statistical and ML-based anomaly detection, and generates plain-English explanations of every significant anomaly using the Claude API — all surfaced in a live Streamlit dashboard.

**[Live demo →](https://finsight-demo.streamlit.app)**

---

## What it does

Financial data teams spend significant time manually investigating data quality issues — missing values, duplicate filings, metric restatements, pipeline failures — that surface only after a downstream stakeholder notices something wrong. By the time an anomaly is flagged, trust in the data is already damaged.

finsight automates that investigation loop end to end:

1. **Ingests** quarterly financial statement data from SEC EDGAR for 6 publicly traded live events companies
2. **Transforms** raw XBRL data into clean quarterly grain using a dbt pipeline with custom data quality tests
3. **Detects** anomalies using a three-layer engine: rolling Z-scores, Isolation Forest, and unified severity scoring
4. **Explains** every HIGH and CRITICAL anomaly in plain English using the Claude API with full financial context
5. **Surfaces** everything in a four-page Streamlit dashboard — no SQL required

---

## Companies monitored

| Ticker | Company | Why |
|--------|---------|-----|
| LYV | Live Nation Entertainment | Anchor — Ticketmaster parent, deepest filing history |
| SEAT | Vivid Seats | Direct secondary market competitor to StubHub |
| EB | Eventbrite | Long public history, acquisition activity |
| MSGE | Madison Square Garden Entertainment | Venue operator, strong seasonality |
| STUB | StubHub Holdings | Industry focus, thin data edge case (IPO'd 2025) |
| SPHR | Sphere Entertainment | Volatile, newer — anomaly-rich dataset |

---

## Architecture
```
SEC EDGAR API
     │
     ▼
ingestion/edgar_client.py      ← throttled API client, CIK resolution
ingestion/ingest.py            ← orchestrator, saves raw JSON, writes audit log
ingestion/load_raw.py          ← loads raw JSON into DuckDB
     │
     ▼
DuckDB (raw tables)
     │
     ▼
dbt transformation pipeline
  ├── staging/
  │     ├── stg_submissions       ← company metadata, industry grouping
  │     └── stg_company_facts     ← filters true quarters, deduplicates amended filings
  ├── intermediate/
  │     └── int_financials        ← joins metadata + facts into company-period grain
  └── marts/
        └── fct_financials        ← QoQ and YoY change columns, source of truth
     │
     ▼
anomaly/detector.py
  ├── Layer 1: Rolling Z-score    ← per-metric, 8-quarter window, threshold = 3σ
  ├── Layer 2: Isolation Forest   ← multivariate, all metrics simultaneously
  └── Layer 3: Severity scoring   ← 0–100 score → LOW / MEDIUM / HIGH / CRITICAL
     │
     ▼
anomaly/explainer.py            ← Claude API, HIGH + CRITICAL only
     │
     ▼
dashboard/app.py                ← Streamlit, 4 pages, deployed to Streamlit Cloud
```

---

## Key engineering decisions

**Why DuckDB?**
Zero infrastructure, columnar performance, Snowflake-compatible SQL. The dbt adapter makes migrating to Snowflake or MotherDuck a one-line config change. All analytical queries run in milliseconds locally.

**Why dbt?**
Industry standard transformation layer. Separates raw ingestion from business logic, enforces data quality via tests, generates documentation automatically, and makes the pipeline auditable. Every transform is version-controlled SQL.

**The duplicate/cumulative problem**
SEC EDGAR stores both true quarterly values and year-to-date cumulative values in the same field with the same period end date. The staging model uses a `period_days between 60 and 105` filter to isolate true quarters, plus a `row_number()` deduplication to handle amended filings. Without this, anomaly detection fires on noise constantly.

**Why Isolation Forest?**
Unsupervised, no labeled training data required, handles multivariate anomalies invisible to per-metric thresholds. A Z-score alone would miss a quarter where revenue, cash, and operating income all decline together but none individually crosses the threshold.

**Why Claude API only for HIGH/CRITICAL?**
Cost and signal control. The severity scoring layer acts as a filter — calling the API on all 435 rows would be expensive and noisy. Only anomalies that cross meaningful statistical thresholds get explanations.

**What didn't work the first time**

  The first version of the staging model used `select *` from the raw facts table with no period filtering. This caused the
  anomaly detector to fire constantly on LYV revenue data, not because the data was anomalous, but because SEC EDGAR stores both true quarterly values and year-to-date cumulative totals in the same field with the same period end date. A Q3 YTD value of $3B sitting next to a true Q3 value of $900M looks like a 3x spike to a Z-score model.

  The fix was the `period_days between 60 and 105` filter in `stg_company_facts` — true quarters are roughly 90 days, so
  filtering on period length cleanly separates quarterly from cumulative values without needing to parse the `frame` field, which is inconsistently populated across companies.


---

## Data quality tests

All 7 tests pass on every build:

| Test | Type | What it enforces |
|------|------|-----------------|
| `not_null` on ticker, metric, value_usd, period_end | Generic | No missing data in critical columns |
| `unique_combination (ticker, metric, period_end)` | dbt_utils | No duplicate filings per company per period |
| `revenue_non_negative` | Custom SQL | Revenue never goes negative |
| `balance_sheet_equation` | Custom SQL | Assets = Liabilities + Equity within 1% |

---

## Quick start

**Prerequisites:** Python 3.11+, git
```bash
# Clone and set up environment
git clone https://github.com/stephaniegracefarah/finsight.git
cd finsight
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# Configure environment
cp .env.example .env
# Add your USER_AGENT (name + email) and ANTHROPIC_API_KEY to .env

# Run the pipeline
python -m ingestion.ingest          # fetch SEC EDGAR data
python -m ingestion.load_raw        # load into DuckDB
cd finsight_dbt && dbt build        # transform + test
cd ..
python -m anomaly.detector          # detect anomalies
python -m anomaly.explainer         # generate AI explanations
bash promote.sh                     # promote dev → prod for dashboard

# Launch dashboard
streamlit run dashboard/app.py
```

---

## Project structure
```
finsight/
├── ingestion/
│   ├── edgar_client.py     # SEC EDGAR API client
│   ├── ingest.py           # ingestion orchestrator
│   └── load_raw.py         # DuckDB loader
├── finsight_dbt/
│   ├── models/
│   │   ├── staging/        # stg_submissions, stg_company_facts
│   │   ├── intermediate/   # int_financials
│   │   └── marts/          # fct_financials
│   └── tests/              # custom SQL data quality tests
├── anomaly/
│   ├── detector.py         # Z-score + Isolation Forest + severity scoring
│   └── explainer.py        # Claude API explanation engine
├── dashboard/
│   └── app.py              # Streamlit dashboard
├── data/
│   ├── raw/                # raw JSON from SEC EDGAR (gitignored)
│   └── logs/               # ingestion run logs (gitignored)
└── requirements.txt
```

---

## Roadmap

**v2**
  - Slack alerting for HIGH/CRITICAL anomalies
  - Price/volume context via yfinance — flag whether an anomaly was already priced in at the time of filing, separating new
  signal from known risk
  - Longitudinal severity tracking — append detection results per run rather than overwriting, enabling multi-quarter trend
  analysis per company
  - Investor-framed AI explanations — reframe Claude output around investment implication, not just operational cause
  - Analyst feedback loop for Isolation Forest retraining
  - dbt Cloud CI/CD integration
  - MotherDuck for cloud DuckDB

  **v3**
  - Sector expansion — broader company coverage beyond live events (sports franchises, adjacent venue operators)
  - Valuation context — link anomalies to consensus estimates to flag beats/misses alongside the operational signal
  - Real-time streaming quality checks
  - Multi-user dashboard with authentication
  - Support for private company data via secure upload

---

## Stack

Python 3.11 · DuckDB · dbt Core · scikit-learn · Anthropic Claude API · Streamlit · Plotly · SEC EDGAR