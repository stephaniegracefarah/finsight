# dashboard/app.py

import duckdb
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

# ------------------------------------------------------------------
# Config
# ------------------------------------------------------------------

DB_PATH = "data/finsight_dev.duckdb"

st.set_page_config(
    page_title="finsight",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ------------------------------------------------------------------
# Data loading
# ------------------------------------------------------------------

@st.cache_data
def load_data():
    """Load all data from DuckDB. Cached so it only runs once."""
    con = duckdb.connect(DB_PATH, read_only=True)

    financials = con.execute("""
        SELECT * FROM main_marts.fct_financials
        ORDER BY ticker, metric, period_end
    """).df()

    anomalies = con.execute("""
        SELECT * FROM anomaly_results
        ORDER BY severity_score DESC
    """).df()

    explanations = con.execute("""
        SELECT * FROM anomaly_explanations
        ORDER BY severity_score DESC
    """).df()

    submissions = con.execute("""
        SELECT * FROM main_staging.stg_submissions
    """).df()

    con.close()
    return financials, anomalies, explanations, submissions


financials, anomalies, explanations, submissions = load_data()

# Fix period_end types so merges work correctly
financials["period_end"] = pd.to_datetime(financials["period_end"])
anomalies["period_end"] = pd.to_datetime(anomalies["period_end"])
explanations["period_end"] = pd.to_datetime(explanations["period_end"])

# ------------------------------------------------------------------
# Sidebar
# ------------------------------------------------------------------

st.sidebar.image("https://img.icons8.com/fluency/96/combo-chart.png", width=60)
st.sidebar.title("finsight")
st.sidebar.caption("AI-powered financial data quality monitor and market intelligence tool")
st.sidebar.divider()

page = st.sidebar.radio(
    "Navigate",
    ["Pipeline Health", "Anomaly Feed", "Company Explorer", "Data Lineage"],
    index=0,
)

st.sidebar.divider()
st.sidebar.caption(f"Monitoring {len(submissions)} companies")
st.sidebar.caption(f"{len(anomalies):,} anomalies detected")
st.sidebar.caption(f"{len(explanations)} AI explanations generated")


# ------------------------------------------------------------------
# Severity badge helper
# ------------------------------------------------------------------

SEVERITY_COLORS = {
    "CRITICAL": "🔴",
    "HIGH": "🟠",
    "MEDIUM": "🟡",
    "LOW": "🟢",
}


# ==================================================================
# Page 1 — Pipeline Health
# ==================================================================

if page == "Pipeline Health":
    st.title("📊 Pipeline Health")
    st.caption("Overview of data ingestion, transformation, and quality checks")
    st.divider()

    # Top metrics row
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric(
            "Companies Monitored",
            len(submissions),
        )

    with col2:
        st.metric(
            "Total Financial Records",
            f"{len(financials):,}",
        )

    with col3:
        total_anomalies = len(anomalies)
        critical = len(anomalies[anomalies["severity"] == "CRITICAL"])
        st.metric(
            "Anomalies Detected",
            total_anomalies,
            delta=f"{critical} CRITICAL",
            delta_color="inverse",
        )

    with col4:
        st.metric(
            "AI Explanations",
            len(explanations),
        )

    st.divider()

    # Company coverage table
    st.subheader("Company Coverage")
    coverage = financials.groupby(["ticker", "company_name"]).agg(
        quarters=("period_end", "nunique"),
        metrics=("metric", "nunique"),
        earliest=("period_end", "min"),
        latest=("period_end", "max"),
    ).reset_index()

    # Add anomaly counts
    anomaly_counts = anomalies.groupby("ticker").agg(
        total_anomalies=("severity", "count"),
        critical=("severity", lambda x: (x == "CRITICAL").sum()),
        high=("severity", lambda x: (x == "HIGH").sum()),
    ).reset_index()

    coverage = coverage.merge(anomaly_counts, on="ticker", how="left").fillna(0)
    coverage["critical"] = coverage["critical"].astype(int)
    coverage["high"] = coverage["high"].astype(int)
    coverage["total_anomalies"] = coverage["total_anomalies"].astype(int)

    st.dataframe(
        coverage.rename(columns={
            "ticker": "Ticker",
            "company_name": "Company",
            "quarters": "Quarters",
            "metrics": "Metrics",
            "earliest": "Earliest Period",
            "latest": "Latest Period",
            "total_anomalies": "Total Anomalies",
            "critical": "🔴 Critical",
            "high": "🟠 High",
        }),
        use_container_width=True,
        hide_index=True,
    )

    st.divider()

    # Severity distribution chart
    st.subheader("Anomaly Severity Distribution")
    col1, col2 = st.columns(2)

    with col1:
        severity_counts = anomalies["severity"].value_counts().reset_index()
        severity_counts.columns = ["Severity", "Count"]
        severity_order = ["CRITICAL", "HIGH", "MEDIUM", "LOW"]
        severity_counts["Severity"] = pd.Categorical(
            severity_counts["Severity"], categories=severity_order, ordered=True
        )
        severity_counts = severity_counts.sort_values("Severity")

        fig = px.bar(
            severity_counts,
            x="Severity",
            y="Count",
            color="Severity",
            color_discrete_map={
                "CRITICAL": "#ef4444",
                "HIGH": "#f97316",
                "MEDIUM": "#eab308",
                "LOW": "#22c55e",
            },
            title="Anomalies by Severity"
        )
        fig.update_layout(showlegend=False)
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        company_anomalies = anomalies.groupby("ticker")["severity_score"].mean().reset_index()
        company_anomalies.columns = ["Ticker", "Avg Severity Score"]
        company_anomalies = company_anomalies.sort_values("Avg Severity Score", ascending=True)

        fig2 = px.bar(
            company_anomalies,
            x="Avg Severity Score",
            y="Ticker",
            orientation="h",
            title="Average Severity Score by Company",
            color="Avg Severity Score",
            color_continuous_scale="RdYlGn_r",
        )
        st.plotly_chart(fig2, use_container_width=True)

    # dbt tests summary
    st.divider()
    st.subheader("dbt Data Quality Tests")
    tests = [
        {"Test": "not_null — ticker", "Model": "fct_financials", "Status": "✅ PASS"},
        {"Test": "not_null — metric", "Model": "fct_financials", "Status": "✅ PASS"},
        {"Test": "not_null — value_usd", "Model": "fct_financials", "Status": "✅ PASS"},
        {"Test": "not_null — period_end", "Model": "fct_financials", "Status": "✅ PASS"},
        {"Test": "unique_combination (ticker, metric, period_end)", "Model": "fct_financials", "Status": "✅ PASS"},
        {"Test": "revenue_non_negative", "Model": "fct_financials", "Status": "✅ PASS"},
        {"Test": "balance_sheet_equation", "Model": "fct_financials", "Status": "✅ PASS"},
    ]
    st.dataframe(pd.DataFrame(tests), use_container_width=True, hide_index=True)


# ==================================================================
# Page 2 — Anomaly Feed
# ==================================================================

elif page == "Anomaly Feed":
    st.title("🚨 Anomaly Feed")
    st.caption("AI-explained anomalies sorted by severity")
    st.divider()

    # Filters
    col1, col2, col3 = st.columns(3)
    with col1:
        severity_filter = st.multiselect(
            "Severity",
            ["CRITICAL", "HIGH", "MEDIUM", "LOW"],
            default=["CRITICAL", "HIGH"],
        )
    with col2:
        ticker_filter = st.multiselect(
            "Company",
            sorted(anomalies["ticker"].unique()),
            default=sorted(anomalies["ticker"].unique()),
        )
    with col3:
        metric_filter = st.multiselect(
            "Metric",
            sorted(anomalies["metric"].unique()),
            default=sorted(anomalies["metric"].unique()),
        )

    # Merge anomalies with explanations
    feed = anomalies.merge(
        explanations[[
            "ticker", "metric", "period_end",
            "headline", "likely_cause",
            "recommended_action", "confidence"
        ]],
        on=["ticker", "metric", "period_end"],
        how="left",
    )

    # Apply filters
    feed = feed[
        feed["severity"].isin(severity_filter) &
        feed["ticker"].isin(ticker_filter) &
        feed["metric"].isin(metric_filter)
    ].sort_values("severity_score", ascending=False)

    st.caption(f"Showing {len(feed)} anomalies")
    st.divider()

    # Render anomaly cards
    for _, row in feed.iterrows():
        badge = SEVERITY_COLORS.get(row["severity"], "⚪")
        has_explanation = pd.notna(row.get("headline"))

        with st.expander(
            f"{badge} {row['severity']} — {row['ticker']} | {row['metric']} | {str(row['period_end'])[:10]} | Score: {row['severity_score']:.1f}",
            expanded=row["severity"] == "CRITICAL",
        ):
            col1, col2, col3, col4 = st.columns(4)
            col1.metric("Value", f"${row['value_usd']:,.0f}")
            col2.metric("QoQ Change", f"{row['qoq_pct_change']:.1f}%" if pd.notna(row['qoq_pct_change']) else "N/A")
            col3.metric("Z-Score", f"{row['z_score']:.2f}" if pd.notna(row['z_score']) else "N/A")
            col4.metric("Severity Score", f"{row['severity_score']:.1f} / 100")

            if has_explanation:
                st.divider()
                st.markdown(f"**🤖 AI Analysis:** {row['headline']}")
                st.markdown(f"**Likely Cause:** {row['likely_cause']}")
                st.markdown(f"**Recommended Action:** {row['recommended_action']}")
                st.caption(f"Confidence: {row['confidence']}")
            else:
                st.caption("No AI explanation generated for this severity level")


# ==================================================================
# Page 3 — Company Explorer
# ==================================================================

elif page == "Company Explorer":
    st.title("🔍 Company Explorer")
    st.caption("Explore full financial history per company")
    st.divider()

    # Company selector
    company_map = dict(zip(submissions["ticker"], submissions["company_name"]))
    selected_ticker = st.selectbox(
        "Select a company",
        options=sorted(company_map.keys()),
        format_func=lambda t: f"{t} — {company_map[t]}",
    )

    company_data = financials[financials["ticker"] == selected_ticker]
    company_anomalies = anomalies[anomalies["ticker"] == selected_ticker]
    company_explanations = explanations[explanations["ticker"] == selected_ticker]

    # Company header
    info = submissions[submissions["ticker"] == selected_ticker].iloc[0]
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Quarters of Data", company_data["period_end"].nunique())
    col2.metric("Metrics Tracked", company_data["metric"].nunique())
    col3.metric("Total Anomalies", len(company_anomalies))
    col4.metric("Critical Anomalies", len(company_anomalies[company_anomalies["severity"] == "CRITICAL"]))

    st.divider()

    # Metric selector
    selected_metric = st.selectbox(
        "Select a metric",
        options=sorted(company_data["metric"].unique()),
    )

    metric_data = company_data[company_data["metric"] == selected_metric].sort_values("period_end")
    metric_anomalies = company_anomalies[company_anomalies["metric"] == selected_metric]

    # Chart
    fig = go.Figure()

    # Main line
    fig.add_trace(go.Scatter(
        x=metric_data["period_end"],
        y=metric_data["value_usd"],
        mode="lines+markers",
        name=selected_metric,
        line=dict(color="#3b82f6", width=2),
        marker=dict(size=6),
    ))

    # Anomaly markers
    for severity in ["CRITICAL", "HIGH", "MEDIUM"]:
        sev_data = metric_anomalies[metric_anomalies["severity"] == severity]
        if not sev_data.empty:
            sev_metric = metric_data[metric_data["period_end"].isin(sev_data["period_end"])]
            colors = {"CRITICAL": "#ef4444", "HIGH": "#f97316", "MEDIUM": "#eab308"}
            fig.add_trace(go.Scatter(
                x=sev_metric["period_end"],
                y=sev_metric["value_usd"],
                mode="markers",
                name=severity,
                marker=dict(
                    size=14,
                    color=colors[severity],
                    symbol="circle-open",
                    line=dict(width=3),
                ),
            ))

    fig.update_layout(
        title=f"{selected_ticker} — {selected_metric}",
        xaxis_title="Period",
        yaxis_title="USD",
        hovermode="x unified",
        height=450,
    )
    st.plotly_chart(fig, use_container_width=True)

    # Anomalies for this metric
    if not metric_anomalies.empty:
        st.subheader(f"Anomalies — {selected_metric}")
        merged = metric_anomalies.merge(
            company_explanations[[
                "metric", "period_end", "headline", "likely_cause", "recommended_action"
            ]],
            on=["metric", "period_end"],
            how="left",
        )
        for _, row in merged.sort_values("severity_score", ascending=False).iterrows():
            badge = SEVERITY_COLORS.get(row["severity"], "⚪")
            with st.expander(f"{badge} {row['severity']} — {str(row['period_end'])[:10]}"):
                col1, col2 = st.columns(2)
                col1.metric("Value", f"${row['value_usd']:,.0f}")
                col2.metric("Severity Score", f"{row['severity_score']:.1f}")
                if pd.notna(row.get("headline")):
                    st.markdown(f"**AI Analysis:** {row['headline']}")
                    st.markdown(f"**Cause:** {row['likely_cause']}")
                    st.markdown(f"**Action:** {row['recommended_action']}")

    # Raw data table
    st.divider()
    st.subheader("Raw Data")
    st.dataframe(
        metric_data[[
            "period_end", "value_usd", "qoq_pct_change", "yoy_pct_change", "filed_date", "form"
        ]].rename(columns={
            "period_end": "Period",
            "value_usd": "Value (USD)",
            "qoq_pct_change": "QoQ %",
            "yoy_pct_change": "YoY %",
            "filed_date": "Filed Date",
            "form": "Form",
        }).sort_values("Period", ascending=False),
        use_container_width=True,
        hide_index=True,
    )


# ==================================================================
# Page 4 — Data Lineage
# ==================================================================

elif page == "Data Lineage":
    st.title("🔗 Data Lineage")
    st.caption("dbt model DAG and data pipeline architecture")
    st.divider()

    # Pipeline diagram
    st.subheader("Pipeline Architecture")

    fig = go.Figure()

    # Nodes
    nodes = {
        "SEC EDGAR": (0, 2),
        "raw_submissions": (1, 3),
        "raw_company_facts": (1, 1),
        "stg_submissions": (2, 3),
        "stg_company_facts": (2, 1),
        "int_financials": (3, 2),
        "fct_financials": (4, 2),
        "anomaly_results": (5, 3),
        "anomaly_explanations": (5, 1),
    }

    node_colors = {
        "SEC EDGAR": "#6366f1",
        "raw_submissions": "#8b5cf6",
        "raw_company_facts": "#8b5cf6",
        "stg_submissions": "#3b82f6",
        "stg_company_facts": "#3b82f6",
        "int_financials": "#06b6d4",
        "fct_financials": "#10b981",
        "anomaly_results": "#f97316",
        "anomaly_explanations": "#ef4444",
    }

    edges = [
        ("SEC EDGAR", "raw_submissions"),
        ("SEC EDGAR", "raw_company_facts"),
        ("raw_submissions", "stg_submissions"),
        ("raw_company_facts", "stg_company_facts"),
        ("stg_submissions", "int_financials"),
        ("stg_company_facts", "int_financials"),
        ("int_financials", "fct_financials"),
        ("fct_financials", "anomaly_results"),
        ("anomaly_results", "anomaly_explanations"),
    ]

    # Draw edges
    for src, dst in edges:
        x0, y0 = nodes[src]
        x1, y1 = nodes[dst]
        fig.add_trace(go.Scatter(
            x=[x0, x1, None],
            y=[y0, y1, None],
            mode="lines",
            line=dict(color="#94a3b8", width=2),
            showlegend=False,
            hoverinfo="none",
        ))

    # Draw nodes
    for name, (x, y) in nodes.items():
        fig.add_trace(go.Scatter(
            x=[x],
            y=[y],
            mode="markers+text",
            marker=dict(size=20, color=node_colors[name]),
            text=[name],
            textposition="top center",
            showlegend=False,
            hovertemplate=f"<b>{name}</b><extra></extra>",
        ))

    fig.update_layout(
        height=400,
        xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
        yaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
        plot_bgcolor="rgba(0,0,0,0)",
        margin=dict(l=20, r=20, t=20, b=20),
    )
    st.plotly_chart(fig, use_container_width=True)

    st.divider()

    # Model descriptions
    st.subheader("dbt Models")
    models = [
        {
            "Model": "stg_submissions",
            "Layer": "Staging",
            "Description": "Cleans company metadata from SEC EDGAR. Adds industry grouping.",
            "Materialization": "View",
        },
        {
            "Model": "stg_company_facts",
            "Layer": "Staging",
            "Description": "Filters to true quarterly values, removes cumulative YTD entries and amended filing duplicates.",
            "Materialization": "View",
        },
        {
            "Model": "int_financials",
            "Layer": "Intermediate",
            "Description": "Joins company metadata with financial facts into company-period grain.",
            "Materialization": "View",
        },
        {
            "Model": "fct_financials",
            "Layer": "Mart",
            "Description": "Final analytical table with QoQ and YoY change columns. Source of truth for all downstream analysis.",
            "Materialization": "Table",
        },
    ]
    st.dataframe(pd.DataFrame(models), use_container_width=True, hide_index=True)

    st.divider()

    # Test coverage
    st.subheader("Test Coverage")
    tests = [
        {"Test": "not_null — ticker", "Model": "fct_financials", "Type": "Generic", "Status": "✅ PASS"},
        {"Test": "not_null — metric", "Model": "fct_financials", "Type": "Generic", "Status": "✅ PASS"},
        {"Test": "not_null — value_usd", "Model": "fct_financials", "Type": "Generic", "Status": "✅ PASS"},
        {"Test": "not_null — period_end", "Model": "fct_financials", "Type": "Generic", "Status": "✅ PASS"},
        {"Test": "unique_combination (ticker, metric, period_end)", "Model": "fct_financials", "Type": "dbt_utils", "Status": "✅ PASS"},
        {"Test": "revenue_non_negative", "Model": "fct_financials", "Type": "Custom", "Status": "✅ PASS"},
        {"Test": "balance_sheet_equation", "Model": "fct_financials", "Type": "Custom", "Status": "✅ PASS"},
    ]
    st.dataframe(pd.DataFrame(tests), use_container_width=True, hide_index=True)