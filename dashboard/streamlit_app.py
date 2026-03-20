"""
Scottish Equity Risk Pipeline - Streamlit Dashboard
File: dashboard/streamilit_app.py
"""

import streamlit as st
import pandas as pd
import snowflake.connector
from dotenv import load_dotenv
import os
import plotly.graph_objects as go
import plotly.express as px
from datetime import datetime

# 0.Config & Connection
load_dotenv()

st.set_page_config(
    page_title="Scottish Equity Risk Pipeline",
    page_icon="🏴󠁧󠁢󠁳󠁣󠁴󠁿",
    layout="wide",
    initial_sidebar_state="expanded",
)

#Custom CSS:Industrial precision dark theme
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;600&family=IBM+Plex+Sans:wght@300;400;600&display=swap');
    
    html,body, [class*="css"] {
            font-family: 'IBM Plex Sans', sans-serif;
            background-color:#0d0f14;
            color: #c8cdd8;        
    }
    .stApp {background-color: #0d0f14;}
            
    /* Sidebar */
    section[data-testid="stSidebar"] {
        background-color: #12151c;
        border-right: 1px solid #1e2330;
    }
    section[data-testid="stSidebar"] * {color: #8892a4 !important; }
    section[data-testid="stSidebar"] .stRadio label {color: #c8cdd8 !important; }
    
    /* Metric cards */
    div[data-testid="metric-container"] {
        background: #12151c;
        border: 1px solid #1e2330;
        border-radius: 4px;
        padding:16px 20px;
    }
    div[data-testid="metric-container"] label {
        font-family: 'IBM Plex Mono', monospace;
        font-size: 10px;
        letter-spacing: 0.12em;
        text-transform: uppercase;
        color: #4a5568 !important;
    }
    div[data-testid="metric-container"] [data-testid="stMetricValue"] {
        font-family: 'IBM Plex Mono', monospace;
        font-size: 22px;
        color: #e2e8f0 !important;
    }
            
    /* Dataframe */
    .stDataframe {border: 1px solid #1e2330; border-radius: 4px; }

    /* Page title */
    .page-title {
        font-family: 'IBM Plex Mono', monospace;
        font-size: 11px;
        letter-spacing: 0.18em;
        text-transform: uppercase;
        color: #4a90d9;
        margin-bottom: 4px;
    }
    .page-heading {
        font-family: 'IBM Plex Sans', sans-serif;
        font-weight: 300;
        font-size: 28px;
        color: #e2e8f0;
        margin-bottom: 24px;
        letter-spacing: -0.01em;
    }
    .divider {
        border: none;
        border-top: 1px solid #1e2330;
        margin: 24px 0;
    }
    .alert-high {color: #fc8181; font-weight: 600; }
    .alert-medium {color: #ffad55; font-weight: 600; }
    .alert-low {color: #68d391; font-weight: 600; }
    .mono {font-family: 'IBM Plex Mono', monospace; font-size: 13px; }
</style>
""", unsafe_allow_html=True)

@st.cache_resource(show_spinner=False)
def get_connection():
    """Create a cached Snowflake connection."""
    return snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE", "EQUITY_WH"),
        database=os.getenv("SNOWFLAKE_DATABASE", "EQUITY_DB"),
        role=os.getenv("SNOWFLAKE_ROLE", "ACCOUNTADMIN"),
    )

@st.cache_data(ttl=300, show_spinner=False)
def query(_conn, sql: str) -> pd.DataFrame:
    """Run a SQL and return a Dataframe. Cache for 5 mins"""
    return pd.read_sql(sql, _conn)

# 1. Sidebar
with st.sidebar:
    st.markdown("### 🏴󠁧󠁢󠁳󠁣󠁴󠁿 Scottish Equity")
    st.markdown("**Risk Pipeline**")
    st.markdown("----")

    page = st.radio(
        "Navigate",
        ["Risk Metrics", "Portfolio Overview", "Real-time Alerts"],
        label_visibility="collapsed",
    )

    st.markdown("----")
    st.markdown("**Equity tracked**")
    tickers = ["NWG.L", "ABDN.L", "SMT.L", "MNKS.L",
               "AV.L", "HIK.L", "SSE.L", "WEIR.L"]
    for t in tickers:
        st.markdown(f"<span class='mono'>· {t}</span>", unsafe_allow_html=True)

    st.markdown("----")
    st.caption(f"Last refreshed: {datetime.now().strftime('%H:%M:%S')}")
    if st.button("↻ Refresh data"):
        st.cache_data.clear()
        st.rerun()

# Helper: establish connection
try:
    conn = get_connection()
except Exception as e:
    st.error(f"Cannot connect to Snowflake: {e}")
    st.stop()

# ------- Page 1 RISK METRICS -----------
if page == "Risk Metrics":
    st.markdown("<div class='page-title'>Batch · Historical</div>", unsafe_allow_html=True)
    st.markdown("<div class='page-heading'>Risk Metrics</div>", unsafe_allow_html=True)

    with st.spinner("Loading risk metrics..."):
        df = query(conn, """
            SELECT
                SYMBOL,
                ROUND(ANNUAL_VOLATILITY_PCT, 2)  AS "Ann. Volatility (%)",
                ROUND(VAR_95_PCT, 2)             AS "VaR 95% (daily %)",
                ROUND(MAX_DAILY_GAIN_PCT, 2)     AS "Max Daily Gain (%)",
                ROUND(MAX_DAILY_LOSS_PCT, 2)     AS "Max Daily Loss (%)"
            FROM EQUITY_DB.STAGING_MARTS.MART_RISK_METRICS
            ORDER BY SYMBOL
        """)
    if df.empty:
        st.warning("No data found in MART_RISK_METRICS.")
        st.stop()
    
    # KPI summary row
    avg_vol = df["Ann. Volatility (%)"].mean()
    avg_var = df["VaR 95% (daily %)"].mean()
    worst = df.loc[df["Max Daily Loss (%)"].idxmin(), "SYMBOL"]
    best = df.loc[df["Max Daily Gain (%)"].idxmax(), "SYMBOL"]

    c1,c2,c3,c4 = st.columns(4)
    c1.metric("Avg Ann. Volatility", f"{avg_vol:.1f}%")
    c2.metric("Avg VaR 95%", f"{avg_var:.2f}%")
    c3.metric("Worst Max Loss", worst)
    c4.metric("Best Max Gain", best)

    st.markdown("<hr class='divider'>", unsafe_allow_html=True)

    # Data table
    st.markdown("### Per-equity breakdown")
    st.dataframe(
        df.style
        .background_gradient(subset=["Ann. Volatility (%)"], cmap="YlOrRd")
        .background_gradient(subset=["VaR 95% (daily %)"], cmap="YlOrRd")
        .format("{:.2f}", subset=df.columns[1:]),
        use_container_width= True,
        hide_index=True,
    )

    st.markdown("<hr class='divider'>", unsafe_allow_html=True)

    # Chart: Annualised Volatility bar
    st.markdown("#### Annualised Volatility by symbol")
    fig_vol = go.Figure(go.Bar(
        x=df["SYMBOL"],
        y=df["Ann. Volatility (%)"],
        marker=dict(
            color=df["Ann. Volatility (%)"],
            colorscale=[[0, "#1e3a5f"], [0.5, "#2b6cb0"], [1, "#4a90d9"]],
            line=dict(color="#0d0f14", width=1),
        ),
        text=df["Ann. Volatility (%)"].map(lambda v: f"{v:.1f}%"),
        textposition="outside",
        textfont=dict(family="IBM Plex Mono", size=11, color="#8892a4"),
    ))
    fig_vol.update_layout(
        plot_bgcolor="#0d0f14", paper_bgcolor="#0d0f14",
        font=dict(family="IBM Plex Sans", color="#8892a4"),
        yaxis=dict(gridcolor="#1e2330", title="Volatity (%)"),
        xaxis=dict(gridcolor="#1e2330"),
        margin=dict(t=20, b=20),
        height=320,
    )
    st.plotly_chart(fig_vol, use_container_width=True)

    #Chart:VaR vs Max Loss scatter
    st.markdown("#### VaR 95% vs Max Daily Loss")
    fig_scatter = go.Figure(go.Scatter(
        x=df["VaR 95% (daily %)"],
        y=df["Max Daily Loss (%)"],
        mode="markers+text",
        text=df["SYMBOL"],
        textposition="top center",
        textfont=dict(family="IBM Plex Mono", size=10, color="#8892a4"),
        marker=dict(size=12, color="#4a90d9",
                    line=dict(color="#0d0f14", width=2)),
    ))
    fig_scatter.update_layout(
        plot_bgcolor="#0d0f14", paper_bgcolor="#0d0f14",
        font=dict(family="IBM Plex Sans", color="#8892a4"),
        xaxis=dict(gridcolor="#1e2330", title="VaR 95% (%)"),
        yaxis=dict(gridcolor="#1e2330", title="Max Daily Loss (%)"),
        margin=dict(t=20, b=20),
        height=340,
    )
    st.plotly_chart(fig_scatter, use_container_width=True)

# ------- Page 2 PORTFOLIO OVERVIEW -----------
elif page == "Portfolio Overview":
    st.markdown("<div class='page-title'>Batch · Latest Prices</div>", unsafe_allow_html=True)
    st.markdown("<div class='page-heading'>Portfolio Overview</div>", unsafe_allow_html=True)

    with st.spinner("Loading portfolio data..."):
        df = query(conn, """
            SELECT
                SYMBOL,
                ROUND(LATEST_CLOSE, 4)  AS LATEST_PRICE,
                ROUND(AVG_CLOSE, 4)     AS AVG_PRICE,
                AVG_VOLUME              AS TOTAL_VOLUME
            FROM EQUITY_DB.STAGING_MARTS.MART_PORTFOLIO_SUMMARY
            ORDER BY SYMBOL
            """)
    if df.empty:
        st.warning("No data found in MART_PORTFOLIO_SUMMARY.")
        st.stop()
    
    # Derived: price vs average
    df["vs_avg_%"] = ((df["LATEST_PRICE"] - df["AVG_PRICE"]) / df["AVG_PRICE"] * 100).round(2)

    # KPI row
    total_vol  = df["TOTAL_VOLUME"].sum()
    top_vol = df.loc[df["TOTAL_VOLUME"].idxmax(), "SYMBOL"]
    above_avg = (df["vs_avg_%"] > 0).sum()

    c1, c2, c3 = st.columns(3)
    c1.metric("Total Portfolio Volume", f"{total_vol:,.0f}")
    c2.metric("Highest Volume Stock", top_vol)
    c3.metric("Stock Above Historic Avg", f"{above_avg} / {len(df)}")

    st.markdown("<hr class='divider'>", unsafe_allow_html=True)

    # Table
    st.markdown("#### Current Prices vs Historic Average")

    def colour_vs_avg(val):
        if val > 0: return "color: #68d391"
        if val < 0: return "color: #fc8181"
        return ""

    display = df.rename(columns={
        "SYMBOL": "Symbol",
        "LATEST_PRICE": "Latest Price(£)",
        "AVG_PRICE": "Avg Price(£)",
        "TOTAL_VOLUME": "Total Volume",
        "vs_avg_%": "vs Avg (%)",
    })
    st.dataframe(
        display.style
        .applymap(colour_vs_avg, subset=["vs Avg (%)"])
        .format({
            "Latest Price(£)": "{:.4f}",
            "Avg Price(£)": "{:.4f}",
            "Total Volume": "{:,.0f}",
            "vs Avg (%)": "{:+.2f}%",
        }),
        use_container_width=True,
        hide_index=True,
    )

    st.markdown("<hr class='divider'>", unsafe_allow_html=True)

    # Chart: Latest vs Average Price
    st.markdown("#### Latest vs Average Price")
    fig = go.Figure()
    fig.add_trace(go.Bar(
        name="Latest Price",
        x=df["SYMBOL"], y=df["LATEST_PRICE"],
        marker_color="#4a90d9",
    ))
    fig.add_trace(go.Bar(
        name="Avg Price",
        x=df["SYMBOL"], y=df["AVG_PRICE"],
        marker_color="#2d3748",
        marker_line=dict(color="#4a5568",width=1),
    ))
    fig.update_layout(
        barmode="group",
        plot_bgcolor="#0d0f14", paper_bgcolor="#0d0f14",
        font=dict(family="IBM Plex Sans", color="#8892a4"),
        legend=dict(bgcolor="#12151c", bordercolor="#1e2330", borderwidth=1),
        yaxis=dict(gridcolor="#1e2330",title="Price (£)"),
        xaxis=dict(gridcolor="#1e2330"),
        margin=dict(t=20, b=20),
        height=340,
    )
    st.plotly_chart(fig, use_container_width=True)

    # Chart: Volume bar
    st.markdown("#### Total Volume by Symbol")
    fig_v = go.Figure(go.Bar(
        x=df["SYMBOL"], y=df["TOTAL_VOLUME"],
        marker=dict(
            color=df["TOTAL_VOLUME"],
            colorscale=[[0, "#1a2744"], [1, "#4a90d9"]],
            line=dict(color="#0d0f14", width=1),
        ),
        text=df["TOTAL_VOLUME"].map(lambda v: f"{v/1e6:.1f}M"),
        textposition="outside",
        textfont=dict(family="IBM Plex Mono", size=11, color="#8892a4"),
    ))
    fig_v.update_layout(
        plot_bgcolor="#0d0f14", paper_bgcolor="#0d0f14",
        font=dict(family="IBM Plex Sans", color="#8892a4"),
        yaxis=dict(gridcolor="#1e2330", title="Volume"),
        xaxis=dict(gridcolor="#1e2330"),
        margin=dict(t=20, b=20),
        height=230,
    )
    st.plotly_chart(fig_v, use_container_width=True)

# ------- Page 3 REAL-TIME ALERTS -----------
elif page == "Real-time Alerts":

    st.markdown("<div class='page-title'>Streaming · Live</div>", unsafe_allow_html=True)
    st.markdown("<div class='page-heading'>Real-time Alerts</div>", unsafe_allow_html=True)

    with st.spinner("Loading alerts..."):
        df = query(conn, """
            SELECT
                ALERT_ID,
                SYMBOL,
                ALERT_TYPE,
                ROUND(METRIC_VALUE, 4) AS METRIC_VALUE,
                ROUND(THRESHOLD_VALUE, 4) AS THRESHOLD_VALUE,
                ROUND(PRICE_AT_ALERT, 4) AS PRICE_AT_ALERT,
                TRIGGERED_AT
            FROM EQUITY_DB.ALERTS.RISK_ALERTS
            ORDER BY TRIGGERED_AT DESC
            LIMIT 200               
        """)
    if df.empty:
        st.info("No alerts yet. The Spark Streaming job writes here when thresholds are breached.")
        st.stop()
    
    # KPI row
    total = len(df)
    unique_sym = df["SYMBOL"].nunique()
    latest_ts = pd.to_datetime(df["TRIGGERED_AT"]).max()

    c1, c2, c3 = st.columns(3)
    c1.metric("Total Alerts", total)
    c2.metric("Symbols Affected", unique_sym)
    c3.metric("Latest Alert", latest_ts.strftime("%Y-%m-%d %H:%M") if pd.notna(latest_ts) else "-")

    st.markdown("<hr class='divider'>", unsafe_allow_html=True)

    # Filters
    col_f1, col_f2 = st.columns([1,2])
    with col_f1:
        sym_filter  = st.multiselect(
            "Filter by symbol",
            options=sorted(df["SYMBOL"].unique()),
            default=[],
        )
    with col_f2:
        type_filter = st.multiselect(
            "Filter by alert type",
            options=sorted(df["ALERT_TYPE"].unique()),
            default=[]
        )

    filtered = df.copy()
    if sym_filter:
        filtered  = filtered[filtered["SYMBOL"].isin(sym_filter)]
    if type_filter:
        filtered = filtered[filtered["ALERT_TYPE"].isin(type_filter)]

    st.markdown(f"<span class='mono'>Showing {len(filtered)} of {total} alerts</span>", unsafe_allow_html=True)

    # Table
    st.markdown("#### Alert Log")

    def style_alert_type(val: str):
        val = str(val).lower()
        if "high" in val or "breach" in val or "loss" in val:
            return "color: #fc8181; font-weight: 600"
        if "warn" in val or "medium" in val:
            return "color: #f6ad55; font-weight: 600"
        return "color: #68d391; font-weight: 600"
    
    display = filtered.rename(columns= {
        "ALERT_ID": "ID",
        "SYMBOL": "Symbol",
        "ALERT_TYPE": "Type",
        "METRIC_VALUE": "Metric Value",
        "THRESHOLD_VALUE": "Threshold",
        "PRICE_AT_ALERT": "Price",
        "TRIGGERED_AT": "Triggered At",
    })
    st.dataframe(
        display.style.applymap(style_alert_type, subset=["Type"]),
        use_container_width=True,
        hide_index=True,
    )

    st.markdown("<hr class='divider'>", unsafe_allow_html=True)
        
    # Chart: Alerts per symbol
    st.markdown("#### Alerts by Symbol")
    counts = filtered.groupby("SYMBOL").size().reset_index(name="Count").sort_values("Count", ascending=False)
    fig_a = go.Figure(go.Bar(
        x=counts["SYMBOL"], y=counts["Count"],
        marker=dict(color="#fc8181", line=dict(color="#0d0f14", width=1)),
        text=counts["Count"], textposition="outside",
        textfont=dict(family="IBM Plex Mono", size=11, color="#8892a4"),
    ))
    fig_a.update_layout(
        plot_bgcolor="#0d0f14", paper_bgcolor="#0d0f14",
        font=dict(family="IBM Plex Sans", color="#8892a4"),
        yaxis=dict(gridcolor="#1e2330", title="Alert Count"),
        xaxis=dict(gridcolor="#1e2330"),
        margin=dict(t=20, b=20),
        height=300,
    )
    st.plotly_chart(fig_a, use_container_width=True)

    # Chart: Alerts over time
    st.markdown("#### Alerts time line ")
    filtered["date"] =pd.to_datetime(filtered["TRIGGERED_AT"]).dt.date
    timeline = filtered.groupby("date").size().reset_index(name="Count")
    fig_t = go.Figure(go.Scatter(
        x=timeline["date"], y=timeline["Count"],
        mode="lines+markers",
        line=dict(color="#4a90d9", width=2),
        marker=dict(size=6, color="#4a90d9"),
        fill="tozeroy",
        fillcolor="rgba(74,144,217,0.08)",
    ))
    fig_t.update_layout(
        plot_bgcolor="#0d0f14", paper_bgcolor="#0d0f14",
        font=dict(family="IBM Plex Sans", color="#8892a4"),
        yaxis=dict(gridcolor="#1e2330", title="Alert Count"),
        xaxis=dict(gridcolor="#1e2330"),
        margin=dict(t=20, b=20),
        height=200,
    )
    st.plotly_chart(fig_t, use_container_width=True)


# ------- Page  -----------
