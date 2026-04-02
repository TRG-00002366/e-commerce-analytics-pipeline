import streamlit as st
import pandas as pd
import numpy as np
import snowflake.connector
from datetime import datetime, timedelta
from streamlit_autorefresh import st_autorefresh
# Page config
st.set_page_config(
    page_title="Amazon Analytics Dashboard",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Snowflake connection
@st.cache_resource(ttl=300)
def get_snowflake_conn():
    return snowflake.connector.connect(
        user=st.secrets["snowflake"]["user"],
        password=st.secrets["snowflake"]["password"],
        account=st.secrets["snowflake"]["account"],
        warehouse=st.secrets["snowflake"]["warehouse"],
        database=st.secrets["snowflake"]["database"],
        schema=st.secrets["snowflake"]["schema"],
        role=st.secrets["snowflake"].get("role")
    )

conn = get_snowflake_conn()

# Query helper
@st.cache_data(ttl=600)
def run_query(query: str) -> pd.DataFrame:
    return pd.read_sql(query, conn)

# Sidebar filters
st.sidebar.header("Filters")

# Date range filter
today = datetime.utcnow().date()
start_date = st.sidebar.date_input("Start date", today - timedelta(days=7))
end_date = st.sidebar.date_input("End date", today)

# Metric selector
metrics_options = [
    "Hourly Sales",
    "Top Products",
    "Regional Revenue",
    "Customer Segments",
    "Status Metrics"
]
selected_metric = st.sidebar.selectbox("Select Metric", metrics_options)

# Filter helper for dbt gold tables
def apply_filters(df, date_col=None):
    """Apply sidebar date filter to dataframe, only if date_col exists."""
    if date_col and date_col in df.columns:
        df[date_col] = pd.to_datetime(df[date_col])
        df = df[(df[date_col] >= pd.to_datetime(start_date)) & 
                (df[date_col] <= pd.to_datetime(end_date))]
    return df

# Main dashboard
st.title("Amazon Analytics Dashboard")
st.subheader(f"Data from {start_date} to {end_date}")

if selected_metric == "Hourly Sales":
    df = run_query("SELECT * FROM hourly_sales ORDER BY DATE, HOUR")
    df = apply_filters(df, date_col='date')
    df['DATE_HOUR'] = pd.to_datetime(df['DATE']) + pd.to_timedelta(df['HOUR'], unit='h')

    # KPI cards
    total_revenue = df['TOTAL_REVENUE'].sum()
    total_orders = df['TOTAL_ORDERS'].sum()
    avg_order_value = df['AVG_ORDER_VALUE'].mean() if len(df) > 0 else 0
    col1, col2, col3 = st.columns(3)
    col1.metric("Total Revenue", f"${total_revenue:,.2f}")
    col2.metric("Total Orders", f"{total_orders}")
    col3.metric("Avg Order Value", f"${avg_order_value:,.2f}")

    # Outlier detection
    threshold = df['TOTAL_REVENUE'].mean() + 2 * df['TOTAL_REVENUE'].std()
    df['OUTLIER'] = df['TOTAL_REVENUE'] > threshold
    st.markdown("### Hourly Sales Table (highlighting revenue outliers)")
    
    def highlight_outliers(row):
        if row['OUTLIER']:
            return ['background-color: #FFDDDD'] * len(row)
        else:
            return [''] * len(row)
    st.dataframe(df.style.apply(highlight_outliers, axis=1))
    st.markdown("### Hourly Orders & Revenue")
    st.line_chart(df.set_index("DATE_HOUR")[["TOTAL_ORDERS", "TOTAL_REVENUE", "AVG_ORDER_VALUE"]])

elif selected_metric == "Top Products":
    df = run_query("SELECT * FROM top_products ORDER BY CATEGORY, TOTAL_QTY DESC")
    df = apply_filters(df)
    categories = df['CATEGORY'].unique().tolist()
    selected_category = st.selectbox("Category", categories)
    st.markdown(f"### Top 10 Products in {selected_category}")
    filtered = df[df['CATEGORY'] == selected_category]
    st.bar_chart(
        filtered.set_index("PRODUCT_NAME")["TOTAL_QTY"]
    )
    st.table(df[df['CATEGORY'] == selected_category][["PRODUCT_NAME", "TOTAL_QTY", "RANK"]])

elif selected_metric == "Regional Revenue":
    df = run_query("SELECT * FROM regional_revenue ORDER BY REVENUE DESC")
    df = apply_filters(df, date_col='DATE')
    st.markdown("### Revenue by Region")
    st.bar_chart(df.set_index("REGION")["REVENUE"])

elif selected_metric == "Customer Segments":
    df = run_query("SELECT * FROM customer_segment_kpis ORDER BY REVENUE DESC")
    df = apply_filters(df, date_col='DATE')
    st.markdown("### Customer Segment KPIs")
    st.bar_chart(
        df.set_index("CUSTOMER_SEGMENT")["REVENUE"]
    )
    st.dataframe(df)

elif selected_metric == "Status Metrics":
    df = run_query("SELECT * FROM status_metrics ORDER BY CATEGORY")
    df = apply_filters(df, date_col='DATE')
    st.markdown("### Return & Cancellation Rates by Category")
    st.bar_chart(
        df.set_index("CATEGORY")[["RETURN_RATE", "CANCELLATION_RATE"]]
    )
    st.dataframe(df)

st.markdown("---")
st.caption("Data powered by Snowflake & dbt gold layer")

# ----------------------
# Auto-refresh every 60 seconds
# ----------------------
st_autorefresh(interval=60_000, key="dashboard_refresh")