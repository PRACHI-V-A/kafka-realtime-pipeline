import streamlit as st
import psycopg2
import pandas as pd
import plotly.express as px
import time

st.set_page_config(page_title="Realtime Ecommerce Dashboard", layout="wide")

# -------------------------
# Database Connection
# -------------------------
def get_connection():
    return psycopg2.connect(
        host="localhost",
        database="realtime_db",
        user="admin",
        password="admin",
        port=5432
    )

# -------------------------
# Load Data
# -------------------------
def load_data(query):
    conn = get_connection()
    df = pd.read_sql(query, conn)
    conn.close()
    return df

# Auto refresh every 5 sec
st_autorefresh = st.empty()

# -------------------------
# Title
# -------------------------
st.title("💙 Realtime Ecommerce Analytics Dashboard")

# -------------------------
# KPI Section
# -------------------------
col1, col2 = st.columns(2)

total_orders_df = load_data("SELECT * FROM total_orders_per_minute ORDER BY window_start DESC LIMIT 1;")
avg_order_df = load_data("SELECT * FROM avg_order_value ORDER BY window_start DESC LIMIT 1;")

with col1:
    if not total_orders_df.empty:
        st.metric("Total Orders (Last Minute)", int(total_orders_df['total_orders'].iloc[0]))

with col2:
    if not avg_order_df.empty:
        st.metric("Average Order Value", round(avg_order_df['avg_order_value'].iloc[0], 2))

# -------------------------
# Revenue Trend
# -------------------------
revenue_df = load_data("SELECT * FROM revenue_per_minute ORDER BY window_start;")

if not revenue_df.empty:
    fig_revenue = px.line(
        revenue_df,
        x="window_start",
        y="total_revenue",
        title="Revenue Per Minute",
        markers=True
    )
    st.plotly_chart(fig_revenue, use_container_width=True)

# -------------------------
# Revenue By Category
# -------------------------
category_df = load_data("""
SELECT * FROM revenue_per_category
WHERE window_start = (SELECT MAX(window_start) FROM revenue_per_category);
""")

if not category_df.empty:
    fig_category = px.bar(
        category_df,
        x="product_category",
        y="category_revenue",
        title="Revenue By Category (Latest Window)"
    )
    st.plotly_chart(fig_category, use_container_width=True)

# -------------------------
# Payment Method Distribution
# -------------------------
payment_df = load_data("""
SELECT * FROM orders_per_payment
WHERE window_start = (SELECT MAX(window_start) FROM orders_per_payment);
""")

if not payment_df.empty:
    fig_payment = px.pie(
        payment_df,
        names="payment_method",
        values="order_count",
        title="Orders by Payment Method"
    )
    st.plotly_chart(fig_payment, use_container_width=True)

# -------------------------
# Auto Refresh
# -------------------------
time.sleep(5)
st.experimental_rerun()