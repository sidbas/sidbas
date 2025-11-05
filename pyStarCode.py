import streamlit as st
from pystarburst.dbapi import connect
from datetime import datetime
import time

# ---- Starburst connection helper ----
def get_connection():
    return connect(
        host="starburst.yourdomain.com",
        port=443,
        user="data_engineer",
        catalog="analytics",
        schema="default",
        http_scheme="https",
        auth=("JWT", "your_jwt_or_password_here"),
    )

# ---- Main refresh routine ----
def run_monthly_refresh_live(progress_area):
    conn = get_connection()
    cursor = conn.cursor()

    steps = [
        ("Delete current month", """
            DELETE FROM analytics.monthly_sales
            WHERE sales_month = date_trunc('month', current_date)
        """),
        ("Insert new month data", """
            INSERT INTO analytics.monthly_sales
            SELECT
                region,
                date_trunc('month', order_ts),
                SUM(amount),
                COUNT(order_id),
                current_timestamp
            FROM raw.sales
            WHERE order_ts >= date_trunc('month', current_date)
              AND order_ts < date_add('month', 1, date_trunc('month', current_date))
            GROUP BY region, date_trunc('month', order_ts)
        """),
        ("Delete old data (>18 months)", """
            DELETE FROM analytics.monthly_sales
            WHERE sales_month < date_add('month', -18, current_date)
        """),
        ("Refresh materialized view", """
            REFRESH MATERIALIZED VIEW analytics.mv_monthly_sales_summary
        """),
    ]

    results = []
    total = len(steps)
    bar = progress_area.progress(0)
    status_placeholder = progress_area.empty()

    for i, (name, sql) in enumerate(steps, start=1):
        status_placeholder.info(f"Running step {i}/{total}: **{name}**")
        try:
            cursor.execute(sql)
            results.append((name, "✅ Success"))
        except Exception as e:
            results.append((name, f"❌ Failed: {str(e)}"))
            status_placeholder.error(f"Step failed: {name}")
            break

        # simulate visible progress updates
        bar.progress(int((i / total) * 100))
        time.sleep(0.3)

    bar.progress(100)
    status_placeholder.success("All steps complete!")
    return results


# ---- Streamlit UI ----
st.set_page_config(page_title="Starburst Refresh Dashboard", layout="centered")
st.title("🌟 Starburst Monthly Refresh Control Panel")

st.write("Use this interface to manually trigger the monthly refresh pipeline for MicroStrategy.")

run_btn = st.button("🚀 Run Monthly Refresh Now")
progress_area = st.container()

if run_btn:
    with st.spinner("Connecting to Starburst and executing refresh..."):
        results = run_monthly_refresh_live(progress_area)

    st.success("✅ Refresh finished.")
    for step, status in results:
        st.write(f"**{step}:** {status}")

st.divider()
st.caption(f"Last loaded: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")



from pystarburst.dbapi import connect

# --- Create connection ---
conn = connect(
    host='your-starburst-host.yourdomain.com',  # e.g. galaxy.starburst.io or internal hostname
    port=443,
    user='your_username',
    catalog='hive',          # or iceberg / delta / whatever catalog you're querying
    schema='default',
    http_scheme='https',     # use 'https' for Starburst Galaxy / Enterprise
    auth=('JWT', 'your_jwt_token_here')  # or ('Basic', 'your_password') if basic auth is enabled
)

# --- Create cursor and execute query ---
cur = conn.cursor()
cur.execute("SELECT * FROM your_table LIMIT 5")

# --- Fetch results ---
rows = cur.fetchall()

# --- Print results ---
for row in rows:
    print(row)

# --- Close connection ---
cur.close()
conn.close()



