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