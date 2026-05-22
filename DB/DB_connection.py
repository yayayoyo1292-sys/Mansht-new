from db import get_conn

conn = get_conn()
print("✅ DATABASE CONNECTED SUCCESSFULLY")
conn.close()