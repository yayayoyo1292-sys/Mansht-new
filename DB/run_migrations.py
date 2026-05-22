"""Run migrations.sql against the configured DATABASE_URL."""
import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from DB.db import get_conn

MIGRATIONS_FILE = os.path.join(os.path.dirname(__file__), "migrations.sql")

conn = get_conn()
conn.autocommit = True
cur  = conn.cursor()

with open(MIGRATIONS_FILE, "r", encoding="utf-8") as f:
    sql = f.read()

# Execute statement by statement, skip empty/comment-only blocks
import re
statements = [s.strip() for s in sql.split(";") if s.strip() and not s.strip().startswith("--")]
errors = 0
for stmt in statements:
    try:
        cur.execute(stmt)
        print(f"✅ {stmt[:60].replace(chr(10),' ')}")
    except Exception as exc:
        print(f"⚠️  {exc} | stmt={stmt[:60].replace(chr(10),' ')}")
        errors += 1

cur.close()
conn.close()
print(f"\n{'✅ Migrations complete' if not errors else f'⚠️ Completed with {errors} warnings'}")
