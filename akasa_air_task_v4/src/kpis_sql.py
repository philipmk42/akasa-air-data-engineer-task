from __future__ import annotations
from datetime import timedelta
from sqlalchemy import text
from .db import engine
from .utils import now_utc

def repeat_customers() -> list[tuple[str,int]]:
    sql = text('''
        SELECT o.mobile_number, COUNT(*) AS order_count
        FROM orders o
        GROUP BY o.mobile_number
        HAVING COUNT(*) > 1
        ORDER BY order_count DESC, o.mobile_number
    ''')
    with engine.begin() as conn:
        return [(r[0], int(r[1])) for r in conn.execute(sql)]

def monthly_order_trends() -> list[tuple[str,int]]:
    sql = text('''
        SELECT DATE_FORMAT(CONVERT_TZ(order_date_time, "+00:00", "+05:30"), "%Y-%m") AS ym, COUNT(*) AS orders
        FROM orders
        GROUP BY ym
        ORDER BY ym
    ''')
    with engine.begin() as conn:
        return [(r[0], int(r[1])) for r in conn.execute(sql)]

def regional_revenue() -> list[tuple[str,float]]:
    sql = text('''
        SELECT c.region, SUM(o.total_amount) AS revenue
        FROM orders o
        JOIN customers c ON c.mobile_number = o.mobile_number
        GROUP BY c.region
        ORDER BY revenue DESC
    ''')
    with engine.begin() as conn:
        return [(r[0], float(r[1])) for r in conn.execute(sql)]

def top_customers_last_30_days(limit: int = 10) -> list[tuple[str,float]]:
    since = now_utc() - timedelta(days=30)
    sql = text('''
        SELECT o.mobile_number, SUM(o.total_amount) AS spend_30d
        FROM orders o
        WHERE o.order_date_time >= :since
        GROUP BY o.mobile_number
        ORDER BY spend_30d DESC
        LIMIT :limit
    ''')
    with engine.begin() as conn:
        res = conn.execute(sql, {"since": since, "limit": limit})
        return [(r[0], float(r[1])) for r in res]
