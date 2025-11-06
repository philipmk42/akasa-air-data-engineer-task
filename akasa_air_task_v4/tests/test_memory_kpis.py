import pandas as pd
from datetime import datetime, timezone, timedelta
from src.kpis_memory import repeat_customers, top_customers_last_30_days

def test_repeat_customers():
    df = pd.DataFrame({
        "mobile_number": ["a","a","b"],
        "order_date_time": [datetime.now(timezone.utc)]*3,
        "sku_id": ["x","y","z"],
        "sku_count": [1,1,1],
        "total_amount": [10,20,30],
    })
    out = repeat_customers(df)
    assert out.iloc[0]["mobile_number"] == "a"
    assert out.iloc[0]["order_count"] == 2

def test_top_customers_last_30_days():
    now = datetime.now(timezone.utc)
    df = pd.DataFrame({
        "mobile_number": ["a","b","b"],
        "order_date_time": [now - timedelta(days=5), now - timedelta(days=1), now - timedelta(days=40)],
        "sku_id": ["x","y","z"],
        "sku_count": [1,1,1],
        "total_amount": [10,20,100],
    })
    out = top_customers_last_30_days(df, limit=1)
    assert out.iloc[0]["mobile_number"] == "b"
    assert out.iloc[0]["spend_30d"] == 20
