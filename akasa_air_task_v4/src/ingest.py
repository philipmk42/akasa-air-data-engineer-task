from __future__ import annotations
import pandas as pd
from lxml import etree
from sqlalchemy import insert
from sqlalchemy.exc import IntegrityError
from .db import engine
from .models import Base, Customer, Order
from .utils import parse_ts
from datetime import datetime

def init_db():
    Base.metadata.create_all(engine)

def _clean_series_str(s: pd.Series) -> pd.Series:
    return (s.astype('string')
             .fillna('')
             .str.strip())

def load_customers_csv(path: str) -> pd.DataFrame:
    # Read as strings to avoid dtype issues (e.g., 'CUST-001')
    df = pd.read_csv(path, dtype='string', encoding='utf-8', keep_default_na=False)
    # Normalize column names
    df.columns = [c.strip().lower() for c in df.columns]

    # Expected columns (allow some common variants)
    colmap = {
        'customer_id': ['customer_id', 'cust_id', 'id'],
        'customer_name': ['customer_name', 'name', 'full_name'],
        'mobile_number': ['mobile_number', 'mobile', 'phone', 'msisdn', 'contact'],
        'region': ['region', 'state', 'zone', 'area']
    }

    def pick(df, opts, required=True):
        for o in opts:
            if o in df.columns:
                return o
        if required:
            raise ValueError(f"Missing required column. Tried any of: {opts}")
        return None

    cid = pick(df, colmap['customer_id'])
    cname = pick(df, colmap['customer_name'])
    mob = pick(df, colmap['mobile_number'])
    reg = pick(df, colmap['region'], required=False)

    out = pd.DataFrame({
        'customer_id': _clean_series_str(df[cid]),
        'customer_name': _clean_series_str(df[cname]),
        'mobile_number': _clean_series_str(df[mob]).str.replace(r"\s+", "", regex=True),
    })
    if reg:
        out['region'] = _clean_series_str(df[reg]).where(lambda x: x != '', other=pd.NA)
    else:
        out['region'] = pd.NA

    # Deduplicate by mobile_number (keep first)
    before = len(out)
    out = out.drop_duplicates(subset=['mobile_number'], keep='first').reset_index(drop=True)
    after = len(out)
    if before != after:
        print(f"[load_customers_csv] Deduped customers by mobile_number: {before} -> {after}")

    return out

def load_orders_xml(path: str) -> pd.DataFrame:
    rows = []
    # Streaming parse for large XMLs
    context = etree.iterparse(path, tag='order')
    for _, node in context:
        rows.append({
            'order_id': (node.findtext('order_id') or '').strip(),
            'mobile_number': (node.findtext('mobile_number') or '').replace(' ', '').strip(),
            'order_date_time': parse_ts(node.findtext('order_date_time')),
            'sku_id': (node.findtext('sku_id') or '').strip(),
            'sku_count': int((node.findtext('sku_count') or '0').strip() or 0),
            'total_amount': float((node.findtext('total_amount') or '0').strip() or 0.0),
        })
        # clear node to free memory
        node.clear()
        while node.getprevious() is not None:
            del node.getparent()[0]
    return pd.DataFrame(rows)

def upsert_customers(df: pd.DataFrame) -> int:
    inserted = 0
    with engine.begin() as conn:
        for i in range(0, len(df), 1000):
            chunk = df.iloc[i:i+1000]
            try:
                conn.execute(insert(Customer).prefix_with('IGNORE'), chunk.to_dict(orient='records'))
                inserted += len(chunk)
            except IntegrityError:
                pass
    return inserted

def upsert_orders(df: pd.DataFrame) -> int:
    inserted = 0
    with engine.begin() as conn:
        for i in range(0, len(df), 1000):
            chunk = df.iloc[i:i+1000]
            try:
                conn.execute(insert(Order).prefix_with('IGNORE'), chunk.to_dict(orient='records'))
                inserted += len(chunk)
            except IntegrityError:
                pass
    return inserted

def ingest_all(customers_csv: str, orders_xml: str) -> dict:
    init_db()
    cdf = load_customers_csv(customers_csv)
    odf = load_orders_xml(orders_xml)
    ci = upsert_customers(cdf)
    oi = upsert_orders(odf)
    return {'customers_inserted': ci, 'orders_inserted': oi}
