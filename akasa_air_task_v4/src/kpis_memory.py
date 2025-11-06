from __future__ import annotations
import pandas as pd
from datetime import timedelta, datetime, timezone
from .utils import now_utc

def _mask_mobile(s: pd.Series) -> pd.Series:
    # Mask all but last 4 digits
    def m(x: str) -> str:
        x = str(x)
        tail = x[-4:] if len(x) >= 4 else x
        return 'X' * max(len(x) - len(tail), 0) + tail
    return s.astype('string').fillna('').map(m)

def normalize_orders(df_orders: pd.DataFrame, order_granularity: str = 'header') -> pd.DataFrame:
    """Return an orders dataframe at requested granularity.
    header: one row per (order_id, mobile_number, order_date_time) using the first occurrence, and total_amount taken from the first row.
    line:   keep as-is (one row per line). Note: revenue should still be computed at header level to avoid double-counting.
    """
    if order_granularity == 'header':
        cols = ['order_id', 'mobile_number', 'order_date_time', 'total_amount']
        # Drop duplicates by order_id keeping the first
        dfh = (df_orders
               .sort_values('order_date_time')
               .drop_duplicates(subset=['order_id'], keep='first')[cols]
               .reset_index(drop=True))
        return dfh
    else:
        return df_orders.copy()

def repeat_customers(df_orders: pd.DataFrame, order_granularity: str = 'header', mask_pii: bool = True) -> pd.DataFrame:
    df = normalize_orders(df_orders, order_granularity)
    out = (df.groupby('mobile_number')
             .size()
             .rename('order_count')
             .reset_index()
             .query('order_count > 1')
             .sort_values(['order_count','mobile_number'], ascending=[False, True])
             .reset_index(drop=True))
    if mask_pii:
        out['mobile_number'] = _mask_mobile(out['mobile_number'])
    return out

def monthly_order_trends(df_orders: pd.DataFrame, tz_name: str = 'Asia/Kolkata', order_granularity: str = 'header') -> pd.DataFrame:
    df = normalize_orders(df_orders, order_granularity)
    df['ym'] = (df['order_date_time'].dt.tz_convert(tz_name).dt.strftime('%Y-%m'))
    return (df.groupby('ym').size().rename('orders').reset_index().sort_values('ym').reset_index(drop=True))

def regional_revenue(df_orders: pd.DataFrame, df_customers: pd.DataFrame) -> pd.DataFrame:
    # Revenue computed at header level to avoid double counting where XML repeats total_amount per line
    dfh = normalize_orders(df_orders, 'header')
    df = dfh.merge(df_customers[['mobile_number','region']].drop_duplicates(subset=['mobile_number']), on='mobile_number', how='left')
    return (df.groupby('region', dropna=False)['total_amount']
              .sum()
              .rename('revenue')
              .reset_index()
              .sort_values('revenue', ascending=False)
              .reset_index(drop=True))

def top_customers_window(df_orders: pd.DataFrame, window_days: int = 30, now_override: datetime | None = None, mask_pii: bool = True) -> pd.DataFrame:
    ref_now = now_override if now_override is not None else now_utc()
    since = ref_now - timedelta(days=window_days)
    dfh = normalize_orders(df_orders, 'header')
    dfw = dfh[dfh['order_date_time'] >= since]
    out = (dfw.groupby('mobile_number')['total_amount']
             .sum()
             .rename('spend_window')
             .reset_index()
             .sort_values('spend_window', ascending=False)
             .reset_index(drop=True))
    if mask_pii:
        out['mobile_number'] = _mask_mobile(out['mobile_number'])
    return out
