from __future__ import annotations
import argparse
from pathlib import Path
from datetime import datetime, timezone
import pandas as pd
import matplotlib.pyplot as plt

def run_ingest(customers_path: str, orders_path: str):
    from .ingest import ingest_all
    res = ingest_all(customers_path, orders_path)
    print("Ingestion summary:", res)

def _plot_series(df: pd.DataFrame, x: str, y: str, title: str, outpath: Path):
    plt.figure()
    plt.plot(df[x], df[y], marker='o')
    plt.title(title)
    plt.xlabel(x)
    plt.ylabel(y)
    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()
    plt.savefig(outpath)
    plt.close()

def _plot_bar(df: pd.DataFrame, x: str, y: str, title: str, outpath: Path):
    plt.figure()
    plt.bar(df[x], df[y])
    plt.title(title)
    plt.xlabel(x)
    plt.ylabel(y)
    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()
    plt.savefig(outpath)
    plt.close()

def run_kpis_memory(customers_path: str, orders_path: str, outdir: str | None, tz_name: str,
                    order_granularity: str, date_window: int, now_str: str | None, mask_pii: bool):
    from .ingest import load_customers_csv, load_orders_xml
    from .kpis_memory import repeat_customers, monthly_order_trends, regional_revenue, top_customers_window

    # Load
    cdf = load_customers_csv(customers_path)
    odf = load_orders_xml(orders_path)

    # Parse now override
    now_override = None
    if now_str:
        now_override = datetime.fromisoformat(now_str).replace(tzinfo=timezone.utc)

    # KPIs
    rep = repeat_customers(odf, order_granularity=order_granularity, mask_pii=mask_pii)
    mon = monthly_order_trends(odf, tz_name=tz_name, order_granularity=order_granularity)
    reg = regional_revenue(odf, cdf)
    top = top_customers_window(odf, window_days=date_window, now_override=now_override, mask_pii=mask_pii)

    # Print
    print("\n[MEM] Repeat customers:"); print(rep.head(20))
    print("\n[MEM] Monthly order trends:"); print(mon.head(24))
    print("\n[MEM] Regional revenue:"); print(reg.head(50))
    print(f"\n[MEM] Top customers last {date_window} days:"); print(top.head(20))

    # Outputs
    if outdir:
        out_path = Path(outdir); out_path.mkdir(parents=True, exist_ok=True)
        plots = out_path / "plots"; plots.mkdir(parents=True, exist_ok=True)

        rep.to_csv(out_path / "repeat_customers.csv", index=False)
        mon.to_csv(out_path / "monthly_order_trends.csv", index=False)
        reg.to_csv(out_path / "regional_revenue.csv", index=False)
        top.to_csv(out_path / "top_customers_last_{0}_days.csv".format(date_window), index=False)

        # Plots
        if not mon.empty:
            _plot_series(mon, "ym", "orders", "Orders by Month", plots / "orders_by_month.png")
        if not reg.empty:
            _plot_bar(reg, "region", "revenue", "Revenue by Region", plots / "revenue_by_region.png")
        if not top.empty:
            _plot_bar(top.head(10), "mobile_number", "spend_window", "Top Customers (Window)", plots / "top_customers.png")

        # Summary.md
        summary = []
        summary.append(f"# Run Summary\n")
        summary.append(f"- Customers file: `{customers_path}`")
        summary.append(f"- Orders file: `{orders_path}`")
        summary.append(f"- Timezone (monthly trends): `{tz_name}`")
        summary.append(f"- Order granularity: `{order_granularity}`")
        summary.append(f"- Date window (days): `{date_window}`")
        summary.append(f"- Now override (UTC): `{now_str or 'None'}`")
        summary.append(f"- Mask PII: `{mask_pii}`\n")
        def tab(df: pd.DataFrame, title: str, n: int = 10):
            return f"## {title}\n\n" + df.head(n).to_markdown(index=False) + "\n"
        summary.append(tab(rep, "Repeat Customers (top)"))
        summary.append(tab(mon, "Monthly Order Trends"))
        summary.append(tab(reg, "Regional Revenue"))
        summary.append(tab(top, f"Top Customers (last {date_window} days)"))
        (out_path / "summary.md").write_text("\n".join(summary), encoding="utf-8")
        print(f"\nSaved CSVs, plots, and summary to: {out_path.resolve()}\n")

def run_kpis_sql():
    from .kpis_sql import repeat_customers as sql_repeat, monthly_order_trends as sql_monthly, regional_revenue as sql_revenue, top_customers_last_30_days as sql_top
    print("\n[SQL] Repeat customers:", sql_repeat())
    print("[SQL] Monthly order trends:", sql_monthly())
    print("[SQL] Regional revenue:", sql_revenue())
    print("[SQL] Top customers last 30 days:", sql_top())

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--ingest", action="store_true", help="Ingest data to MySQL")
    ap.add_argument("--kpis-sql", action="store_true", help="Compute KPIs via SQL on MySQL tables")
    ap.add_argument("--kpis-memory", action="store_true", help="Compute KPIs via in-memory Pandas")
    ap.add_argument("--customers", default="task_DE_new_customers.csv")
    ap.add_argument("--orders", default="task_DE_new_orders.xml")
    ap.add_argument("--outdir", default="outputs", help="Directory to write outputs (CSV, plots, summary)")
    ap.add_argument("--tz", default="Asia/Kolkata")
    ap.add_argument("--order-granularity", choices=["header","line"], default="header")
    ap.add_argument("--date-window", type=int, default=30)
    ap.add_argument("--now", dest="now_str", default=None, help="Override 'now' in UTC, e.g., 2025-11-07")
    ap.add_argument("--mask-pii", type=str, default="true", help="true|false")
    args = ap.parse_args()

    if args.ingest:
        run_ingest(args.customers, args.orders)

    if args.kpis_sql:
        run_kpis_sql()

    if args.kpis_memory:
        mask_pii = str(args.mask_pii).lower() in ("1","true","yes","y")
        outdir = args.outdir if args.outdir.strip() else None
        run_kpis_memory(args.customers, args.orders, outdir, args.tz, args.order_granularity, args.date_window, args.now_str, mask_pii)

if __name__ == "__main__":
    main()
