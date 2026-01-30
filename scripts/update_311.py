import os
from datetime import datetime, timedelta, timezone

import pandas as pd
import requests
import matplotlib.pyplot as plt

BASE = "https://data.cityofnewyork.us/resource/erm2-nwe9.json"

def soql_get(params, app_token=None):
    headers = {}
    if app_token:
        headers["X-App-Token"] = app_token
    r = requests.get(BASE, params=params, headers=headers, timeout=60)
    r.raise_for_status()
    return r.json()

def socrata_dt(dt):
    # Matches the dataset format you saw: 2026-01-28T02:05:51.000
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000")

def escape_soql_string(s: str) -> str:
    return s.replace("'", "''")

def ensure_dirs():
    os.makedirs("data", exist_ok=True)
    os.makedirs("images", exist_ok=True)

def save_top20_chart(comp: pd.DataFrame):
    top20 = comp.sort_values("n_this", ascending=False).head(20)

    plt.figure()
    plt.barh(top20["complaint_type"][::-1], top20["n_this"][::-1])
    plt.title("NYC 311: Top Complaint Types (Last 7 Days)")
    plt.xlabel("Requests")
    plt.ylabel("")
    plt.tight_layout()
    plt.savefig("images/top20_latest.png", dpi=150)
    plt.close()

def save_movers_charts(comp: pd.DataFrame):
    up = comp.sort_values("delta", ascending=False).head(15)
    down = comp.sort_values("delta", ascending=True).head(15)

    plt.figure()
    plt.barh(up["complaint_type"][::-1], up["delta"][::-1])
    plt.title("NYC 311: Biggest Increases vs Prior Week")
    plt.xlabel("Δ Requests")
    plt.tight_layout()
    plt.savefig("images/movers_up_latest.png", dpi=150)
    plt.close()

    plt.figure()
    plt.barh(down["complaint_type"][::-1], down["delta"][::-1])
    plt.title("NYC 311: Biggest Decreases vs Prior Week")
    plt.xlabel("Δ Requests")
    plt.tight_layout()
    plt.savefig("images/movers_down_latest.png", dpi=150)
    plt.close()

def save_trend_chart(comp: pd.DataFrame, app_token=None):
    now = datetime.now(timezone.utc)
    start_30 = now - timedelta(days=30)

    top5 = comp.sort_values("n_this", ascending=False).head(5)["complaint_type"].tolist()
    where_types = " OR ".join([f"complaint_type = '{escape_soql_string(t)}'" for t in top5])

    params_trend = {
        "$select": "date_trunc_ymd(created_date) as day, complaint_type, count(*) as n",
        "$where": f"created_date >= '{socrata_dt(start_30)}' AND ({where_types})",
        "$group": "day, complaint_type",
        "$order": "day ASC",
        "$limit": 50000
    }

    trend = pd.DataFrame(soql_get(params_trend, app_token=app_token))
    if trend.empty:
        # Nothing to plot (rare). Avoid failing the workflow.
        return

    trend["n"] = trend["n"].astype(int)
    trend["day"] = pd.to_datetime(trend["day"])

    pivot = (trend.pivot_table(index="day", columns="complaint_type", values="n", aggfunc="sum")
                  .fillna(0)
                  .sort_index())

    plt.figure()
    for col in pivot.columns:
        plt.plot(pivot.index, pivot[col], label=col)
    plt.title("NYC 311: Daily Requests (Top 5, Last 30 Days)")
    plt.xlabel("Day")
    plt.ylabel("Requests")
    plt.legend()
    plt.tight_layout()
    plt.savefig("images/trend_latest.png", dpi=150)
    plt.close()

def update_readme_timestamp():
    path = "README.md"
    if not os.path.exists(path):
        return
    with open(path, "r", encoding="utf-8") as f:
        content = f.read()

    stamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    line = f"**Last updated:** {stamp}"

    if "**Last updated:**" in content:
        # Replace the existing line
        import re
        content = re.sub(r"\*\*Last updated:\*\*.*", line, content)
    else:
        # Add near the top
        content = content.replace("\n", f"\n{line}\n", 1)

    with open(path, "w", encoding="utf-8") as f:
        f.write(content)

def main():
    ensure_dirs()

    app_token = os.getenv("SOCRATA_APP_TOKEN")  # optional
    now = datetime.now(timezone.utc)
    start_this = now - timedelta(days=7)
    start_last = now - timedelta(days=14)
    end_last = now - timedelta(days=7)

    params_this = {
        "$select": "complaint_type, count(*) as n",
        "$where": f"created_date >= '{socrata_dt(start_this)}'",
        "$group": "complaint_type",
        "$order": "count(*) DESC",
        "$limit": 200
    }

    params_last = {
        "$select": "complaint_type, count(*) as n",
        "$where": f"created_date >= '{socrata_dt(start_last)}' AND created_date < '{socrata_dt(end_last)}'",
        "$group": "complaint_type",
        "$order": "count(*) DESC",
        "$limit": 500
    }

    this_week = pd.DataFrame(soql_get(params_this, app_token=app_token))
    last_week = pd.DataFrame(soql_get(params_last, app_token=app_token))

    if this_week.empty:
        raise RuntimeError("No data returned for this_week. Query may have failed silently.")

    this_week["n"] = this_week["n"].astype(int)
    if not last_week.empty:
        last_week["n"] = last_week["n"].astype(int)

    comp = (
        this_week.rename(columns={"n": "n_this"})
        .merge(last_week.rename(columns={"n": "n_last"}), on="complaint_type", how="left")
        .fillna({"n_last": 0})
    )
    comp["n_last"] = comp["n_last"].astype(int)
    comp["delta"] = comp["n_this"] - comp["n_last"]

    # Save outputs
    comp.sort_values("n_this", ascending=False).to_csv("data/latest_weekly_summary.csv", index=False)

    save_top20_chart(comp)
    save_movers_charts(comp)
    save_trend_chart(comp, app_token=app_token)

    update_readme_timestamp()

if __name__ == "__main__":
    main()
