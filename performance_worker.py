import os
import time
import requests
import psycopg2
from datetime import datetime, timedelta, timezone

DATABASE_URL = os.environ["DATABASE_URL"]
ALPHA_KEY = os.environ["ALPHA_VANTAGE_API_KEY"]

HORIZON_HOURS = 24
SLEEP_SECONDS = 15  # Alpha Vantage free tier

def get_conn():
    return psycopg2.connect(DATABASE_URL, sslmode="require")

def fetch_hourly(symbol):
    r = requests.get(
        "https://www.alphavantage.co/query",
        params={
            "function": "TIME_SERIES_INTRADAY",
            "symbol": symbol,
            "interval": "60min",
            "outputsize": "full",
            "apikey": ALPHA_KEY,
        },
        timeout=30,
    )
    r.raise_for_status()
    data = r.json().get("Time Series (60min)", {})

    series = {}
    for ts, v in data.items():
        # Treat timestamps as UTC-naive but consistent
        dt = datetime.fromisoformat(ts).replace(tzinfo=timezone.utc)
        series[dt] = float(v["4. close"])

    return series

def run():
    price_cache = {}
    print("RUN FUNCTION ENTERED")

    with get_conn() as conn:
        with conn.cursor() as cur:

            cur.execute("""
                SELECT a.id, a.symbol, a.type, a.signal_time, a.price, a.rating
                FROM alerts a
                LEFT JOIN alert_performance p ON p.alert_id = a.id
                WHERE p.alert_id IS NULL
                  AND a.signal_time < NOW() - INTERVAL '24 hours'
                ORDER BY a.signal_time ASC
            """)

            alerts = cur.fetchall()

            for alert_id, symbol, side, t0, entry_price, rating in alerts:

                if symbol not in price_cache:
                    price_cache[symbol] = fetch_hourly(symbol)
                    time.sleep(SLEEP_SECONDS)

                series = price_cache[symbol]

                cutoff = t0 + timedelta(hours=HORIZON_HOURS)

                eligible = [(ts, px) for ts, px in series.items() if ts <= cutoff]
                if not eligible:
                    continue

                eligible.sort(key=lambda x: x[0], reverse=True)
                exit_time_actual, exit_price = eligible[0]

                ret = (exit_price - entry_price) / entry_price
                if side.upper() == "BEAR":
                    ret = -ret

                cur.execute("""
                    INSERT INTO alert_performance (
                        alert_id,
                        symbol,
                        type,
                        alert_time,
                        entry_price,
                        horizon_hours,
                        exit_time,
                        exit_price,
                        return_pct,
                        rating
                    )
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """, (
                    alert_id,
                    symbol,
                    side,
                    t0,
                    entry_price,
                    HORIZON_HOURS,
                    exit_time_actual,
                    exit_price,
                    ret * 100,
                    rating,
                ))

        conn.commit()

if __name__ == "__main__":
    run()
