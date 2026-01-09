import os
import psycopg2
import yfinance as yf
from datetime import datetime, timedelta, timezone

DATABASE_URL = os.environ["DATABASE_URL"]

HORIZONS = [1, 4, 24]   # hours
BATCH_SIZE = 50        # alerts per run (safe)
LOOKBACK_DAYS = 5      # enough for intraday fetch

def get_conn():
    return psycopg2.connect(DATABASE_URL, sslmode="require")

def fetch_intraday(symbol: str):
    """
    Returns a dict: {datetime_utc: close_price}
    Uses 60m candles (stable, low rate).
    """
    try:
        t = yf.Ticker(symbol)
        hist = t.history(
            period=f"{LOOKBACK_DAYS}d",
            interval="60m",
            auto_adjust=False
        )
        if hist.empty:
            return {}

        series = {}
        for ts, row in hist.iterrows():
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)
            series[ts.astimezone(timezone.utc)] = float(row["Close"])

        return series
    except Exception as e:
        print(f"[PRICE ERROR] {symbol}: {e}")
        return {}

def run():
    print("PERFORMANCE WORKER RUNNING")

    with get_conn() as conn:
        with conn.cursor() as cur:

            # ---- fetch alerts needing at least one horizon ----
            cur.execute("""
                SELECT
                    a.id,
                    a.symbol,
                    a.type,
                    a.signal_time,
                    a.price
                FROM alerts a
                WHERE EXISTS (
                    SELECT 1
                    FROM (
                        SELECT unnest(%s::int[]) AS h
                    ) horizons
                    WHERE NOT EXISTS (
                        SELECT 1
                        FROM alert_performance p
                        WHERE p.alert_id = a.id
                          AND p.horizon_hours = horizons.h
                    )
                )
                ORDER BY a.signal_time ASC
                LIMIT %s;
            """, (HORIZONS, BATCH_SIZE))

            alerts = cur.fetchall()
            print(f"Fetched {len(alerts)} alerts")

            price_cache = {}

            for alert_id, symbol, side, signal_time, entry_price in alerts:

                if symbol not in price_cache:
                    price_cache[symbol] = fetch_intraday(symbol)

                series = price_cache[symbol]
                if not series:
                    continue

                for h in HORIZONS:
                    # skip if already computed
                    cur.execute("""
                        SELECT 1 FROM alert_performance
                        WHERE alert_id = %s AND horizon_hours = %s
                    """, (alert_id, h))
                    if cur.fetchone():
                        continue

                    cutoff = signal_time + timedelta(hours=h)

                    # find last close <= cutoff
                    eligible = [
                        (ts, px) for ts, px in series.items()
                        if ts <= cutoff
                    ]
                    if not eligible:
                        continue

                    eligible.sort(key=lambda x: x[0], reverse=True)
                    exit_time, exit_price = eligible[0]

                    if side.upper() == "BULL":
                        ret = (exit_price - entry_price) / entry_price * 100
                    else:  # BEAR
                        ret = (entry_price - exit_price) / entry_price * 100

                    cur.execute("""
                        INSERT INTO alert_performance (
                            alert_id,
                            horizon_hours,
                            exit_time,
                            exit_price,
                            return_pct
                        )
                        VALUES (%s,%s,%s,%s,%s)
                        ON CONFLICT DO NOTHING
                    """, (
                        alert_id,
                        h,
                        exit_time,
                        exit_price,
                        ret
                    ))

                    print(f"[OK] alert {alert_id} {symbol} {h}h = {ret:.2f}%")

        conn.commit()

if __name__ == "__main__":
    run()
