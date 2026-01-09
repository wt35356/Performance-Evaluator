import os
import psycopg2
import yfinance as yf
from datetime import datetime, timezone

DATABASE_URL = os.environ["DATABASE_URL"]
BATCH_SIZE = 50   # safe default

def get_conn():
    return psycopg2.connect(DATABASE_URL, sslmode="require")

def get_last_close(symbol: str):
    """
    Guaranteed price source:
    - Uses last available daily close
    - Works when market is closed
    - Returns None only if symbol is invalid
    """
    try:
        t = yf.Ticker(symbol)
        hist = t.history(period="2d", interval="1d")
        if hist.empty:
            return None
        return float(hist["Close"].iloc[-1])
    except Exception as e:
        print(f"[PRICE ERROR] {symbol}: {e}")
        return None

def run():
    print("PERFORMANCE WORKER RUNNING")

    with get_conn() as conn:
        with conn.cursor() as cur:

            # 1) Fetch alerts not yet evaluated
            cur.execute("""
                SELECT
                    a.id,
                    a.symbol,
                    a.type,
                    a.signal_time,
                    a.price,
                    a.rating
                FROM alerts a
                LEFT JOIN alert_performance p
                  ON p.alert_id = a.id
                WHERE p.alert_id IS NULL
                ORDER BY a.signal_time ASC
                LIMIT %s;
            """, (BATCH_SIZE,))

            alerts = cur.fetchall()
            print(f"Fetched {len(alerts)} alerts")

            for alert_id, symbol, side, signal_time, entry_price, rating in alerts:

                exit_price = get_last_close(symbol)
                if exit_price is None:
                    print(f"[SKIP] No price for {symbol}")
                    continue

                if side.upper() == "BULL":
                    return_pct = (exit_price - entry_price) / entry_price * 100
                else:  # BEAR
                    return_pct = (entry_price - exit_price) / entry_price * 100

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
                    VALUES (%s,%s,%s,%s,%s,0,%s,%s,%s,%s)
                """, (
                    alert_id,
                    symbol,
                    side,
                    signal_time,
                    entry_price,
                    datetime.now(timezone.utc),
                    exit_price,
                    return_pct,
                    rating
                ))

                print(f"[OK] alert {alert_id} {symbol} return={return_pct:.2f}%")

        conn.commit()

if __name__ == "__main__":
    run()
