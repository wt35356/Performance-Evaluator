import os
import psycopg2
import yfinance as yf
from datetime import datetime, timezone

DATABASE_URL = os.environ["DATABASE_URL"]

def get_conn():
    return psycopg2.connect(DATABASE_URL, sslmode="require")

def get_last_price(symbol: str) -> float | None:
    try:
        t = yf.Ticker(symbol)
        data = t.fast_info
        return data.get("lastPrice")
    except Exception:
        return None

def run():
    print("PERFORMANCE WORKER RUNNING")

    with get_conn() as conn:
        with conn.cursor() as cur:

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
                LIMIT 50;
            """)

            alerts = cur.fetchall()
            print(f"Fetched {len(alerts)} alerts")

            for alert_id, symbol, side, signal_time, entry_price, rating in alerts:

                exit_price = get_last_price(symbol)
                if exit_price is None:
                    print(f"Price fetch failed for {symbol}, skipping")
                    continue

                if side.upper() == "BULL":
                    return_pct = (exit_price - entry_price) / entry_price * 100
                else:  # BEAR
                    return_pct = (entry_price - exit_price) / entry_price * 100

                exit_time = datetime.now(timezone.utc)

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
                    exit_time,
                    exit_price,
                    return_pct,
                    rating
                ))

                print(f"Inserted real performance for alert {alert_id}")

        conn.commit()

if __name__ == "__main__":
    run()
