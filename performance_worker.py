import os
import psycopg2
from datetime import datetime, timezone

DATABASE_URL = os.environ["DATABASE_URL"]

def get_conn():
    return psycopg2.connect(DATABASE_URL, sslmode="require")

def run():
    print("PERFORMANCE WORKER RUNNING")

    with get_conn() as conn:
        with conn.cursor() as cur:

            # 1) Fetch alerts not yet evaluated
            cur.execute("""
                SELECT
                    a.id,
                    a.ticker,
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

            if not alerts:
                return

            for alert_id, ticker, side, signal_time, entry_price, rating in alerts:
                exit_price = entry_price  # TEMP: neutral exit
                exit_time = datetime.now(timezone.utc)
                return_pct = 0.0

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
                    ticker,
                    side,
                    signal_time,
                    entry_price,
                    exit_time,
                    exit_price,
                    return_pct,
                    rating
                ))

                print(f"Inserted performance for alert {alert_id}")

        conn.commit()

if __name__ == "__main__":
    run()
