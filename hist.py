import sys
import duckdb
import yfinance as yf
from datetime import datetime, timezone
from prefect import flow, task


DB_PATH = "market_data.duckdb"
TICKERS = ["AAPL", "TSLA", "MSFT", "F", "UEC"]
HISTORICAL_START = "2015-01-01"


@task(name="setup_duckdb")
def setup_duckdb(db_path: str) -> None:
    con = duckdb.connect(db_path)
    con.execute("CREATE SCHEMA IF NOT EXISTS bronze")
    con.execute("""
        CREATE TABLE IF NOT EXISTS bronze.tabla_precios (
            created_at  TIMESTAMPTZ NOT NULL,
            updated_at  TIMESTAMPTZ NOT NULL,
            fecha       DATE        NOT NULL,
            ticker      VARCHAR     NOT NULL,
            price       DOUBLE      NOT NULL
        )
    """)
    con.close()


@task(name="fetch_historical_prices", retries=2, retry_delay_seconds=10)
def fetch_historical_prices(ticker: str, start: str = HISTORICAL_START) -> list[dict]:
    hist = yf.Ticker(ticker).history(start=start, auto_adjust=True)
    now = datetime.now(timezone.utc)
    rows = []
    for date_idx, row in hist.iterrows():
        rows.append({
            "created_at": now,
            "updated_at": now,
            "fecha": date_idx.date(),
            "ticker": ticker,
            "price": float(row["Close"]),
        })
    return rows


@task(name="load_to_bronze")
def load_to_bronze(db_path: str, rows: list[dict]) -> int:
    if not rows:
        return 0
    con = duckdb.connect(db_path)
    ticker = rows[0]["ticker"]
    data = [
        (r["created_at"], r["updated_at"], r["fecha"], r["ticker"], r["price"])
        for r in rows
    ]
    con.execute("DROP TABLE IF EXISTS staging_prices")
    con.execute("""
        CREATE TEMP TABLE staging_prices (
            created_at  TIMESTAMPTZ,
            updated_at  TIMESTAMPTZ,
            fecha       DATE,
            ticker      VARCHAR,
            price       DOUBLE
        )
    """)
    con.executemany("INSERT INTO staging_prices VALUES (?, ?, ?, ?, ?)", data)
    before = con.execute(
        "SELECT COUNT(*) FROM bronze.tabla_precios WHERE ticker = ?", [ticker]
    ).fetchone()[0]
    con.execute("""
        INSERT INTO bronze.tabla_precios
        SELECT s.*
        FROM staging_prices s
        WHERE NOT EXISTS (
            SELECT 1 FROM bronze.tabla_precios p
            WHERE p.fecha = s.fecha AND p.ticker = s.ticker
        )
    """)
    after = con.execute(
        "SELECT COUNT(*) FROM bronze.tabla_precios WHERE ticker = ?", [ticker]
    ).fetchone()[0]
    con.execute("DROP TABLE IF EXISTS staging_prices")
    con.close()
    return after - before


@flow(name="historical_load", log_prints=True)
def historical_load(
    tickers: list[str] = TICKERS,
    db_path: str = DB_PATH,
    start: str = HISTORICAL_START,
) -> None:
    setup_duckdb(db_path)
    for ticker in tickers:
        rows = fetch_historical_prices(ticker, start=start)
        loaded = load_to_bronze(db_path, rows)
        print(f"{ticker}: {loaded} row(s) inserted (historical from {start}) into bronze.tabla_precios")


if __name__ == "__main__":
    start = sys.argv[1] if len(sys.argv) > 1 else HISTORICAL_START
    historical_load(start=start)
