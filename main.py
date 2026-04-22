import duckdb
import yfinance as yf
from datetime import datetime, timezone
from prefect import flow, task


DB_PATH = "market_data.duckdb"
TICKERS = ["AAPL", "TSLA", "MSFT", "F", "UEC"]


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


@task(name="fetch_prices", retries=2, retry_delay_seconds=10)
def fetch_prices(ticker: str) -> list[dict]:
    hist = yf.Ticker(ticker).history(period="1d")
    now = datetime.now(timezone.utc)
    rows = []
    for date, row in hist.iterrows():
        rows.append({
            "created_at": now,
            "updated_at": now,
            "fecha": date.date(),
            "ticker": ticker,
            "price": float(row["Close"]),
        })
    return rows


@task(name="load_to_bronze")
def load_to_bronze(db_path: str, rows: list[dict]) -> int:
    if not rows:
        return 0
    con = duckdb.connect(db_path)
    con.executemany(
        """
        INSERT INTO bronze.tabla_precios (created_at, updated_at, fecha, ticker, price)
        VALUES (?, ?, ?, ?, ?)
        """,
        [(r["created_at"], r["updated_at"], r["fecha"], r["ticker"], r["price"]) for r in rows],
    )
    con.close()
    return len(rows)


@flow(name="ingest_market_prices", log_prints=True)
def ingest_market_prices(tickers: list[str] = TICKERS, db_path: str = DB_PATH) -> None:
    setup_duckdb(db_path)
    for ticker in tickers:
        rows = fetch_prices(ticker)
        loaded = load_to_bronze(db_path, rows)
        print(f"{ticker}: {loaded} row(s) loaded into bronze.tabla_precios")


if __name__ == "__main__":
    ingest_market_prices()
