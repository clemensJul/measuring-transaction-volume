import duckdb
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DB_PATH = PROJECT_ROOT / "data" / "main.duckdb"

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS blocks (
  "number"      BIGINT PRIMARY KEY,
  "timestamp"   TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS transactions (
  "hash"         VARCHAR,
  "log_number"   INTEGER,
  "block_number" BIGINT REFERENCES blocks(number),
  "coin"         VARCHAR,
  "from_addr"    VARCHAR,
  "to_addr"      VARCHAR,
  "amount"       HUGEINT,
  "usd_value"    DOUBLE,
  PRIMARY KEY (hash, log_number)
);

CREATE TABLE IF NOT EXISTS block_ingestions (
  "block_number" BIGINT REFERENCES blocks(number),
  "coin"         VARCHAR,
  PRIMARY KEY (block_number, coin)
);

CREATE TABLE IF NOT EXISTS coin_values (
  "coin"        VARCHAR,
  "date"        DATE,
  "usd_value"   DOUBLE,
  PRIMARY KEY (coin, date)
);

CREATE INDEX IF NOT EXISTS idx_coin ON transactions(block_number, coin);
CREATE INDEX IF NOT EXISTS idx_coin_value ON coin_values(coin, date);
"""

def open_db() -> duckdb.DuckDBPyConnection:
    Path(DB_PATH).parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(DB_PATH))
    con.execute(SCHEMA_SQL)
    return con

if __name__ == "__main__":
    con = open_db()
    print("DB ready.")
