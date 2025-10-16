import os, re, pandas as pd
from sqlalchemy import create_engine, text

# === 配置 ===
DB_URL = os.getenv("DATABASE_URL", "sqlite:///data/app.db")
NYC_PATH = os.getenv("NYC311_SNAPSHOT", "data/raw/nyc_311_12mo.csv.gz")
WINE_PATH = os.getenv("WINE_SNAPSHOT",  "data/raw/winemag-data-130k-v2.csv")

CHUNKSIZE = int(os.getenv("CHUNKSIZE", "100000"))
engine = create_engine(DB_URL)

def clean_name(s: str) -> str:
    s = re.sub(r"[^0-9A-Za-z_]", "_", s).lower()
    s = re.sub(r"_+", "_", s).strip("_")
    return s or "col"

def load_csv(path: str, table: str, parse_dates=None):
    table = clean_name(table)
    first = True
    kwargs = {"low_memory": False}
    # 自动识别压缩
    if path.endswith(".gz"):
        kwargs["compression"] = "gzip"
    # 自动日期解析
    if parse_dates:
        kwargs["parse_dates"] = parse_dates

    print(f"[load] {path} -> table '{table}'")
    total = 0
    for chunk in pd.read_csv(path, chunksize=CHUNKSIZE, **kwargs):
        # 规范列名
        chunk.columns = [clean_name(c) for c in chunk.columns]
        # 写入
        chunk.to_sql(table, engine, if_exists="replace" if first else "append", index=False)
        total += len(chunk)
        first = False
        print(f"  + {len(chunk):,} rows (total {total:,})")
    return table, total

def add_indexes(table: str, cols: list[str]):
    with engine.begin() as conn:
        for c in cols:
            idx = f"idx_{table}_{clean_name(c)}"
            try:
                conn.execute(text(f'CREATE INDEX IF NOT EXISTS {idx} ON "{table}"("{clean_name(c)}")'))
                print(f"  - index on {table}({c}) OK")
            except Exception as e:
                print(f"  - index on {table}({c}) skipped: {e}")

def main():
    os.makedirs("data", exist_ok=True)

    nyc_table, nyc_rows = load_csv(
        NYC_PATH, "nyc_311",
        parse_dates=["created_date","closed_date"]  # NYC 311 的时间列
    )
    add_indexes(nyc_table, ["created_date", "borough", "agency", "complaint_type", "status"])

    wine_table, wine_rows = load_csv(
        WINE_PATH, "wine_reviews"
        # Wine Reviews 无需特意 parse_dates
    )
    add_indexes(wine_table, ["country", "province", "variety", "points", "price"])

    print("\n=== Done ===")
    print("DB URL:", DB_URL)
    print(f"{nyc_table}: {nyc_rows:,} rows")
    print(f"{wine_table}: {wine_rows:,} rows")

if __name__ == "__main__":
    main()
