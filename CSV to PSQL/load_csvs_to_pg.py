import os, re, glob
from pathlib import Path
import pandas as pd
import csv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL
from dotenv import load_dotenv

# --- Load database credentials ---
BASE_DIR = Path(__file__).resolve().parent
for env_path in (BASE_DIR / "config" / ".env", BASE_DIR / ".env"):
    if env_path.exists():
        load_dotenv(env_path)
        break

PGHOST = os.getenv("PGHOST", "127.0.0.1")
PGPORT = os.getenv("PGPORT")
PGDB = os.getenv("PGDATABASE")
PGUSER = os.getenv("PGUSER")
PGPW = os.getenv("PGPASSWORD")

# Validate required env vars
missing = [k for k, v in {"PGDATABASE": PGDB, "PGUSER": PGUSER, "PGPASSWORD": PGPW}.items() if not v]
if missing:
    raise SystemExit(f"Missing required env vars: {', '.join(missing)}. Ensure they are set in .env or your environment.")

port = int(PGPORT) if (PGPORT and PGPORT.isdigit()) else None

# Connect to PostgreSQL (omit port if not provided)
db_url = URL.create(
    "postgresql+psycopg2",
    username=PGUSER,
    password=PGPW,
    host=PGHOST,
    port=port,
    database=PGDB,
)
engine = create_engine(db_url, future=True)

# Helper: convert filename into safe table name
def fname_to_table(fp):
    name = os.path.splitext(os.path.basename(fp))[0]
    return re.sub(r'[^0-9a-zA-Z_]+', '_', name).lower()


# Find all CSVs in data/raw folder (relative to this script)
data_dir = BASE_DIR / "data" / "raw"
csv_paths = sorted(str(p) for p in data_dir.glob("*.csv"))
if not csv_paths:
    raise SystemExit(f"No CSVs found in {data_dir}! Current working dir is {Path.cwd()}.")

# Load each CSV into PostgreSQL
for fp in csv_paths:
    tbl = fname_to_table(fp)
    print(f"Loading {fp} -> table {tbl} ...")

    # Read CSV into pandas DataFrame (robust to delimiters/encodings)
    def read_csv_robust(path: str) -> pd.DataFrame:
        encodings = ["utf-8", "utf-8-sig", "cp1252"]
        # Try sniffing delimiter first
        for enc in encodings:
            try:
                with open(path, "r", encoding=enc, errors="replace") as f:
                    sample = f.read(32768)
                try:
                    dialect = csv.Sniffer().sniff(sample, delimiters=",;\t|")
                    sep = dialect.delimiter
                except Exception:
                    sep = None  # let pandas infer
                return pd.read_csv(
                    path,
                    sep=sep,  # None -> infer when using engine='python'
                    engine="python",
                    encoding=enc,
                    on_bad_lines="error",
                )
            except Exception as e_first:
                # Fallback: skip bad lines
                try:
                    df_fb = pd.read_csv(
                        path,
                        sep=None,
                        engine="python",
                        encoding=enc,
                        on_bad_lines="skip",
                    )
                    print(f"! Warning: {path} read with encoding={enc} skipping malformed lines: {e_first}")
                    return df_fb
                except Exception:
                    continue
        # Last resort with default encoding and skipping bad lines
        return pd.read_csv(path, engine="python", sep=None, on_bad_lines="skip")

    try:
        df = read_csv_robust(fp)
    # Keep original column labels (mixed case) as in the CSV
        # Write to Postgres (replace table each time)
        df.to_sql(
            tbl,
            con=engine,
            schema="public",
            if_exists="replace",
            index=False,
            method="multi",
            chunksize=50000,
        )
        print(f"✓ Loaded {len(df):,} rows into table {tbl}")
    except Exception as e:
        print(f"x Failed to load {fp} -> {tbl}: {e}")

print("All CSVs loaded successfully into PostgreSQL (schema=public).")