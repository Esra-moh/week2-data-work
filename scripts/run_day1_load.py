from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[1]

sys.path.append(str(ROOT / "src"))


from bootcamp_data.config import make_paths
from bootcamp_data.io import read_orders_csv, read_users_csv, write_parquet
from bootcamp_data.transforms import enforce_schema


def main() -> None:
    paths = make_paths(ROOT)

    orders_raw = read_orders_csv(paths.raw / "orders.csv")
    users_raw = read_users_csv(paths.raw / "users.csv")


    orders_clean = enforce_schema(orders_raw)

  
    write_parquet(orders_clean, paths.processed / "orders.parquet")


if __name__ == "__main__":
    main()

  
    import pandas as pd

    df = pd.read_parquet("data/processed/orders.parquet")
    print(df.head())