from pathlib import Path
import pandas as pd


def read_orders_csv(path: Path) -> pd.DataFrame:
  
    return pd.read_csv(path)


def read_users_csv(path: Path) -> pd.DataFrame:

    return pd.read_csv(path)


def write_parquet(df: pd.DataFrame, path: Path) -> None:
  
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False)
