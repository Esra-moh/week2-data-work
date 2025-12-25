from __future__ import annotations

import logging
import sys
from pathlib import Path

import pandas as pd

# make src/ importable
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from bootcamp_data.config import make_paths
from bootcamp_data.quality import (
    require_columns,
    assert_non_empty,
    assert_unique_key,
)
from bootcamp_data.transforms import (
    parse_datetime,
    add_time_parts,
    winsorize,
)
from bootcamp_data.joins import safe_left_join

# optional
try:
    from bootcamp_data.transforms import add_outlier_flag
except ImportError:
    add_outlier_flag = None


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(levelname)s %(message)s",
    )
    log = logging.getLogger(__name__)

    p = make_paths(ROOT)

    orders = pd.read_parquet(p.processed / "orders_clean.parquet")
    users = pd.read_parquet(p.processed / "users.parquet")

    require_columns(
        orders,
        ["order_id", "user_id", "amount", "quantity", "created_at", "status_clean"],
    )
    require_columns(users, ["user_id", "country", "signup_date"])

    assert_non_empty(orders, "orders")
    assert_non_empty(users, "users")
    assert_unique_key(users, "user_id")

    # time transforms
    orders_t = (
        orders
        .pipe(parse_datetime, col="created_at", utc=True)
        .pipe(add_time_parts, ts_col="created_at")
    )

    log.info(
        "missing created_at after parse: %s",
        int(orders_t["created_at"].isna().sum()),
    )

    # safe join
    joined = safe_left_join(
        orders_t,
        users,
        on="user_id",
        validate="many_to_one",
        suffixes=("", "_user"),
    )

    if len(joined) != len(orders_t):
        raise AssertionError("Row count changed after join")

    log.info("rows after join: %s", len(joined))
    log.info(
        "country match rate: %.3f",
        1.0 - float(joined["country"].isna().mean()),
    )

    # outliers
    joined = joined.assign(
        amount_winsor=winsorize(joined["amount"])
    )

    if add_outlier_flag is not None:
        joined = add_outlier_flag(joined, "amount")

    # write output
    out = p.processed / "analytics_table.parquet"
    out.parent.mkdir(parents=True, exist_ok=True)
    joined.to_parquet(out, index=False)

    log.info("wrote %s", out)


if __name__ == "__main__":
    main()
 
joined = pd.read_parquet("data/processed/analytics_table.parquet")

summary = (
     joined
    .groupby("country", dropna=False)
    .agg(n=("order_id", "size"),
        revenue=("amount", "sum"),
    )
    .reset_index()
    .sort_values("revenue", ascending=False)
)

print(summary)




import pandas as pd
df = pd.read_parquet('data/processed/analytics_table.parquet')
print(df.columns)
print(df.head())