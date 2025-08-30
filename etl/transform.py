import pandas as pd

def clean_sales(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [c.strip().lower() for c in df.columns]
    df["sale_date"] = pd.to_datetime(df["sale_date"])
    df["total_amount"] = df["quantity"] * df["unit_price"]
    return df

def clean_inventory(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [c.strip().lower() for c in df.columns]
    return df

def aggregate_sales_daily(df):
    out = (
        df.groupby(["sale_date", "store_id"], as_index=False)
          .agg(
              total_qty=("quantity", "sum"),
              total_revenue=("total_amount", "sum"),
              avg_unit_price=("unit_price", "mean"),
              distinct_items=("sku", "nunique"),
          )
    )
    return out
def aggregate_sales_daily(df):
    out = (
        df.groupby(["sale_date", "store_id"], as_index=False)
          .agg(
              total_qty=("quantity", "sum"),
              total_revenue=("total_amount", "sum"),
              avg_unit_price=("unit_price", "mean"),
              distinct_items=("sku", "nunique"),
          )
    )
    return out

def validate_sales(df):
    # no negative quantities or prices; required keys must not be null
    if df["quantity"].lt(0).any():
        raise ValueError("Quantity contains negative values.")
    if df["unit_price"].lt(0).any():
        raise ValueError("Unit price contains negative values.")
    if df["sale_date"].isna().any() or df["store_id"].isna().any() or df["sku"].isna().any():
        raise ValueError("Key columns contain nulls.")
    return True