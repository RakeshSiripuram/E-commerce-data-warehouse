import os
import pandas as pd

def extract_sales(path: str) -> pd.DataFrame:
    return pd.read_csv(path)

def extract_inventory(path: str) -> pd.DataFrame:
    return pd.read_csv(path)

if __name__ == "__main__":
    sales = extract_sales(os.getenv("SALES_PATH", "data/sales.csv"))
    inv = extract_inventory(os.getenv("INVENTORY_PATH", "data/inventory.csv"))
    print(sales.head(), inv.head(), sep="\n\n")
