-- Warehouse schema (PostgreSQL)
CREATE TABLE IF NOT EXISTS dim_store (
  store_id TEXT PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS dim_item (
  sku TEXT PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS fact_sales (
  sale_date DATE,
  store_id TEXT REFERENCES dim_store(store_id),
  sku TEXT REFERENCES dim_item(sku),
  quantity INT,
  unit_price NUMERIC(10,2),
  total_amount NUMERIC(12,2)
);

CREATE TABLE IF NOT EXISTS fact_inventory (
  store_id TEXT REFERENCES dim_store(store_id),
  sku TEXT REFERENCES dim_item(sku),
  on_hand INT,
  reorder_point INT
);
