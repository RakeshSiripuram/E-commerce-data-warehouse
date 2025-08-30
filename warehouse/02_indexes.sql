-- Helpful indexes for queries
CREATE INDEX IF NOT EXISTS ix_sales_date ON fact_sales (sale_date);
CREATE INDEX IF NOT EXISTS ix_sales_store ON fact_sales (store_id);
CREATE INDEX IF NOT EXISTS ix_sales_sku ON fact_sales (sku);
CREATE INDEX IF NOT EXISTS ix_inventory_store ON fact_inventory (store_id);
CREATE INDEX IF NOT EXISTS ix_inventory_sku ON fact_inventory (sku);
