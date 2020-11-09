create external table products (product_id string, product_name string, price string)
  stored as parquet
  location '/data/products/products_parquet/'
  tblproperties('parquet.compression'='SNAPPY');

