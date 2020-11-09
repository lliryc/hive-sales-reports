create external table sales (order_id string, product_id string, seller_id string, `date` string, num_pieces_sold string, bill_raw_text string)
  stored as parquet
  location '/data/products/sales_parquet/'
  tblproperties('parquet.compression'='SNAPPY');
  