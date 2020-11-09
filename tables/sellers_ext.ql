create external table sellers_ext (seller_id string, seller_name string, daily_target string)
  stored as parquet
  location '/data/products/sellers_parquet/'
  tblproperties('parquet.compression'='SNAPPY');

