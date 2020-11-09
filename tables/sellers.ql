create table sellers
  stored as parquet
  tblproperties('parquet.compression'='SNAPPY')
  AS SELECT CAST(seller_id AS bigint) as seller_id , seller_name, CAST(daily_target AS int) as daily_target FROM sellers_ext;