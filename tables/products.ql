create table products
  stored as parquet
  tblproperties('parquet.compression'='SNAPPY')
  AS SELECT CAST(product_id AS bigint) as product_id , product_name, CAST(price AS int) as price FROM products_ext;