set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
create table sales (order_id bigint, product_id bigint, `date` date, num_pieces_sold bigint, bill_raw_text string)
  partitioned by (seller_id bigint)
  clustered by (product_id) sorted by (`date`) into 5 buckets
  stored as parquet
  tblproperties('parquet.compression'='SNAPPY');

insert into  table sales
partition (seller_id)
select cast(order_id as bigint) as order_id, cast(product_id as bigint) as product_id, cast(`date` as date) as `date`,
        cast(num_pieces_sold as bigint) as num_pieces_sold, bill_raw_text, cast(seller_id as bigint) as seller_id from sales_ext;