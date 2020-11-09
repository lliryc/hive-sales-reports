set hive.optimize.skewjoin = true;
set hive.skewjoin.key = 300000;
set hive.skewjoin.mapjoin.map.tasks=10000;
set hive.skewjoin.mapjoin.min.split=33554432;

with sellers_rating as
(select sl.seller_id, s.product_id, sm, dense_rank() over(partition by s.product_id order by s.sm asc) as r_asc,
dense_rank() over(partition by s.product_id order by s.sm desc) as r_desc
from (select seller_id, product_id, sum(num_pieces_sold) as sm from sales group by seller_id, product_id) as s
inner join sellers as sl on s.seller_id=sl.seller_id)
select seller_id, product_id, sm, r_asc, r_desc from sellers_rating where r_asc=2 or r_desc=2 and product_id=0
union
select seller_id, product_id, sm, r_asc, r_desc from sellers_rating where r_asc=1 and r_desc=1
order by sm desc;