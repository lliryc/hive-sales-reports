set hive.optimize.skewjoin = true;
set hive.skewjoin.key = 300000;
set hive.skewjoin.mapjoin.map.tasks=10000;
set hive.skewjoin.mapjoin.min.split=33554432;

select avg(p.price * s.num_pieces_sold) as avg_order_revenue from sales as s inner join products as p on s.product_id = p.product_id;
