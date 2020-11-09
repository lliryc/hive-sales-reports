select product_id, cnt from (select product_id, count(*) as cnt from sales group by product_id) as st order by cnt desc limit 1;

