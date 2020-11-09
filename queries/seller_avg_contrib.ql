
select seller_id, avg(s.num_pieces_sold / sl.daily_target) as avg_contrib from sales as s \
inner join sellers as sl on sl.seller_id=s.seller_id \
group by s.seller_id order by seller_id;