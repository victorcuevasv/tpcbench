
select  s.ca_state state, count(*) cnt
 from store_sales_denorm<SUFFIX> s
 where       s.d_month_seq = 
 	     (select distinct (d_month_seq)
 	      from store_sales_denorm<SUFFIX>
               where d_year = 2000
 	        and d_moy = 2 )
 	and s.i_current_price > 1.2 * 
             (select avg(j.i_current_price) 
 	     from store_sales_denorm<SUFFIX> j 
 	     where j.i_category = s.i_category)
 group by s.ca_state
 having count(*) >= 10
 order by cnt, s.ca_state 
 limit 100


