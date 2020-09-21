
select  
    sum(ss_net_profit)/sum(ss_ext_sales_price) as gross_margin
   ,i_category
   ,i_class
   ,grouping(i_category)+grouping(i_class) as lochierarchy
   ,rank() over (
 	partition by grouping(i_category)+grouping(i_class),
 	case when grouping(i_class) = 0 then i_category end 
 	order by sum(ss_net_profit)/sum(ss_ext_sales_price) asc) as rank_within_parent
 from
    store_sales_denorm<SUFFIX>
 where
    d_year = 1999 
 and s_state in ('NE','IN','SD','MN',
                 'TX','MN','MI','LA')
 group by rollup(i_category,i_class)
 order by
   lochierarchy desc
  ,case when lochierarchy = 0 then i_category end
  ,rank_within_parent
  limit 100


