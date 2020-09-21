
select  d_year 
       ,i_brand_id brand_id 
       ,i_brand brand
       ,sum(ss_ext_sales_price) sum_agg
 from  store_sales_denorm<SUFFIX>
 where i_manufact_id = 436
   and d_moy=12
 group by d_year
      ,i_brand
      ,i_brand_id
 order by d_year
         ,sum_agg desc
         ,brand_id
 limit 100


