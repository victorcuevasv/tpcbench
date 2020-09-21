
select  i_brand_id brand_id, i_brand brand, i_manufact_id, i_manufact,
 	sum(ss_ext_sales_price) ext_price
 from store_sales_denorm<SUFFIX>
 where i_manager_id=7
   and d_moy=11
   and d_year=1999
   and substr(ca_zip,1,5) <> substr(s_zip,1,5) 
 group by i_brand
      ,i_brand_id
      ,i_manufact_id
      ,i_manufact
 order by ext_price desc
         ,i_brand
         ,i_brand_id
         ,i_manufact_id
         ,i_manufact
limit 100 


