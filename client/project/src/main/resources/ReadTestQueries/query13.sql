
select avg(ss_quantity)
       ,avg(ss_ext_sales_price)
       ,avg(ss_ext_wholesale_cost)
       ,sum(ss_ext_wholesale_cost)
 from store_sales_denorm<SUFFIX>
 where d_year = 2001
 and((cd_marital_status = 'D'
  and cd_education_status = '2 yr Degree'
  and ss_sales_price between 100.00 and 150.00
  and hd_dep_count = 3   
     )or
     (cd_marital_status = 'S'
  and cd_education_status = 'Secondary'
  and ss_sales_price between 50.00 and 100.00   
  and hd_dep_count = 1
     ) or 
     (cd_marital_status = 'W'
  and cd_education_status = 'Advanced Degree'
  and ss_sales_price between 150.00 and 200.00 
  and hd_dep_count = 1  
     ))
 and((ca_country = 'United States'
  and ca_state in ('CO', 'IL', 'MN')
  and ss_net_profit between 100 and 200  
     ) or
     (ca_country = 'United States'
  and ca_state in ('OH', 'MT', 'NM')
  and ss_net_profit between 150 and 300  
     ) or
     (ca_country = 'United States'
  and ca_state in ('TX', 'MO', 'MI')
  and ss_net_profit between 50 and 250  
     ))



