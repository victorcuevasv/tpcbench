
select  i_item_id,
        s_state, grouping(s_state) g_state,
        avg(ss_quantity) agg1,
        avg(ss_list_price) agg2,
        avg(ss_coupon_amt) agg3,
        avg(ss_sales_price) agg4
 from store_sales_denorm<SUFFIX>
 where cd_gender = 'F' and
       cd_marital_status = 'M' and
       cd_education_status = '4 yr Degree' and
       d_year = 2002 and
       s_state in ('NE','IN', 'SD', 'MN', 'TX', 'MN')
 group by rollup (i_item_id, s_state)
 order by i_item_id
         ,s_state
 limit 100


