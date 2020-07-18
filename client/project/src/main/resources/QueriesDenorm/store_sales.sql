select * from
store_sales, date_dim, time_dim, customer, customer_demographics, household_demographics, customer_address, store, promotion, item
where
ss_sold_date_sk = d_date_sk and
ss_store_sk = s_store_sk and
ss_sold_time_sk = t_time_sk and
ss_item_sk = i_item_sk and
ss_customer_sk = c_customer_sk and
ss_cdemo_sk = cd_demo_sk and
ss_hdemo_sk = hd_demo_sk and
ss_addr_sk = ca_address_sk and
ss_promo_sk = p_promo_sk
