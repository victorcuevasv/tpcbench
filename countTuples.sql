with table_sizes as(
select 'call_center' as name, count(*) as count from call_center union
select 'catalog_page' as name, count(*) as count from catalog_page union
select 'catalog_returns' as name, count(*) as count from catalog_returns union
select 'catalog_sales' as name, count(*) as count from catalog_sales union
select 'customer' as name, count(*) as count from customer union
select 'customer_address' as name, count(*) as count from customer_address union
select 'customer_demographics' as name, count(*) as count from customer_demographics union
select 'date_dim' as name, count(*) as count from date_dim union
select 'household_demographics' as name, count(*) as count from household_demographics union
select 'income_band' as name, count(*) as count from income_band union
select 'inventory' as name, count(*) as count from inventory union
select 'item' as name, count(*) as count from item union
select 'promotion' as name, count(*) as count from promotion union
select 'reason' as name, count(*) as count from reason union
select 'ship_mode' as name, count(*) as count from ship_mode union
select 'store' as name, count(*) as count from store union
select 'store_returns' as name, count(*) as count from store_returns union
select 'store_sales' as name, count(*) as count from store_sales union
select 'time_dim' as name, count(*) as count from time_dim union
select 'warehouse' as name, count(*) as count from warehouse union
select 'web_page' as name, count(*) as count from web_page union
select 'web_returns' as name, count(*) as count from web_returns union
select 'web_sales' as name, count(*) as count from web_sales union
select 'web_site' as name, count(*) as count from web_site)
select * from table_sizes order by name
