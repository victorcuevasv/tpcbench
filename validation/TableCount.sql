with table_count as(
(select 'call_center' as table_name, count(*) as tuple_count from call_center) union
(select 'catalog_page' as table_name, count(*) as tuple_count from catalog_page) union
(select 'catalog_returns' as table_name, count(*) as tuple_count from catalog_returns) union
(select 'catalog_sales' as table_name, count(*) as tuple_count from catalog_sales) union
(select 'customer' as table_name, count(*) as tuple_count from customer) union
(select 'customer_address' as table_name, count(*) as tuple_count from customer_address) union
(select 'customer_demographics' as table_name, count(*) as tuple_count from customer_demographics) union
(select 'date_dim' as table_name, count(*) as tuple_count from date_dim) union
(select 'household_demographics' as table_name, count(*) as tuple_count from household_demographics) union
(select 'income_band' as table_name, count(*) as tuple_count from income_band) union
(select 'inventory' as table_name, count(*) as tuple_count from inventory) union
(select 'item' as table_name, count(*) as tuple_count from item) union
(select 'promotion' as table_name, count(*) as tuple_count from promotion) union
(select 'reason' as table_name, count(*) as tuple_count from reason) union
(select 'ship_mode' as table_name, count(*) as tuple_count from ship_mode) union
(select 'store' as table_name, count(*) as tuple_count from store) union
(select 'store_returns' as table_name, count(*) as tuple_count from store_returns) union
(select 'store_sales' as table_name, count(*) as tuple_count from store_sales) union
(select 'time_dim' as table_name, count(*) as tuple_count from time_dim) union
(select 'warehouse' as table_name, count(*) as tuple_count from warehouse) union
(select 'web_page' as table_name, count(*) as tuple_count from web_page) union
(select 'web_returns' as table_name, count(*) as tuple_count from web_returns) union
(select 'web_sales' as table_name, count(*) as tuple_count from web_sales) union
(select 'web_site' as table_name, count(*) as tuple_count from web_site))
select * from table_count
order by table_name;


