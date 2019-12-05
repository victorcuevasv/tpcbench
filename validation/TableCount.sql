(select 'call_center', count(*) from call_center) union
(select 'catalog_page', count(*) from catalog_page) union
(select 'catalog_returns', count(*) from catalog_returns) union
(select 'catalog_sales', count(*) from catalog_sales) union
(select 'customer', count(*) from customer) union
(select 'customer_address', count(*) from customer_address) union
(select 'customer_demographics', count(*) from customer_demographics) union
(select 'date_dim', count(*) from date_dim) union
(select 'household_demographics', count(*) from household_demographics) union
(select 'income_band', count(*) from income_band) union
(select 'inventory', count(*) from inventory) union
(select 'item', count(*) from item) union
(select 'promotion', count(*) from promotion) union
(select 'reason', count(*) from reason) union
(select 'ship_mode', count(*) from ship_mode) union
(select 'store', count(*) from store) union
(select 'store_returns', count(*) from store_returns) union
(select 'store_sales', count(*) from store_sales) union
(select 'time_dim', count(*) from time_dim) union
(select 'warehouse', count(*) from warehouse) union
(select 'web_page', count(*) from web_page) union
(select 'web_returns', count(*) from web_returns) union
(select 'web_sales', count(*) from web_sales) union
(select 'web_site', count(*) from web_site)

