CREATE OR REPLACE DATABASE PUBLIC;
USE DATABASE PUBLIC;
CREATE OR REPLACE SCHEMA PUBLIC;
USE SCHEMA PUBLIC;

create or replace stage PUBLIC.tpcds_3t_parquet
	STORAGE_INTEGRATION = s3_integration
	--url='s3://tpcds-datasets/tmp/nico/tpcds-2.13/tpcds_sf3000_parquet_etl/';
	url='s3://tpcds-datasets/TPC2020/tpcds-2.13/tpcds_sf3000_parquet_32mb/';

create or replace external table store (
    s_store_sk                integer                        AS (VALUE:s_store_sk::INT)               ,
    s_store_id                char(16)              AS (VALUE:s_store_id::VARCHAR),
    s_rec_start_date          date                        AS (VALUE:s_rec_start_date::date),
    s_rec_end_date            date                        AS (VALUE:s_rec_end_date::date),
    s_closed_date_sk          integer                        AS (VALUE:s_closed_date_sk::INT)                       ,
    s_store_name              varchar(50)                   AS (VALUE:s_store_name::VARCHAR),
    s_number_employees        integer                        AS (VALUE:s_number_employees::INT)                       ,
    s_floor_space             integer                        AS (VALUE:s_floor_space::INT)                       ,
    s_hours                   char(20)                      AS (VALUE:s_hours::VARCHAR),
    s_manager                 varchar(40)                   AS (VALUE:s_manager::VARCHAR),
    s_market_id               integer                        AS (VALUE:s_market_id::INT)                       ,
    s_geography_class         varchar(100)                  AS (VALUE:s_geography_class::VARCHAR),
    s_market_desc             varchar(100)                  AS (VALUE:s_market_desc::VARCHAR),
    s_market_manager          varchar(40)                   AS (VALUE:s_market_manager::VARCHAR),
    s_division_id             integer                        AS (VALUE:s_division_id::INT)                       ,
    s_division_name           varchar(50)                   AS (VALUE:s_division_name::VARCHAR),
    s_company_id              integer                        AS (VALUE:s_company_id::INT)                       ,
    s_company_name            varchar(50)                   AS (VALUE:s_company_name::VARCHAR),
    s_street_number           varchar(10)                   AS (VALUE:s_street_number::VARCHAR),
    s_street_name             varchar(60)                   AS (VALUE:s_street_name::VARCHAR),
    s_street_type             char(15)                      AS (VALUE:s_street_type::VARCHAR),
    s_suite_number            char(10)                      AS (VALUE:s_suite_number::VARCHAR),
    s_city                    varchar(60)                   AS (VALUE:s_city::VARCHAR),
    s_county                  varchar(30)                   AS (VALUE:s_county::VARCHAR),
    s_state                   char(2)                       AS (VALUE:s_state::VARCHAR),
    s_zip                     char(10)                      AS (VALUE:s_zip::VARCHAR),
    s_country                 varchar(20)                   AS (VALUE:s_country::VARCHAR),
    s_gmt_offset              double                        AS (VALUE:s_gmt_offset::double)                  ,
    s_tax_precentage          double                        AS (VALUE:s_tax_precentage::double) 
)  with location = @PUBLIC.tpcds_3t_parquet/store
file_format = (type = parquet);

create or replace external table date_dim (
    d_date_sk                 string                        AS (VALUE:d_date_sk::VARCHAR)               ,
    d_date_id                 char(16)              AS (VALUE:d_date_id::VARCHAR),
    d_date                    date                        AS (VALUE:d_date::date),
    d_month_seq               integer                        AS (VALUE:d_month_seq::INT)                       ,
    d_week_seq                integer                        AS (VALUE:d_week_seq::INT)                       ,
    d_quarter_seq             integer                        AS (VALUE:d_quarter_seq::INT)                       ,
    d_year                    integer                        AS (VALUE:d_year::INT)                       ,
    d_dow                     integer                        AS (VALUE:d_dow::INT)                       ,
    d_moy                     integer                        AS (VALUE:d_moy::INT)                       ,
    d_dom                     integer                        AS (VALUE:d_dom::INT)                       ,
    d_qoy                     integer                        AS (VALUE:d_qoy::INT)                       ,
    d_fy_year                 integer                        AS (VALUE:d_fy_year::INT)                       ,
    d_fy_quarter_seq          integer                        AS (VALUE:d_fy_quarter_seq::INT)                       ,
    d_fy_week_seq             integer                        AS (VALUE:d_fy_week_seq::INT)                       ,
    d_day_name                char(9)                       AS (VALUE:d_day_name::VARCHAR),
    d_quarter_name            char(6)                       AS (VALUE:d_quarter_name::VARCHAR),
    d_holiday                 char(1)                       AS (VALUE:d_holiday::VARCHAR),
    d_weekend                 char(1)                       AS (VALUE:d_weekend::VARCHAR),
    d_following_holiday       char(1)                       AS (VALUE:d_following_holiday::VARCHAR),
    d_first_dom               integer                        AS (VALUE:d_first_dom::INT)                       ,
    d_last_dom                integer                        AS (VALUE:d_last_dom::INT)                       ,
    d_same_day_ly             integer                        AS (VALUE:d_same_day_ly::INT)                       ,
    d_same_day_lq             integer                        AS (VALUE:d_same_day_lq::INT)                       ,
    d_current_day             char(1)                       AS (VALUE:d_current_day::VARCHAR),
    d_current_week            char(1)                       AS (VALUE:d_current_week::VARCHAR),
    d_current_month           char(1)                       AS (VALUE:d_current_month::VARCHAR),
    d_current_quarter         char(1)                       AS (VALUE:d_current_quarter::VARCHAR),
    d_current_year            char(1)                       AS (VALUE:d_current_year::VARCHAR)
)  
with location = @PUBLIC.tpcds_3t_parquet/date_dim
file_format = (type = parquet);


create or replace external table store_returns (
    sr_returned_date_sk  string as CASE
  WHEN substring(split_part(metadata$filename, '=', 2),1,7) = '__HIVE_' THEN null
  ELSE substring(split_part(metadata$filename, '=', 2),1,7)
END,
  sr_return_time_sk integer                        AS (VALUE:sr_return_time_sk::INT) ,    
  sr_item_sk integer                        AS (VALUE:sr_item_sk::INT)  ,  
  sr_customer_sk integer                        AS (VALUE:sr_customer_sk::INT) ,       
  sr_cdemo_sk integer                        AS (VALUE:sr_cdemo_sk::INT) ,          
  sr_hdemo_sk integer                        AS (VALUE:sr_hdemo_sk::INT) ,          
  sr_addr_sk integer                        AS (VALUE:sr_addr_sk::INT) ,           
  sr_store_sk integer                        AS (VALUE:sr_store_sk::INT) ,          
  sr_reason_sk integer                        AS (VALUE:sr_reason_sk::INT) ,         
  sr_ticket_number bigint                        AS (VALUE:sr_ticket_number::bigint) ,               
  sr_return_quantity integer                        AS (VALUE:sr_return_quantity::INT) ,   
  sr_return_amt decimal(7,2)                        AS (VALUE:sr_return_amt::decimal(7,2)) ,
  sr_return_tax decimal(7,2)                        AS (VALUE:sr_return_tax::decimal(7,2)) ,
  sr_return_amt_inc_tax decimal(7,2)                        AS (VALUE:sr_return_amt_inc_tax::decimal(7,2)) ,          
  sr_fee decimal(7,2)                        AS (VALUE:sr_fee::decimal(7,2)) ,       
  sr_return_ship_cost decimal(7,2)                        AS (VALUE:sr_return_ship_cost::decimal(7,2)) ,            
  sr_refunded_cash decimal(7,2)                        AS (VALUE:sr_refunded_cash::decimal(7,2)) ,               
  sr_reversed_charge decimal(7,2)                        AS (VALUE:sr_reversed_charge::decimal(7,2)) ,             
  sr_store_credit decimal(7,2)                        AS (VALUE:sr_store_credit::decimal(7,2)) ,                
  sr_net_loss decimal(7,2)                        AS (VALUE:sr_net_loss::decimal(7,2))
)
PARTITION BY (sr_returned_date_sk)
with location = @PUBLIC.tpcds_3t_parquet/store_returns
AUTO_REFRESH = true
FILE_FORMAT = (TYPE = PARQUET);

create or replace external table store_sales (
  ss_sold_date_sk  string as CASE
  WHEN substring(split_part(metadata$filename, '=', 2),1,7) = '__HIVE_' THEN null
  ELSE substring(split_part(metadata$filename, '=', 2),1,7)
END, 
  ss_sold_time_sk integer                        AS (VALUE:ss_sold_time_sk::INT) ,     
  ss_item_sk integer                        AS (VALUE:ss_item_sk::INT)  ,      
  ss_customer_sk integer                        AS (VALUE:ss_customer_sk::INT) ,           
  ss_cdemo_sk integer                        AS (VALUE:ss_cdemo_sk::INT) ,              
  ss_hdemo_sk integer                        AS (VALUE:ss_hdemo_sk::INT) ,         
  ss_addr_sk integer                        AS (VALUE:ss_addr_sk::INT) ,               
  ss_store_sk integer                        AS (VALUE:ss_store_sk::INT) ,           
  ss_promo_sk integer                        AS (VALUE:ss_promo_sk::INT) ,           
  ss_ticket_number bigint                        AS (VALUE:ss_ticket_number::bigint) ,        
  ss_quantity integer                        AS (VALUE:ss_quantity::INT) ,           
  ss_wholesale_cost decimal(7,2)                        AS (VALUE:ss_wholesale_cost::decimal(7,2)) ,          
  ss_list_price decimal(7,2)                        AS (VALUE:ss_list_price::decimal(7,2)) ,              
  ss_sales_price decimal(7,2)                        AS (VALUE:ss_sales_price::decimal(7,2)) ,
  ss_ext_discount_amt decimal(7,2)                        AS (VALUE:ss_ext_discount_amt::decimal(7,2)) ,             
  ss_ext_sales_price decimal(7,2)                        AS (VALUE:ss_ext_sales_price::decimal(7,2)) ,              
  ss_ext_wholesale_cost decimal(7,2)                        AS (VALUE:ss_ext_wholesale_cost::decimal(7,2)) ,           
  ss_ext_list_price decimal(7,2)                        AS (VALUE:ss_ext_list_price::decimal(7,2)) ,               
  ss_ext_tax decimal(7,2)                        AS (VALUE:ss_ext_tax::decimal(7,2)) ,                 
  ss_coupon_amt decimal(7,2)                        AS (VALUE:ss_coupon_amt::decimal(7,2)) , 
  ss_net_paid decimal(7,2)                        AS (VALUE:ss_net_paid::decimal(7,2)) ,   
  ss_net_paid_inc_tax decimal(7,2)                        AS (VALUE:ss_net_paid_inc_tax::decimal(7,2)) ,             
  ss_net_profit decimal(7,2)                        AS (VALUE:ss_net_profit::decimal(7,2))
) PARTITION BY (ss_sold_date_sk)
with location = @PUBLIC.tpcds_3t_parquet/store_sales
AUTO_REFRESH = true
FILE_FORMAT = (TYPE = PARQUET);



create or replace external table item (
i_item_sk integer                        AS (VALUE:i_item_sk::INT) ,                     
  i_item_id char(16)  AS (VALUE:i_item_id::VARCHAR),      
  i_rec_start_date date                        AS (VALUE:i_rec_start_date::date),             
  i_rec_end_date date                        AS (VALUE:i_rec_end_date::date),               
  i_item_desc varchar(200) AS (VALUE:i_item_desc::VARCHAR),         
  i_current_price double                        AS (VALUE:i_current_price::double),      
  i_wholesale_cost double                        AS (VALUE:i_wholesale_cost::double),     
  i_brand_id integer                        AS (VALUE:i_brand_id::INT),                   
  i_brand char(50) AS (VALUE:i_brand::VARCHAR),                 
  i_class_id integer                        AS (VALUE:i_class_id::INT),                   
  i_class char(50) AS (VALUE:i_class::VARCHAR),                 
  i_category_id integer                        AS (VALUE:i_category_id::INT),                
  i_category char(50) AS (VALUE:i_category::VARCHAR),
  i_manufact_id integer                        AS (VALUE:i_manufact_id::INT),                
  i_manufact char(50) AS (VALUE:i_manufact::VARCHAR),
  i_size char(20) AS (VALUE:i_size::VARCHAR),
  i_formulation char(20) AS (VALUE:i_formulation::VARCHAR),
  i_color char(20) AS (VALUE:i_color::VARCHAR),            
  i_units char(10) AS (VALUE:i_units::VARCHAR),             
  i_container char(10) AS (VALUE:i_container::VARCHAR),
  i_manager_id integer                        AS (VALUE:i_manager_id::INT),            
  i_product_name char(50) AS (VALUE:i_product_name::VARCHAR)
)  with location = @PUBLIC.tpcds_3t_parquet/item
AUTO_REFRESH = true
FILE_FORMAT = (TYPE = PARQUET);


create or replace external table web_sales (
    ws_sold_date_sk  string as CASE
  WHEN substring(split_part(metadata$filename, '=', 2),1,7) = '__HIVE_' THEN null
  ELSE substring(split_part(metadata$filename, '=', 2),1,7)
END, 
ws_sold_time_sk integer                        AS (VALUE:ws_sold_time_sk::INT) ,        
  ws_ship_date_sk integer                        AS (VALUE:ws_ship_date_sk::INT) ,        
  ws_item_sk integer                        AS (VALUE:ws_item_sk::INT) ,    
  ws_bill_customer_sk integer                        AS (VALUE:ws_bill_customer_sk::INT) ,    
  ws_bill_cdemo_sk integer                        AS (VALUE:ws_bill_cdemo_sk::INT) ,       
  ws_bill_hdemo_sk integer                        AS (VALUE:ws_bill_hdemo_sk::INT) ,       
  ws_bill_addr_sk integer                        AS (VALUE:ws_bill_addr_sk::INT) ,        
  ws_ship_customer_sk integer                        AS (VALUE:ws_ship_customer_sk::INT) ,    
  ws_ship_cdemo_sk integer                        AS (VALUE:ws_ship_cdemo_sk::INT) ,       
  ws_ship_hdemo_sk integer                        AS (VALUE:ws_ship_hdemo_sk::INT) ,       
  ws_ship_addr_sk integer                        AS (VALUE:ws_ship_addr_sk::INT) ,        
  ws_web_page_sk integer                        AS (VALUE:ws_web_page_sk::INT) ,         
  ws_web_site_sk integer                        AS (VALUE:ws_web_site_sk::INT) ,         
  ws_ship_mode_sk integer                        AS (VALUE:ws_ship_mode_sk::INT) ,        
  ws_warehouse_sk integer                        AS (VALUE:ws_warehouse_sk::INT) ,        
  ws_promo_sk integer                        AS (VALUE:ws_promo_sk::INT) ,            
  ws_order_number bigint                        AS (VALUE:ws_order_number::bigint) ,
  ws_quantity integer                        AS (VALUE:ws_quantity::INT) ,            
  ws_wholesale_cost numeric(7,2)                        AS (VALUE:ws_wholesale_cost::numeric(7,2)) ,                
  ws_list_price numeric(7,2)                        AS (VALUE:ws_list_price::numeric(7,2)) ,  
  ws_sales_price numeric(7,2)                        AS (VALUE:ws_sales_price::numeric(7,2)) , 
  ws_ext_discount_amt numeric(7,2)                        AS (VALUE:ws_ext_discount_amt::numeric(7,2)) ,              
  ws_ext_sales_price numeric(7,2)                        AS (VALUE:ws_ext_sales_price::numeric(7,2)) ,
  ws_ext_wholesale_cost numeric(7,2)                        AS (VALUE:ws_ext_wholesale_cost::numeric(7,2)) ,               
  ws_ext_list_price numeric(7,2)                        AS (VALUE:ws_ext_list_price::numeric(7,2)) , 
  ws_ext_tax numeric(7,2)                        AS (VALUE:ws_ext_tax::numeric(7,2)) ,     
  ws_coupon_amt numeric(7,2)                        AS (VALUE:ws_coupon_amt::numeric(7,2)) ,  
  ws_ext_ship_cost numeric(7,2)                        AS (VALUE:ws_ext_ship_cost::numeric(7,2)) ,                 
  ws_net_paid numeric(7,2)                        AS (VALUE:ws_net_paid::numeric(7,2)) ,    
  ws_net_paid_inc_tax numeric(7,2)                        AS (VALUE:ws_net_paid_inc_tax::numeric(7,2)) ,              
  ws_net_paid_inc_ship numeric(7,2)                        AS (VALUE:ws_net_paid_inc_ship::numeric(7,2)) ,             
  ws_net_paid_inc_ship_tax numeric(7,2)                        AS (VALUE:ws_net_paid_inc_ship_tax::numeric(7,2)) ,         
  ws_net_profit numeric(7,2)                        AS (VALUE:ws_net_profit::numeric(7,2))
)  
PARTITION BY (ws_sold_date_sk)
with location = @tpcds_3t_parquet/web_sales
file_format = (type = parquet)
;

CREATE OR REPLACE EXTERNAL TABLE call_center(
    cc_call_center_id         char(16)           AS (VALUE:cc_call_center_id::VARCHAR),
    cc_call_center_sk             integer                AS (VALUE:cc_call_center_sk::INT),
    cc_rec_start_date         date AS (VALUE:cc_rec_start_date::Date),
    cc_rec_end_date           date AS (VALUE:cc_rec_start_date::date),
    cc_closed_date_sk         integer                        AS (VALUE:cc_closed_date_sk::INT),
    cc_open_date_sk           integer                        AS (VALUE:cc_open_date_sk::INT),
    cc_name                   varchar(50)                    AS (VALUE:cc_name::VARCHAR),
    cc_class                  varchar(50)                    AS (VALUE:cc_class::VARCHAR),
    cc_employees              integer                        AS (VALUE:cc_employees::INT),
    cc_sq_ft                  integer                        AS (VALUE:cc_sq_ft::INT),
    cc_hours                  char(20)                       AS (VALUE:cc_hours::VARCHAR),
    cc_manager                varchar(40)                    AS (VALUE:cc_manager::VARCHAR),
    cc_mkt_id                 integer                        AS (VALUE:cc_mkt_id::INT),
    cc_mkt_class              char(50)                       AS (VALUE:cc_mkt_class::VARCHAR),
    cc_mkt_desc               varchar(100)                   AS (VALUE:cc_mkt_desc::VARCHAR),
    cc_market_manager         varchar(40)                    AS (VALUE:cc_market_manager::VARCHAR),
    cc_division               integer                        AS (VALUE:cc_division::INT),
    cc_division_name          varchar(50)                    AS (VALUE:cc_division_name::VARCHAR),
    cc_company                integer                        AS (VALUE:cc_company::INT),
    cc_company_name           char(50)                       AS (VALUE:cc_company_name::VARCHAR),
    cc_street_number          char(10)                       AS (VALUE:cc_street_number::VARCHAR),
    cc_street_name            varchar(60)                    AS (VALUE:cc_street_name::VARCHAR),
    cc_street_type            char(15)                       AS (VALUE:cc_street_type::VARCHAR),
    cc_suite_number           char(10)                       AS (VALUE:cc_suite_number::VARCHAR),
    cc_city                   varchar(60)                    AS (VALUE:cc_city::VARCHAR),
    cc_county                 varchar(30)                    AS (VALUE:cc_county::VARCHAR),
    cc_state                  char(2)                        AS (VALUE:cc_state::VARCHAR),
    cc_zip                    char(10)                       AS (VALUE:cc_zip::VARCHAR),
    cc_country                varchar(20)                    AS (VALUE:cc_country::VARCHAR),
    cc_gmt_offset             numeric(7,2)                   AS (VALUE:cc_gmt_offset::numeric(7,2)),
    cc_tax_percentage         numeric(7,2) AS (VALUE:cc_tax_percentage::numeric(7,2))
  )
  WITH LOCATION = @tpcds_3t_parquet/call_center
  FILE_FORMAT = (TYPE = PARQUET);

create or replace external table web_site (
    web_site_sk               integer                        AS (VALUE:web_site_sk::INT)               ,
    web_site_id               char(16)              AS (VALUE:web_site_id::VARCHAR),
    web_rec_start_date        date                        AS (VALUE:web_rec_start_date::date),
    web_rec_end_date          date                        AS (VALUE:web_rec_end_date::date),
    web_name                  varchar(50)                   AS (VALUE:web_name::VARCHAR),
    web_open_date_sk          integer                        AS (VALUE:web_open_date_sk::INT)                       ,
    web_close_date_sk         integer                        AS (VALUE:web_close_date_sk::INT)                       ,
    web_class                 varchar(50)                   AS (VALUE:web_class::VARCHAR),
    web_manager               varchar(40)                   AS (VALUE:web_manager::VARCHAR),
    web_mkt_id                integer                        AS (VALUE:web_mkt_id::INT)                       ,
    web_mkt_class             varchar(50)                   AS (VALUE:web_mkt_class::VARCHAR),
    web_mkt_desc              varchar(100)                  AS (VALUE:web_mkt_desc::VARCHAR),
    web_market_manager        varchar(40)                   AS (VALUE:web_market_manager::VARCHAR),
    web_company_id            integer                        AS (VALUE:web_company_id::INT)                       ,
    web_company_name          char(50)                      AS (VALUE:web_company_name::VARCHAR),
    web_street_number         char(10)                      AS (VALUE:web_street_number::VARCHAR),
    web_street_name           varchar(60)                   AS (VALUE:web_street_name::VARCHAR),
    web_street_type           char(15)                      AS (VALUE:web_street_type::VARCHAR),
    web_suite_number          char(10)                      AS (VALUE:web_suite_number::VARCHAR),
    web_city                  varchar(60)                   AS (VALUE:web_city::VARCHAR),
    web_county                varchar(30)                   AS (VALUE:web_county::VARCHAR),
    web_state                 char(2)                       AS (VALUE:web_state::VARCHAR),
    web_zip                   char(10)                      AS (VALUE:web_zip::VARCHAR),
    web_country               varchar(20)                   AS (VALUE:web_country::VARCHAR),
    web_gmt_offset            numeric(7,2)                        AS (VALUE:web_gmt_offset::numeric(7,2))                  ,
    web_tax_percentage        numeric(7,2)                        AS (VALUE:web_tax_percentage::numeric(7,2))
)  with location = @TPCDS_3T_PARQUET/web_site
file_format = (type = parquet);

create or replace external table web_returns (
    wr_returned_date_sk  string as CASE
  WHEN substring(split_part(metadata$filename, '=', 2),1,7) = '__HIVE_' THEN null
  ELSE substring(split_part(metadata$filename, '=', 2),1,7)
END, 
  
  wr_returned_time_sk integer                        AS (VALUE:wr_returned_time_sk::INT) , 
  wr_item_sk integer                        AS (VALUE:wr_item_sk::INT)  , 
  wr_refunded_customer_sk integer                        AS (VALUE:wr_refunded_customer_sk::INT) ,
  wr_refunded_cdemo_sk integer                        AS (VALUE:wr_refunded_cdemo_sk::INT) ,   
  wr_refunded_hdemo_sk integer                        AS (VALUE:wr_refunded_hdemo_sk::INT) ,   
  wr_refunded_addr_sk integer                        AS (VALUE:wr_refunded_addr_sk::INT) ,    
  wr_returning_customer_sk integer                        AS (VALUE:wr_returning_customer_sk::INT) ,
  wr_returning_cdemo_sk integer                        AS (VALUE:wr_returning_cdemo_sk::INT) ,   
  wr_returning_hdemo_sk integer                        AS (VALUE:wr_returning_hdemo_sk::INT) ,  
  wr_returning_addr_sk integer                        AS (VALUE:wr_returning_addr_sk::INT) ,   
  wr_web_page_sk integer                        AS (VALUE:wr_web_page_sk::INT) ,         
  wr_reason_sk integer                        AS (VALUE:wr_reason_sk::INT) ,           
  wr_order_number bigint                        AS (VALUE:wr_order_number::bigint) ,
  wr_return_quantity integer                        AS (VALUE:wr_return_quantity::INT) ,     
  wr_return_amt numeric(7,2)                        AS (VALUE:wr_return_amt::numeric(7,2)) ,  
  wr_return_tax numeric(7,2)                        AS (VALUE:wr_return_tax::numeric(7,2)) ,  
  wr_return_amt_inc_tax numeric(7,2)                        AS (VALUE:wr_return_amt_inc_tax::numeric(7,2)) ,
  wr_fee numeric(7,2)                        AS (VALUE:wr_fee::numeric(7,2)) ,         
  wr_return_ship_cost numeric(7,2)                        AS (VALUE:wr_return_ship_cost::numeric(7,2)) ,
  wr_refunded_cash numeric(7,2)                        AS (VALUE:wr_refunded_cash::numeric(7,2)) ,   
  wr_reversed_charge numeric(7,2)                        AS (VALUE:wr_reversed_charge::numeric(7,2)) ,  
  wr_account_credit numeric(7,2)                        AS (VALUE:wr_account_credit::numeric(7,2)) ,   
  wr_net_loss numeric(7,2)                        AS (VALUE:wr_net_loss::numeric(7,2))
)  
PARTITION BY (wr_returned_date_sk)
with location = @tpcds_3t_parquet/web_returns
file_format = (type = parquet)
;

create or replace external table web_page (
    wp_web_page_sk            integer                        AS (VALUE:wp_web_page_sk::INT)               ,
    wp_web_page_id            char(16)              AS (VALUE:wp_web_page_id::VARCHAR),
    wp_rec_start_date         date                        AS (VALUE:wp_rec_start_date::date),
    wp_rec_end_date           date                        AS (VALUE:wp_rec_end_date::date),
    wp_creation_date_sk       integer                        AS (VALUE:wp_creation_date_sk::INT)                       ,
    wp_access_date_sk         integer                        AS (VALUE:wp_access_date_sk::INT)                       ,
    wp_autogen_flag           char(1)                       AS (VALUE:wp_autogen_flag::VARCHAR),
    wp_customer_sk            integer                        AS (VALUE:wp_customer_sk::INT)                       ,
    wp_url                    varchar(100)                  AS (VALUE:wp_url::VARCHAR),
    wp_type                   char(50)                      AS (VALUE:wp_type::VARCHAR),
    wp_char_count             integer                        AS (VALUE:wp_char_count::INT)                       ,
    wp_link_count             integer                        AS (VALUE:wp_link_count::INT)                       ,
    wp_image_count            integer                        AS (VALUE:wp_image_count::INT)                       ,
    wp_max_ad_count           integer                        AS (VALUE:wp_max_ad_count::INT)
)  with location = @TPCDS_3T_PARQUET/web_page
file_format = (type = parquet);

create or replace external table warehouse (
    w_warehouse_sk            integer                        AS (VALUE:w_warehouse_sk::INT)               ,
    w_warehouse_id            char(16)              AS (VALUE:w_warehouse_id::VARCHAR),
    w_warehouse_name          varchar(20)                   AS (VALUE:w_warehouse_name::VARCHAR),
    w_warehouse_sq_ft         integer                        AS (VALUE:w_warehouse_sq_ft::INT)                       ,
    w_street_number           char(10)                      AS (VALUE:w_street_number::VARCHAR),
    w_street_name             varchar(60)                   AS (VALUE:w_street_name::VARCHAR),
    w_street_type             char(15)                      AS (VALUE:w_street_type::VARCHAR),
    w_suite_number            char(10)                      AS (VALUE:w_suite_number::VARCHAR),
    w_city                    varchar(60)                   AS (VALUE:w_city::VARCHAR),
    w_county                  varchar(30)                   AS (VALUE:w_county::VARCHAR),
    w_state                   char(2)                       AS (VALUE:w_state::VARCHAR),
    w_zip                     char(10)                      AS (VALUE:w_zip::VARCHAR),
    w_country                 varchar(20)                   AS (VALUE:w_country::VARCHAR),
    w_gmt_offset              numeric(7,2)                        AS (VALUE:w_gmt_offset::numeric(7,2))                  
)  with location = @TPCDS_3T_PARQUET/warehouse
file_format = (type = parquet);

create or replace external table time_dim (
    t_time_sk                 integer                        AS (VALUE:t_time_sk::INT)               ,
    t_time_id                 char(16)              AS (VALUE:t_time_id::VARCHAR),
    t_time                    integer                        AS (VALUE:t_time::INT)                       ,
    t_hour                    integer                        AS (VALUE:t_hour::INT)                       ,
    t_minute                  integer                        AS (VALUE:t_minute::INT)                       ,
    t_second                  integer                        AS (VALUE:t_second::INT)                       ,
    t_am_pm                   char(2)                       AS (VALUE:t_am_pm::VARCHAR),
    t_shift                   char(20)                      AS (VALUE:t_shift::VARCHAR),
    t_sub_shift               char(20)                      AS (VALUE:t_sub_shift::VARCHAR),
    t_meal_time               char(20)                      AS (VALUE:t_meal_time::VARCHAR)
)  with location = @TPCDS_3T_PARQUET/time_dim
file_format = (type = parquet);


create or replace external table ship_mode (
    sm_ship_mode_sk           integer                        AS (VALUE:sm_ship_mode_sk::INT)               ,
    sm_ship_mode_id           char(16)              AS (VALUE:sm_ship_mode_id::VARCHAR),
    sm_type                   char(30)                      AS (VALUE:sm_type::VARCHAR),
    sm_code                   char(10)                      AS (VALUE:sm_code::VARCHAR),
    sm_carrier                char(20)                      AS (VALUE:sm_carrier::VARCHAR),
    sm_contract               char(20)                      AS (VALUE:sm_contract::VARCHAR)
)  with location = @TPCDS_3T_PARQUET/ship_mode
file_format = (type = parquet);

create or replace external table reason (
    r_reason_sk               integer                        AS (VALUE:r_reason_sk::INT)               ,
    r_reason_id               char(16)              AS (VALUE:r_reason_id::VARCHAR),
    r_reason_desc             char(100)             AS (VALUE:r_reason_desc::VARCHAR)        
)  with location = @TPCDS_3T_PARQUET/reason
file_format = (type = parquet);

create or replace external table promotion (
    p_promo_sk                integer                        AS (VALUE:p_promo_sk::INT)               ,
    p_promo_id                char(16)              AS (VALUE:p_promo_id::VARCHAR),
    p_start_date_sk           integer                        AS (VALUE:p_start_date_sk::INT)                       ,
    p_end_date_sk             integer                        AS (VALUE:p_end_date_sk::INT)                       ,
    p_item_sk                 integer                        AS (VALUE:p_item_sk::INT)                       ,
    p_cost                    numeric(7,2)                        AS (VALUE:p_cost::numeric(7,2))                 ,
    p_response_target         integer                        AS (VALUE:p_response_target::INT)                       ,
    p_promo_name              char(50)                      AS (VALUE:p_promo_name::VARCHAR),
    p_channel_dmail           char(1)                       AS (VALUE:p_channel_dmail::VARCHAR),
    p_channel_email           char(1)                       AS (VALUE:p_channel_email::VARCHAR),
    p_channel_catalog         char(1)                       AS (VALUE:p_channel_catalog::VARCHAR),
    p_channel_tv              char(1)                       AS (VALUE:p_channel_tv::VARCHAR),
    p_channel_radio           char(1)                       AS (VALUE:p_channel_radio::VARCHAR),
    p_channel_press           char(1)                       AS (VALUE:p_channel_press::VARCHAR),
    p_channel_event           char(1)                       AS (VALUE:p_channel_event::VARCHAR),
    p_channel_demo            char(1)                       AS (VALUE:p_channel_demo::VARCHAR),
    p_channel_details         varchar(100)                  AS (VALUE:p_channel_details::VARCHAR),
    p_purpose                 char(15)                      AS (VALUE:p_purpose::VARCHAR),
    p_discount_active         char(1) AS (VALUE:p_discount_active::VARCHAR)
)  with location = @TPCDS_3T_PARQUET/promotion
file_format = (type = parquet);

create or replace external table inventory (
      inv_date_sk  string as CASE
  WHEN substring(split_part(metadata$filename, '=', 2),1,7) = '__HIVE_' THEN null
  ELSE substring(split_part(metadata$filename, '=', 2),1,7)
END, 
  inv_item_sk integer                        AS (VALUE:inv_item_sk::INT)  ,
  inv_warehouse_sk integer                        AS (VALUE:inv_warehouse_sk::INT)  ,
  inv_quantity_on_hand integer                        AS (VALUE:inv_quantity_on_hand::INT)
)  
PARTITION BY (inv_date_sk)
with location = @tpcds_3t_parquet/inventory
file_format = (type = parquet)
;

create or replace external table income_band (
    ib_income_band_sk         integer                        AS (VALUE:ib_income_band_sk::INT)               ,
    ib_lower_bound            integer                        AS (VALUE:ib_lower_bound::INT)                       ,
    ib_upper_bound            integer                        AS (VALUE:ib_upper_bound::INT)                      
)  with location = @TPCDS_3T_PARQUET/income_band
file_format = (type = parquet);

create or replace external table household_demographics (
	    hd_demo_sk                integer                        AS (VALUE:hd_demo_sk::INT)               ,
    hd_income_band_sk         integer                        AS (VALUE:hd_income_band_sk::INT)                       ,
    hd_buy_potential          char(15)                      AS (VALUE:hd_buy_potential::VARCHAR),
    hd_dep_count              integer                        AS (VALUE:hd_dep_count::INT)                       ,
    hd_vehicle_count          integer                        AS (VALUE:hd_vehicle_count::INT) 
)  with location = @TPCDS_3T_PARQUET/household_demographics
file_format = (type = parquet);

create or replace external table customer_demographics (
  cd_demo_sk integer                        AS (VALUE:cd_demo_sk::INT)  ,   
  cd_gender char(1) AS (VALUE:cd_gender::VARCHAR),          
  cd_marital_status char(1) AS (VALUE:cd_marital_status::VARCHAR),   
  cd_education_status char(20) AS (VALUE:cd_education_status::VARCHAR), 
  cd_purchase_estimate integer                        AS (VALUE:cd_purchase_estimate::INT) ,   
  cd_credit_rating char(10) AS (VALUE:cd_credit_rating::VARCHAR),   
  cd_dep_count integer                        AS (VALUE:cd_dep_count::INT) ,             
  cd_dep_employed_count integer                        AS (VALUE:cd_dep_employed_count::INT) ,    
  cd_dep_college_count integer                        AS (VALUE:cd_dep_college_count::INT)  
)  with location = @TPCDS_3T_PARQUET/customer_demographics
file_format = (type = parquet);

create or replace external table customer_address (
 ca_address_sk integer                        AS (VALUE:ca_address_sk::INT)  ,
  ca_address_id char(16)  AS (VALUE:ca_address_id::VARCHAR),
  ca_street_number char(10) AS (VALUE:ca_street_number::VARCHAR),      
  ca_street_name varchar(60) AS (VALUE:ca_street_name::VARCHAR),   
  ca_street_type char(15) AS (VALUE:ca_street_type::VARCHAR),     
  ca_suite_number char(10) AS (VALUE:ca_suite_number::VARCHAR),    
  ca_city varchar(60) AS (VALUE:ca_city::VARCHAR),
  ca_county varchar(30) AS (VALUE:ca_county::VARCHAR),
  ca_state char(2) AS (VALUE:ca_state::VARCHAR),
  ca_zip char(10) AS (VALUE:ca_zip::VARCHAR),
  ca_country varchar(20) AS (VALUE:ca_country::VARCHAR),
  ca_gmt_offset numeric(7,2)                        AS (VALUE:ca_gmt_offset::numeric(7,2)) ,  
  ca_location_type char(20) AS (VALUE:ca_location_type::VARCHAR)
)  with location = @TPCDS_3T_PARQUET/customer_address
file_format = (type = parquet);

create or replace external table customer (
  c_customer_sk integer                        AS (VALUE:c_customer_sk::INT)  ,                 
  c_customer_id char(16)  AS (VALUE:c_customer_id::VARCHAR),
  c_current_cdemo_sk integer                        AS (VALUE:c_current_cdemo_sk::INT) ,   
  c_current_hdemo_sk integer                        AS (VALUE:c_current_hdemo_sk::INT) ,   
  c_current_addr_sk integer                        AS (VALUE:c_current_addr_sk::INT) ,    
  c_first_shipto_date_sk integer                        AS (VALUE:c_first_shipto_date_sk::INT) ,                 
  c_first_sales_date_sk integer                        AS (VALUE:c_first_sales_date_sk::INT) ,
  c_salutation char(10) AS (VALUE:c_salutation::VARCHAR),
  c_first_name char(20) AS (VALUE:c_first_name::VARCHAR),
  c_last_name char(30) AS (VALUE:c_last_name::VARCHAR),
  c_preferred_cust_flag char(1) AS (VALUE:c_preferred_cust_flag::VARCHAR),               
  c_birth_day integer                        AS (VALUE:c_birth_day::INT) ,          
  c_birth_month integer                        AS (VALUE:c_birth_month::INT) ,        
  c_birth_year integer                        AS (VALUE:c_birth_year::INT) ,         
  c_birth_country varchar(20) AS (VALUE:c_birth_country::VARCHAR),
  c_login char(13) AS (VALUE:c_login::VARCHAR),
  c_email_address char(50) AS (VALUE:c_email_address::VARCHAR),
  c_last_review_date_sk string                        AS (VALUE:c_last_review_date::VARCHAR) 
)  with location = @TPCDS_3T_PARQUET/customer
file_format = (type = parquet);



create or replace external table catalog_sales (
  cs_sold_date_sk  string as CASE WHEN substring(split_part(metadata$filename, '=', 2),1,7) = '__HIVE_' THEN null ELSE substring(split_part(metadata$filename, '=', 2),1,7) END,
  cs_sold_time_sk integer                        AS (VALUE:cs_sold_time_sk::INT) ,        
  cs_ship_date_sk integer                        AS (VALUE:cs_ship_date_sk::INT) ,        
  cs_bill_customer_sk integer                        AS (VALUE:cs_bill_customer_sk::INT) ,    
  cs_bill_cdemo_sk integer                        AS (VALUE:cs_bill_cdemo_sk::INT) ,       
  cs_bill_hdemo_sk integer                        AS (VALUE:cs_bill_hdemo_sk::INT) ,       
  cs_bill_addr_sk integer                        AS (VALUE:cs_bill_addr_sk::INT) ,        
  cs_ship_customer_sk integer                        AS (VALUE:cs_ship_customer_sk::INT) ,    
  cs_ship_cdemo_sk integer                        AS (VALUE:cs_ship_cdemo_sk::INT) ,       
  cs_ship_hdemo_sk integer                        AS (VALUE:cs_ship_hdemo_sk::INT) ,       
  cs_ship_addr_sk integer                        AS (VALUE:cs_ship_addr_sk::INT) ,        
  cs_call_center_sk integer                        AS (VALUE:cs_call_center_sk::INT) ,      
  cs_catalog_page_sk integer                        AS (VALUE:cs_catalog_page_sk::INT) ,     
  cs_ship_mode_sk integer                        AS (VALUE:cs_ship_mode_sk::INT) ,        
  cs_warehouse_sk integer                        AS (VALUE:cs_warehouse_sk::INT) ,        
  cs_item_sk integer                        AS (VALUE:cs_item_sk::INT)  ,    
  cs_promo_sk integer                        AS (VALUE:cs_promo_sk::INT) ,            
  cs_order_number bigint                        AS (VALUE:cs_order_number::bigint)  ,                 
  cs_quantity integer                        AS (VALUE:cs_quantity::INT) ,            
  cs_wholesale_cost numeric(7,2)                        AS (VALUE:cs_wholesale_cost::numeric(7,2)) ,                
  cs_list_price numeric(7,2)                        AS (VALUE:cs_list_price::numeric(7,2)) ,  
  cs_sales_price numeric(7,2)                        AS (VALUE:cs_sales_price::numeric(7,2)) , 
  cs_ext_discount_amt numeric(7,2)                        AS (VALUE:cs_ext_discount_amt::numeric(7,2)) ,              
  cs_ext_sales_price numeric(7,2)                        AS (VALUE:cs_ext_sales_price::numeric(7,2)) ,               
  cs_ext_wholesale_cost numeric(7,2)                        AS (VALUE:cs_ext_wholesale_cost::numeric(7,2)) ,            
  cs_ext_list_price numeric(7,2)                        AS (VALUE:cs_ext_list_price::numeric(7,2)) ,
  cs_ext_tax numeric(7,2)                        AS (VALUE:cs_ext_tax::numeric(7,2)) ,     
  cs_coupon_amt numeric(7,2)                        AS (VALUE:cs_coupon_amt::numeric(7,2)) , 
  cs_ext_ship_cost numeric(7,2)                        AS (VALUE:cs_ext_ship_cost::numeric(7,2)) ,                
  cs_net_paid numeric(7,2)                        AS (VALUE:cs_net_paid::numeric(7,2)) ,   
  cs_net_paid_inc_tax numeric(7,2)                        AS (VALUE:cs_net_paid_inc_tax::numeric(7,2)) ,             
  cs_net_paid_inc_ship numeric(7,2)                        AS (VALUE:cs_net_paid_inc_ship::numeric(7,2)) ,            
  cs_net_paid_inc_ship_tax numeric(7,2)                        AS (VALUE:cs_net_paid_inc_ship_tax::numeric(7,2)) ,        
  cs_net_profit numeric(7,2)                        AS (VALUE:cs_net_profit::numeric(7,2))     
)  PARTITION BY (cs_sold_date_sk)
with location = @tpcds_3t_parquet/catalog_sales
file_format = (type = parquet)
;

create or replace external table catalog_returns (
  cr_returned_date_sk  string as CASE WHEN substring(split_part(metadata$filename, '=', 2),1,7) = '__HIVE_' THEN null ELSE substring(split_part(metadata$filename, '=', 2),1,7) END,
  cr_returned_time_sk integer                        AS (VALUE:cr_returned_time_sk::INT) , 
  cr_item_sk integer                        AS (VALUE:cr_item_sk::INT)  , 
  cr_refunded_customer_sk integer                        AS (VALUE:cr_refunded_customer_sk::INT) ,
  cr_refunded_cdemo_sk integer                        AS (VALUE:cr_refunded_cdemo_sk::INT) ,   
  cr_refunded_hdemo_sk integer                        AS (VALUE:cr_refunded_hdemo_sk::INT) ,   
  cr_refunded_addr_sk integer                        AS (VALUE:cr_refunded_addr_sk::INT) ,    
  cr_returning_customer_sk integer                        AS (VALUE:cr_returning_customer_sk::INT) ,
  cr_returning_cdemo_sk integer                        AS (VALUE:cr_returning_cdemo_sk::INT) ,   
  cr_returning_hdemo_sk integer                        AS (VALUE:cr_returning_hdemo_sk::INT) ,  
  cr_returning_addr_sk integer                        AS (VALUE:cr_returning_addr_sk::INT) ,   
  cr_call_center_sk integer                        AS (VALUE:cr_call_center_sk::INT) ,      
  cr_catalog_page_sk integer                        AS (VALUE:cr_catalog_page_sk::INT) ,     
  cr_ship_mode_sk integer                        AS (VALUE:cr_ship_mode_sk::INT) ,        
  cr_warehouse_sk integer                        AS (VALUE:cr_warehouse_sk::INT) ,        
  cr_reason_sk integer                        AS (VALUE:cr_reason_sk::INT) ,           
  cr_order_number bigint                        AS (VALUE:cr_order_number::bigint) ,
  cr_return_quantity integer                        AS (VALUE:cr_return_quantity::INT) ,     
  cr_return_amount numeric(7,2)                        AS (VALUE:cr_return_amount::numeric(7,2)) ,
  cr_return_tax numeric(7,2)                        AS (VALUE:cr_return_tax::numeric(7,2)) ,   
  cr_return_amt_inc_tax numeric(7,2)                        AS (VALUE:cr_return_amt_inc_tax::numeric(7,2)) ,
  cr_fee numeric(7,2)                        AS (VALUE:cr_fee::numeric(7,2)) ,         
  cr_return_ship_cost numeric(7,2)                        AS (VALUE:cr_return_ship_cost::numeric(7,2)) , 
  cr_refunded_cash numeric(7,2)                        AS (VALUE:cr_refunded_cash::numeric(7,2)) ,    
  cr_reversed_charge numeric(7,2)                        AS (VALUE:cr_reversed_charge::numeric(7,2)) ,  
  cr_store_credit numeric(7,2)                        AS (VALUE:cr_store_credit::numeric(7,2)) ,
  cr_net_loss numeric(7,2)                        AS (VALUE:cr_net_loss::numeric(7,2))
)  PARTITION BY (cr_returned_date_sk)
with location = @tpcds_3t_parquet/catalog_returns
file_format = (type = parquet)
;


create or replace external table catalog_page (
    cp_catalog_page_sk        integer                        AS (VALUE:cp_catalog_page_sk::INT)               ,
    cp_catalog_page_id        char(16)              AS (VALUE:cp_catalog_page_id::VARCHAR),
    cp_start_date_sk          integer                        AS (VALUE:cp_start_date_sk::INT)                       ,
    cp_end_date_sk            integer                        AS (VALUE:cp_end_date_sk::INT)                       ,
    cp_department             varchar(50)                   AS (VALUE:cp_department::VARCHAR),
    cp_catalog_number         integer                        AS (VALUE:cp_catalog_number::INT)                       ,
    cp_catalog_page_number    integer                        AS (VALUE:cp_catalog_page_number::INT)                       ,
    cp_description            varchar(100)                  AS (VALUE:cp_description::VARCHAR),
    cp_type                   varchar(100) AS (VALUE:cp_type::VARCHAR)
)  with location = @TPCDS_3T_PARQUET/catalog_page
file_format = (type = parquet);

create or replace external table call_center (
    cc_call_center_sk         integer                        AS (VALUE:cc_call_center_sk::INT)               ,
    cc_call_center_id         char(16)              AS (VALUE:cc_call_center_id::VARCHAR),
    cc_rec_start_date         date                        AS (VALUE:cc_rec_start_date::date),
    cc_rec_end_date           date                        AS (VALUE:cc_rec_end_date::date),
    cc_closed_date_sk         integer                        AS (VALUE:cc_closed_date_sk::INT)                       ,
    cc_open_date_sk           integer                        AS (VALUE:cc_open_date_sk::INT)                       ,
    cc_name                   varchar(50)                   AS (VALUE:cc_name::VARCHAR),
    cc_class                  varchar(50)                   AS (VALUE:cc_class::VARCHAR),
    cc_employees              integer                        AS (VALUE:cc_employees::INT)                       ,
    cc_sq_ft                  integer                        AS (VALUE:cc_sq_ft::INT)                       ,
    cc_hours                  char(20)                      AS (VALUE:cc_hours::VARCHAR),
    cc_manager                varchar(40)                   AS (VALUE:cc_manager::VARCHAR),
    cc_mkt_id                 integer                        AS (VALUE:cc_mkt_id::INT)                       ,
    cc_mkt_class              char(50)                      AS (VALUE:cc_mkt_class::VARCHAR),
    cc_mkt_desc               varchar(100)                  AS (VALUE:cc_mkt_desc::VARCHAR),
    cc_market_manager         varchar(40)                   AS (VALUE:cc_market_manager::VARCHAR),
    cc_division               integer                        AS (VALUE:cc_division::INT)                       ,
    cc_division_name          varchar(50)                   AS (VALUE:cc_division_name::VARCHAR),
    cc_company                integer                        AS (VALUE:cc_company::INT)                       ,
    cc_company_name           char(50)                      AS (VALUE:cc_company_name::VARCHAR),
    cc_street_number          char(10)                      AS (VALUE:cc_street_number::VARCHAR),
    cc_street_name            varchar(60)                   AS (VALUE:cc_street_name::VARCHAR),
    cc_street_type            char(15)                      AS (VALUE:cc_street_type::VARCHAR),
    cc_suite_number           char(10)                      AS (VALUE:cc_suite_number::VARCHAR),
    cc_city                   varchar(60)                   AS (VALUE:cc_city::VARCHAR),
    cc_county                 varchar(30)                   AS (VALUE:cc_county::VARCHAR),
    cc_state                  char(2)                       AS (VALUE:cc_state::VARCHAR),
    cc_zip                    char(10)                      AS (VALUE:cc_zip::VARCHAR),
    cc_country                varchar(20)                   AS (VALUE:cc_country::VARCHAR),
    cc_gmt_offset             numeric(7,2)                        AS (VALUE:cc_gmt_offset::numeric(7,2))                  ,
    cc_tax_percentage         numeric(7,2)                        AS (VALUE:cc_tax_percentage::numeric(7,2))
)  with location = @TPCDS_3T_PARQUET/call_center
file_format = (type = parquet);

create or replace external table household_demographics (
    hd_demo_sk                integer                        AS (VALUE:hd_demo_sk::INT)               ,
    hd_income_band_sk         integer                        AS (VALUE:hd_income_band_sk::INT)                       ,
    hd_buy_potential          char(15)                      AS (VALUE:hd_buy_potential::VARCHAR),
    hd_dep_count              integer                        AS (VALUE:hd_dep_count::INT)                       ,
    hd_vehicle_count          integer                        AS (VALUE:hd_vehicle_count::INT)
)  with location = @TPCDS_3T_PARQUET/household_demographics
file_format = (type = parquet);
