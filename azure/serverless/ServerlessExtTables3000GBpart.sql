--Execute as azureuser (must run only the create schema statement below)
CREATE SCHEMA tpcds_sf3000_delta_part;

--Execute as azureuser
ALTER USER tpcds_user WITH DEFAULT_SCHEMA = tpcds_sf3000_delta_part;
GRANT CONTROL ON SCHEMA :: tpcds_sf3000_delta_part TO tpcds_user;
GRANT ALTER ON SCHEMA :: tpcds_sf3000_delta_part TO tpcds_user;

--Execute as azureuser
CREATE EXTERNAL DATA SOURCE performance_datasets_tpcds_sf3000_delta_part WITH (
    LOCATION = 'https://bsctpcdsnew.blob.core.windows.net/performance-datasets/tpc/tpcds-2.13/tpcds_sf3000_delta/',   
    CREDENTIAL = tpcds_credential
);

--Execute as azureuser
CREATE EXTERNAL FILE FORMAT tpcds_datasets_parquet1gb_ff
WITH
(  
    FORMAT_TYPE = PARQUET,
    DATA_COMPRESSION = 'org.apache.hadoop.io.compress.SnappyCodec'
)


--Execute as tpcds_user 

CREATE VIEW catalog_returns
AS SELECT *, 
CASE WHEN catalog_returns_rs.filepath(1) = '__HIVE_DEFAULT_PARTITION__' THEN null
	ELSE CAST(catalog_returns_rs.filepath(1) AS int) END AS [cr_returned_date_sk]
FROM
    OPENROWSET(
        BULK 'catalog_returns/cr_returned_date_sk=*/*.parquet',
        DATA_SOURCE = 'performance_datasets_tpcds_sf3000_delta_part',
        FORMAT='PARQUET'
    ) AS catalog_returns_rs;


CREATE VIEW web_returns
AS SELECT *, 
CASE WHEN web_returns_rs.filepath(1) = '__HIVE_DEFAULT_PARTITION__' THEN null
    ELSE CAST(web_returns_rs.filepath(1) AS int) END AS [wr_returned_date_sk]
FROM
    OPENROWSET(
        BULK 'web_returns/wr_returned_date_sk=*/*.parquet',
        DATA_SOURCE = 'performance_datasets_tpcds_sf3000_delta_part',
        FORMAT='PARQUET'
    ) AS web_returns_rs;


CREATE VIEW web_sales
AS SELECT *, CASE WHEN web_sales_rs.filepath(1) = '__HIVE_DEFAULT_PARTITION__' THEN null
    ELSE CAST(web_sales_rs.filepath(1) AS int) END AS [ws_sold_date_sk]
FROM
    OPENROWSET(
        BULK 'web_sales/ws_sold_date_sk=*/*.parquet',
        DATA_SOURCE = 'performance_datasets_tpcds_sf3000_delta_part',
        FORMAT='PARQUET'
    ) AS web_sales_rs;


CREATE VIEW catalog_sales
AS SELECT *, CASE WHEN catalog_sales_rs.filepath(1) = '__HIVE_DEFAULT_PARTITION__' THEN null
    ELSE CAST(catalog_sales_rs.filepath(1) AS int) END AS [cs_sold_date_sk]
FROM
    OPENROWSET(
        BULK 'catalog_sales/cs_sold_date_sk=*/*.parquet',
        DATA_SOURCE = 'performance_datasets_tpcds_sf3000_delta_part',
        FORMAT='PARQUET'
    ) AS catalog_sales_rs;


CREATE VIEW store_sales
AS SELECT *, 
    CASE WHEN store_sales_rs.filepath(1) = '__HIVE_DEFAULT_PARTITION__' THEN null 
        ELSE CAST(store_sales_rs.filepath(1) AS int) END AS [ss_sold_date_sk]
FROM
    OPENROWSET(
        BULK 'store_sales/ss_sold_date_sk=*/*.parquet',
        DATA_SOURCE = 'performance_datasets_tpcds_sf3000_delta_part',
        FORMAT='PARQUET'
    ) AS store_sales_rs;


CREATE VIEW inventory
AS SELECT *, CASE WHEN inventory_rs.filepath(1) = '__HIVE_DEFAULT_PARTITION__' THEN null
    ELSE CAST(inventory_rs.filepath(1) AS int) END AS [inv_date_sk]
FROM
    OPENROWSET(
        BULK 'inventory/inv_date_sk=*/*.parquet',
        DATA_SOURCE = 'performance_datasets_tpcds_sf3000_delta_part',
        FORMAT='PARQUET'
    ) AS inventory_rs;
    

CREATE VIEW store_returns
AS SELECT *, CASE WHEN store_returns_rs.filepath(1) = '__HIVE_DEFAULT_PARTITION__' THEN null
    ELSE CAST(store_returns_rs.filepath(1) AS int) END AS [sr_returned_date_sk]
FROM
    OPENROWSET(
        BULK 'store_returns/sr_returned_date_sk=*/*.parquet',
        DATA_SOURCE = 'performance_datasets_tpcds_sf3000_delta_part',
        FORMAT='PARQUET'
    ) AS store_returns_rs;

    
create external table customer_address
(
    ca_address_sk             integer               ,
    ca_address_id             varchar(16)              ,
    ca_street_number          varchar(10)                      ,
    ca_street_name            varchar(60)                   ,
    ca_street_type            varchar(15)                      ,
    ca_suite_number           varchar(10)                      ,
    ca_city                   varchar(60)                   ,
    ca_county                 varchar(30)                   ,
    ca_state                  varchar(2)                       ,
    ca_zip                    varchar(10)                      ,
    ca_country                varchar(20)                   ,
    ca_gmt_offset             decimal(5,2)                  ,
    ca_location_type          varchar(20)                      
)
WITH (
    LOCATION = '/customer_address/',
    DATA_SOURCE = performance_datasets_tpcds_sf3000_delta_part,  
    FILE_FORMAT = tpcds_datasets_parquet1gb_ff
);

create external table customer_demographics
(
    cd_demo_sk                integer               ,
    cd_gender                 varchar(1)                       ,
    cd_marital_status         varchar(1)                       ,
    cd_education_status       varchar(20)                      ,
    cd_purchase_estimate      integer                       ,
    cd_credit_rating          varchar(10)                      ,
    cd_dep_count              integer                       ,
    cd_dep_employed_count     integer                       ,
    cd_dep_college_count      integer                       
)
WITH (
    LOCATION = '/customer_demographics/',
    DATA_SOURCE = performance_datasets_tpcds_sf3000_delta_part,  
    FILE_FORMAT = tpcds_datasets_parquet1gb_ff
);

create external table date_dim
(
    d_date_sk                 integer               ,
    d_date_id                 varchar(16)              ,
    d_date                    date                          ,
    d_month_seq               integer                       ,
    d_week_seq                integer                       ,
    d_quarter_seq             integer                       ,
    d_year                    integer                       ,
    d_dow                     integer                       ,
    d_moy                     integer                       ,
    d_dom                     integer                       ,
    d_qoy                     integer                       ,
    d_fy_year                 integer                       ,
    d_fy_quarter_seq          integer                       ,
    d_fy_week_seq             integer                       ,
    d_day_name                varchar(9)                       ,
    d_quarter_name            varchar(6)                       ,
    d_holiday                 varchar(1)                       ,
    d_weekend                 varchar(1)                       ,
    d_following_holiday       varchar(1)                       ,
    d_first_dom               integer                       ,
    d_last_dom                integer                       ,
    d_same_day_ly             integer                       ,
    d_same_day_lq             integer                       ,
    d_current_day             varchar(1)                       ,
    d_current_week            varchar(1)                       ,
    d_current_month           varchar(1)                       ,
    d_current_quarter         varchar(1)                       ,
    d_current_year            varchar(1)                       
)
WITH (
    LOCATION = '/date_dim/',
    DATA_SOURCE = performance_datasets_tpcds_sf3000_delta_part,  
    FILE_FORMAT = tpcds_datasets_parquet1gb_ff
);

create external table warehouse
(
    w_warehouse_sk            integer               ,
    w_warehouse_id            varchar(16)              ,
    w_warehouse_name          varchar(20)                   ,
    w_warehouse_sq_ft         integer                       ,
    w_street_number           varchar(10)                      ,
    w_street_name             varchar(60)                   ,
    w_street_type             varchar(15)                      ,
    w_suite_number            varchar(10)                      ,
    w_city                    varchar(60)                   ,
    w_county                  varchar(30)                   ,
    w_state                   varchar(2)                       ,
    w_zip                     varchar(10)                      ,
    w_country                 varchar(20)                   ,
    w_gmt_offset              decimal(5,2)                  
)
WITH (
    LOCATION = '/warehouse/',
    DATA_SOURCE = performance_datasets_tpcds_sf3000_delta_part,  
    FILE_FORMAT = tpcds_datasets_parquet1gb_ff
);

create external table ship_mode
(
    sm_ship_mode_sk           integer               ,
    sm_ship_mode_id           varchar(16)              ,
    sm_type                   varchar(30)                      ,
    sm_code                   varchar(10)                      ,
    sm_carrier                varchar(20)                      ,
    sm_contract               varchar(20)                      
)
WITH (
    LOCATION = '/ship_mode/',
    DATA_SOURCE = performance_datasets_tpcds_sf3000_delta_part,  
    FILE_FORMAT = tpcds_datasets_parquet1gb_ff
);

create external table time_dim
(
    t_time_sk                 integer               ,
    t_time_id                 varchar(16)              ,
    t_time                    integer                       ,
    t_hour                    integer                       ,
    t_minute                  integer                       ,
    t_second                  integer                       ,
    t_am_pm                   varchar(2)                       ,
    t_shift                   varchar(20)                      ,
    t_sub_shift               varchar(20)                      ,
    t_meal_time               varchar(20)                      
)
WITH (
    LOCATION = '/time_dim/',
    DATA_SOURCE = performance_datasets_tpcds_sf3000_delta_part,  
    FILE_FORMAT = tpcds_datasets_parquet1gb_ff
);

create external table reason
(
    r_reason_sk               integer               ,
    r_reason_id               varchar(16)              ,
    r_reason_desc             varchar(100)
)
WITH (
    LOCATION = '/reason/',
    DATA_SOURCE = performance_datasets_tpcds_sf3000_delta_part,  
    FILE_FORMAT = tpcds_datasets_parquet1gb_ff
);

create external table income_band
(
    ib_income_band_sk         integer               ,
    ib_lower_bound            integer                       ,
    ib_upper_bound            integer                       
)
WITH (
    LOCATION = '/income_band/',
    DATA_SOURCE = performance_datasets_tpcds_sf3000_delta_part,  
    FILE_FORMAT = tpcds_datasets_parquet1gb_ff
);

create external table item
(
    i_item_sk                 integer               ,
    i_item_id                 varchar(16)              ,
    i_rec_start_date          date                          ,
    i_rec_end_date            date                          ,
    i_item_desc               varchar(200)                  ,
    i_current_price           decimal(7,2)                  ,
    i_wholesale_cost          decimal(7,2)                  ,
    i_brand_id                integer                       ,
    i_brand                   varchar(50)                      ,
    i_class_id                integer                       ,
    i_class                   varchar(50)                      ,
    i_category_id             integer                       ,
    i_category                varchar(50)                      ,
    i_manufact_id             integer                       ,
    i_manufact                varchar(50)                      ,
    i_size                    varchar(20)                      ,
    i_formulation             varchar(20)                      ,
    i_color                   varchar(20)                      ,
    i_units                   varchar(10)                      ,
    i_container               varchar(10)                      ,
    i_manager_id              integer                       ,
    i_product_name            varchar(50)                      
)
WITH (
    LOCATION = '/item/',
    DATA_SOURCE = performance_datasets_tpcds_sf3000_delta_part,  
    FILE_FORMAT = tpcds_datasets_parquet1gb_ff
);

create external table store
(
    s_store_sk                integer               ,
    s_store_id                varchar(16)             ,
    s_rec_start_date          date                          ,
    s_rec_end_date            date                          ,
    s_closed_date_sk          integer                       ,
    s_store_name              varchar(50)                   ,
    s_number_employees        integer                       ,
    s_floor_space             integer                       ,
    s_hours                   varchar(20)                      ,
    s_manager                 varchar(40)                   ,
    s_market_id               integer                       ,
    s_geography_class         varchar(100)                  ,
    s_market_desc             varchar(100)                  ,
    s_market_manager          varchar(40)                   ,
    s_division_id             integer                       ,
    s_division_name           varchar(50)                   ,
    s_company_id              integer                       ,
    s_company_name            varchar(50)                   ,
    s_street_number           varchar(10)                   ,
    s_street_name             varchar(60)                   ,
    s_street_type             varchar(15)                      ,
    s_suite_number            varchar(10)                      ,
    s_city                    varchar(60)                   ,
    s_county                  varchar(30)                   ,
    s_state                   varchar(2)                       ,
    s_zip                     varchar(10)                      ,
    s_country                 varchar(20)                   ,
    s_gmt_offset              decimal(5,2)                  ,
    s_tax_precentage          decimal(5,2)                  
)
WITH (
    LOCATION = '/store/',
    DATA_SOURCE = performance_datasets_tpcds_sf3000_delta_part,  
    FILE_FORMAT = tpcds_datasets_parquet1gb_ff
);

create external table call_center
(
    cc_call_center_sk         integer               ,
    cc_call_center_id         varchar(16)              ,
    cc_rec_start_date         date                          ,
    cc_rec_end_date           date                          ,
    cc_closed_date_sk         integer                       ,
    cc_open_date_sk           integer                       ,
    cc_name                   varchar(50)                   ,
    cc_class                  varchar(50)                   ,
    cc_employees              integer                       ,
    cc_sq_ft                  integer                       ,
    cc_hours                  varchar(20)                      ,
    cc_manager                varchar(40)                   ,
    cc_mkt_id                 integer                       ,
    cc_mkt_class              varchar(50)                      ,
    cc_mkt_desc               varchar(100)                  ,
    cc_market_manager         varchar(40)                   ,
    cc_division               integer                       ,
    cc_division_name          varchar(50)                   ,
    cc_company                integer                       ,
    cc_company_name           varchar(50)                      ,
    cc_street_number          varchar(10)                      ,
    cc_street_name            varchar(60)                   ,
    cc_street_type            varchar(15)                      ,
    cc_suite_number           varchar(10)                      ,
    cc_city                   varchar(60)                   ,
    cc_county                 varchar(30)                   ,
    cc_state                  varchar(2)                       ,
    cc_zip                    varchar(10)                      ,
    cc_country                varchar(20)                   ,
    cc_gmt_offset             decimal(5,2)                  ,
    cc_tax_percentage         decimal(5,2)                  
)
WITH (
    LOCATION = '/call_center/',
    DATA_SOURCE = performance_datasets_tpcds_sf3000_delta_part,  
    FILE_FORMAT = tpcds_datasets_parquet1gb_ff
);

create external table customer
(
    c_customer_sk             integer               ,
    c_customer_id             varchar(16)              ,
    c_current_cdemo_sk        integer                       ,
    c_current_hdemo_sk        integer                       ,
    c_current_addr_sk         integer                       ,
    c_first_shipto_date_sk    integer                       ,
    c_first_sales_date_sk     integer                       ,
    c_salutation              varchar(10)                      ,
    c_first_name              varchar(20)                      ,
    c_last_name               varchar(30)                      ,
    c_preferred_cust_flag     varchar(1)                       ,
    c_birth_day               integer                       ,
    c_birth_month             integer                       ,
    c_birth_year              integer                       ,
    c_birth_country           varchar(20)                   ,
    c_login                   varchar(13)                      ,
    c_email_address           varchar(50)                      ,
    c_last_review_date_sk     integer                      ,
    c_last_review_date        integer
)
WITH (
    LOCATION = '/customer/',
    DATA_SOURCE = performance_datasets_tpcds_sf3000_delta_part,  
    FILE_FORMAT = tpcds_datasets_parquet1gb_ff
);

create external table web_site
(
    web_site_sk               integer               ,
    web_site_id               varchar(16)              ,
    web_rec_start_date        date                          ,
    web_rec_end_date          date                          ,
    web_name                  varchar(50)                   ,
    web_open_date_sk          integer                       ,
    web_close_date_sk         integer                       ,
    web_class                 varchar(50)                   ,
    web_manager               varchar(40)                   ,
    web_mkt_id                integer                       ,
    web_mkt_class             varchar(50)                   ,
    web_mkt_desc              varchar(100)                  ,
    web_market_manager        varchar(40)                   ,
    web_company_id            integer                       ,
    web_company_name          varchar(50)                      ,
    web_street_number         varchar(10)                      ,
    web_street_name           varchar(60)                   ,
    web_street_type           varchar(15)                      ,
    web_suite_number          varchar(10)                      ,
    web_city                  varchar(60)                   ,
    web_county                varchar(30)                   ,
    web_state                 varchar(2)                       ,
    web_zip                   varchar(10)                      ,
    web_country               varchar(20)                   ,
    web_gmt_offset            decimal(5,2)                  ,
    web_tax_percentage        decimal(5,2)                  
)
WITH (
    LOCATION = '/web_site/',
    DATA_SOURCE = performance_datasets_tpcds_sf3000_delta_part,  
    FILE_FORMAT = tpcds_datasets_parquet1gb_ff
);

create external table household_demographics
(
    hd_demo_sk                integer               ,
    hd_income_band_sk         integer                       ,
    hd_buy_potential          varchar(15)                      ,
    hd_dep_count              integer                       ,
    hd_vehicle_count          integer                       
)
WITH (
    LOCATION = '/household_demographics/',
    DATA_SOURCE = performance_datasets_tpcds_sf3000_delta_part,  
    FILE_FORMAT = tpcds_datasets_parquet1gb_ff
);

create external table web_page
(
    wp_web_page_sk            integer               ,
    wp_web_page_id            varchar(16)              ,
    wp_rec_start_date         date                          ,
    wp_rec_end_date           date                          ,
    wp_creation_date_sk       integer                       ,
    wp_access_date_sk         integer                       ,
    wp_autogen_flag           varchar(1)                       ,
    wp_customer_sk            integer                       ,
    wp_url                    varchar(100)                  ,
    wp_type                   varchar(50)                      ,
    wp_char_count             integer                       ,
    wp_link_count             integer                       ,
    wp_image_count            integer                       ,
    wp_max_ad_count           integer                       
)
WITH (
    LOCATION = '/web_page/',
    DATA_SOURCE = performance_datasets_tpcds_sf3000_delta_part,  
    FILE_FORMAT = tpcds_datasets_parquet1gb_ff
);

create external table promotion
(
    p_promo_sk                integer               ,
    p_promo_id                varchar(16)              ,
    p_start_date_sk           integer                       ,
    p_end_date_sk             integer                       ,
    p_item_sk                 integer                       ,
    p_cost                    decimal(15,2)                 ,
    p_response_target         integer                       ,
    p_promo_name              varchar(50)                      ,
    p_channel_dmail           varchar(1)                       ,
    p_channel_email           varchar(1)                       ,
    p_channel_catalog         varchar(1)                       ,
    p_channel_tv              varchar(1)                       ,
    p_channel_radio           varchar(1)                       ,
    p_channel_press           varchar(1)                       ,
    p_channel_event           varchar(1)                       ,
    p_channel_demo            varchar(1)                       ,
    p_channel_details         varchar(100)                  ,
    p_purpose                 varchar(15)                      ,
    p_discount_active         varchar(1)                       
)
WITH (
    LOCATION = '/promotion/',
    DATA_SOURCE = performance_datasets_tpcds_sf3000_delta_part,  
    FILE_FORMAT = tpcds_datasets_parquet1gb_ff
);

create external table catalog_page
(
    cp_catalog_page_sk        integer               ,
    cp_catalog_page_id        varchar(16)              ,
    cp_start_date_sk          integer                       ,
    cp_end_date_sk            integer                       ,
    cp_department             varchar(50)                   ,
    cp_catalog_number         integer                       ,
    cp_catalog_page_number    integer                       ,
    cp_description            varchar(100)                  ,
    cp_type                   varchar(100)                  
)
WITH (
    LOCATION = '/catalog_page/',
    DATA_SOURCE = performance_datasets_tpcds_sf3000_delta_part,  
    FILE_FORMAT = tpcds_datasets_parquet1gb_ff
);



