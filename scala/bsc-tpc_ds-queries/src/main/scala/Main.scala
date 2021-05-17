class TPCDSQueries {
  
	//TPC-DS create table statements generated with the TPC-DS toolkit to extract the schema from
	val tpcdsSchemas = Map(
  "call_center" -> 
  """create table call_center
(
    cc_call_center_sk         integer               not null,
    cc_call_center_id         varchar(16)              not null,
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
    cc_tax_percentage         decimal(5,2)                  ,
    primary key (cc_call_center_sk)
);
  """, 
  
  "catalog_page" -> 
  """create table catalog_page
(
    cp_catalog_page_sk        integer               not null,
    cp_catalog_page_id        varchar(16)              not null,
    cp_start_date_sk          integer                       ,
    cp_end_date_sk            integer                       ,
    cp_department             varchar(50)                   ,
    cp_catalog_number         integer                       ,
    cp_catalog_page_number    integer                       ,
    cp_description            varchar(100)                  ,
    cp_type                   varchar(100)                  ,
    primary key (cp_catalog_page_sk)
);

  """,
  "catalog_returns" -> 
  """create table catalog_returns
(
    cr_returned_date_sk       integer                       ,
    cr_returned_time_sk       integer                       ,
    cr_item_sk                integer               not null,
    cr_refunded_customer_sk   integer                       ,
    cr_refunded_cdemo_sk      integer                       ,
    cr_refunded_hdemo_sk      integer                       ,
    cr_refunded_addr_sk       integer                       ,
    cr_returning_customer_sk  integer                       ,
    cr_returning_cdemo_sk     integer                       ,
    cr_returning_hdemo_sk     integer                       ,
    cr_returning_addr_sk      integer                       ,
    cr_call_center_sk         integer                       ,
    cr_catalog_page_sk        integer                       ,
    cr_ship_mode_sk           integer                       ,
    cr_warehouse_sk           integer                       ,
    cr_reason_sk              integer                       ,
    cr_order_number           integer               not null,
    cr_return_quantity        integer                       ,
    cr_return_amount          decimal(7,2)                  ,
    cr_return_tax             decimal(7,2)                  ,
    cr_return_amt_inc_tax     decimal(7,2)                  ,
    cr_fee                    decimal(7,2)                  ,
    cr_return_ship_cost       decimal(7,2)                  ,
    cr_refunded_cash          decimal(7,2)                  ,
    cr_reversed_charge        decimal(7,2)                  ,
    cr_store_credit           decimal(7,2)                  ,
    cr_net_loss               decimal(7,2)                  ,
    primary key (cr_item_sk, cr_order_number)
);
  """,
  "catalog_sales" -> 
  """create table catalog_sales
(
    cs_sold_date_sk           integer                       ,
    cs_sold_time_sk           integer                       ,
    cs_ship_date_sk           integer                       ,
    cs_bill_customer_sk       integer                       ,
    cs_bill_cdemo_sk          integer                       ,
    cs_bill_hdemo_sk          integer                       ,
    cs_bill_addr_sk           integer                       ,
    cs_ship_customer_sk       integer                       ,
    cs_ship_cdemo_sk          integer                       ,
    cs_ship_hdemo_sk          integer                       ,
    cs_ship_addr_sk           integer                       ,
    cs_call_center_sk         integer                       ,
    cs_catalog_page_sk        integer                       ,
    cs_ship_mode_sk           integer                       ,
    cs_warehouse_sk           integer                       ,
    cs_item_sk                integer               not null,
    cs_promo_sk               integer                       ,
    cs_order_number           integer               not null,
    cs_quantity               integer                       ,
    cs_wholesale_cost         decimal(7,2)                  ,
    cs_list_price             decimal(7,2)                  ,
    cs_sales_price            decimal(7,2)                  ,
    cs_ext_discount_amt       decimal(7,2)                  ,
    cs_ext_sales_price        decimal(7,2)                  ,
    cs_ext_wholesale_cost     decimal(7,2)                  ,
    cs_ext_list_price         decimal(7,2)                  ,
    cs_ext_tax                decimal(7,2)                  ,
    cs_coupon_amt             decimal(7,2)                  ,
    cs_ext_ship_cost          decimal(7,2)                  ,
    cs_net_paid               decimal(7,2)                  ,
    cs_net_paid_inc_tax       decimal(7,2)                  ,
    cs_net_paid_inc_ship      decimal(7,2)                  ,
    cs_net_paid_inc_ship_tax  decimal(7,2)                  ,
    cs_net_profit             decimal(7,2)                  ,
    primary key (cs_item_sk, cs_order_number)
);
  """,
  "customer" -> 
  """create table customer
(
    c_customer_sk             integer               not null,
    c_customer_id             varchar(16)              not null,
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
    c_last_review_date        varchar(10)                      ,
    primary key (c_customer_sk)
);
  """,
  "customer_address" -> 
  """create table customer_address
(
    ca_address_sk             integer               not null,
    ca_address_id             varchar(16)              not null,
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
    ca_location_type          varchar(20)                      ,
    primary key (ca_address_sk)
);
  """,
  "customer_demographics" -> 
  """create table customer_demographics
(
    cd_demo_sk                integer               not null,
    cd_gender                 varchar(1)                       ,
    cd_marital_status         varchar(1)                       ,
    cd_education_status       varchar(20)                      ,
    cd_purchase_estimate      integer                       ,
    cd_credit_rating          varchar(10)                      ,
    cd_dep_count              integer                       ,
    cd_dep_employed_count     integer                       ,
    cd_dep_college_count      integer                       ,
    primary key (cd_demo_sk)
);
  """,
  "date_dim" -> 
  """create table date_dim
(
    d_date_sk                 integer               not null,
    d_date_id                 varchar(16)              not null,
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
    d_current_year            varchar(1)                       ,
    primary key (d_date_sk)
);
  """,
  "household_demographics" -> 
  """create table household_demographics
(
    hd_demo_sk                integer               not null,
    hd_income_band_sk         integer                       ,
    hd_buy_potential          varchar(15)                      ,
    hd_dep_count              integer                       ,
    hd_vehicle_count          integer                       ,
    primary key (hd_demo_sk)
);
  """,
  "income_band" -> 
  """create table income_band
(
    ib_income_band_sk         integer               not null,
    ib_lower_bound            integer                       ,
    ib_upper_bound            integer                       ,
    primary key (ib_income_band_sk)
);
""",
  "inventory" ->
  """create table inventory
(
    inv_date_sk               integer               not null,
    inv_item_sk               integer               not null,
    inv_warehouse_sk          integer               not null,
    inv_quantity_on_hand      integer                       ,
    primary key (inv_date_sk, inv_item_sk, inv_warehouse_sk)
);
  """,
  "item" -> 
  """create table item
(
    i_item_sk                 integer               not null,
    i_item_id                 varchar(16)              not null,
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
    i_product_name            varchar(50)                      ,
    primary key (i_item_sk)
);
  """,
  "promotion" -> 
  """create table promotion
(
    p_promo_sk                integer               not null,
    p_promo_id                varchar(16)              not null,
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
    p_discount_active         varchar(1)                       ,
    primary key (p_promo_sk)
);
  """,
  "reason" -> 
  """create table reason
(
    r_reason_sk               integer               not null,
    r_reason_id               varchar(16)              not null,
    r_reason_desc             varchar(100)                     ,
    primary key (r_reason_sk)
);
  """,
  "ship_mode" -> 
  """create table ship_mode
(
    sm_ship_mode_sk           integer               not null,
    sm_ship_mode_id           varchar(16)              not null,
    sm_type                   varchar(30)                      ,
    sm_code                   varchar(10)                      ,
    sm_carrier                varchar(20)                      ,
    sm_contract               varchar(20)                      ,
    primary key (sm_ship_mode_sk)
);
  """,
  "store" -> 
  """create table store
(
    s_store_sk                integer               not null,
    s_store_id                varchar(16)              not null,
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
    s_tax_precentage          decimal(5,2)                  ,
    primary key (s_store_sk)
);
  """,
  "store_returns" -> 
  """create table store_returns
(
    sr_returned_date_sk       integer                       ,
    sr_return_time_sk         integer                       ,
    sr_item_sk                integer               not null,
    sr_customer_sk            integer                       ,
    sr_cdemo_sk               integer                       ,
    sr_hdemo_sk               integer                       ,
    sr_addr_sk                integer                       ,
    sr_store_sk               integer                       ,
    sr_reason_sk              integer                       ,
    sr_ticket_number          integer               not null,
    sr_return_quantity        integer                       ,
    sr_return_amt             decimal(7,2)                  ,
    sr_return_tax             decimal(7,2)                  ,
    sr_return_amt_inc_tax     decimal(7,2)                  ,
    sr_fee                    decimal(7,2)                  ,
    sr_return_ship_cost       decimal(7,2)                  ,
    sr_refunded_cash          decimal(7,2)                  ,
    sr_reversed_charge        decimal(7,2)                  ,
    sr_store_credit           decimal(7,2)                  ,
    sr_net_loss               decimal(7,2)                  ,
    primary key (sr_item_sk, sr_ticket_number)
);
  """,
  "store_sales" -> 
  """create table store_sales
(
    ss_sold_date_sk           integer                       ,
    ss_sold_time_sk           integer                       ,
    ss_item_sk                integer               not null,
    ss_customer_sk            integer                       ,
    ss_cdemo_sk               integer                       ,
    ss_hdemo_sk               integer                       ,
    ss_addr_sk                integer                       ,
    ss_store_sk               integer                       ,
    ss_promo_sk               integer                       ,
    ss_ticket_number          integer               not null,
    ss_quantity               integer                       ,
    ss_wholesale_cost         decimal(7,2)                  ,
    ss_list_price             decimal(7,2)                  ,
    ss_sales_price            decimal(7,2)                  ,
    ss_ext_discount_amt       decimal(7,2)                  ,
    ss_ext_sales_price        decimal(7,2)                  ,
    ss_ext_wholesale_cost     decimal(7,2)                  ,
    ss_ext_list_price         decimal(7,2)                  ,
    ss_ext_tax                decimal(7,2)                  ,
    ss_coupon_amt             decimal(7,2)                  ,
    ss_net_paid               decimal(7,2)                  ,
    ss_net_paid_inc_tax       decimal(7,2)                  ,
    ss_net_profit             decimal(7,2)                  ,
    primary key (ss_item_sk, ss_ticket_number)
);
  """,
  "time_dim" -> 
  """create table time_dim
(
    t_time_sk                 integer               not null,
    t_time_id                 varchar(16)              not null,
    t_time                    integer                       ,
    t_hour                    integer                       ,
    t_minute                  integer                       ,
    t_second                  integer                       ,
    t_am_pm                   varchar(2)                       ,
    t_shift                   varchar(20)                      ,
    t_sub_shift               varchar(20)                      ,
    t_meal_time               varchar(20)                      ,
    primary key (t_time_sk)
);
  """,
  "warehouse" -> 
  """create table warehouse
(
    w_warehouse_sk            integer               not null,
    w_warehouse_id            varchar(16)              not null,
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
    w_gmt_offset              decimal(5,2)                  ,
    primary key (w_warehouse_sk)
);
  """,
  "web_page" -> 
  """create table web_page
(
    wp_web_page_sk            integer               not null,
    wp_web_page_id            varchar(16)              not null,
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
    wp_max_ad_count           integer                       ,
    primary key (wp_web_page_sk)
);
  """,
  "web_returns" -> 
  """create table web_returns
(
    wr_returned_date_sk       integer                       ,
    wr_returned_time_sk       integer                       ,
    wr_item_sk                integer               not null,
    wr_refunded_customer_sk   integer                       ,
    wr_refunded_cdemo_sk      integer                       ,
    wr_refunded_hdemo_sk      integer                       ,
    wr_refunded_addr_sk       integer                       ,
    wr_returning_customer_sk  integer                       ,
    wr_returning_cdemo_sk     integer                       ,
    wr_returning_hdemo_sk     integer                       ,
    wr_returning_addr_sk      integer                       ,
    wr_web_page_sk            integer                       ,
    wr_reason_sk              integer                       ,
    wr_order_number           integer               not null,
    wr_return_quantity        integer                       ,
    wr_return_amt             decimal(7,2)                  ,
    wr_return_tax             decimal(7,2)                  ,
    wr_return_amt_inc_tax     decimal(7,2)                  ,
    wr_fee                    decimal(7,2)                  ,
    wr_return_ship_cost       decimal(7,2)                  ,
    wr_refunded_cash          decimal(7,2)                  ,
    wr_reversed_charge        decimal(7,2)                  ,
    wr_account_credit         decimal(7,2)                  ,
    wr_net_loss               decimal(7,2)                  ,
    primary key (wr_item_sk, wr_order_number)
);
  """,
  "web_sales" -> 
  """create table web_sales
(
    ws_sold_date_sk           integer                       ,
    ws_sold_time_sk           integer                       ,
    ws_ship_date_sk           integer                       ,
    ws_item_sk                integer               not null,
    ws_bill_customer_sk       integer                       ,
    ws_bill_cdemo_sk          integer                       ,
    ws_bill_hdemo_sk          integer                       ,
    ws_bill_addr_sk           integer                       ,
    ws_ship_customer_sk       integer                       ,
    ws_ship_cdemo_sk          integer                       ,
    ws_ship_hdemo_sk          integer                       ,
    ws_ship_addr_sk           integer                       ,
    ws_web_page_sk            integer                       ,
    ws_web_site_sk            integer                       ,
    ws_ship_mode_sk           integer                       ,
    ws_warehouse_sk           integer                       ,
    ws_promo_sk               integer                       ,
    ws_order_number           integer               not null,
    ws_quantity               integer                       ,
    ws_wholesale_cost         decimal(7,2)                  ,
    ws_list_price             decimal(7,2)                  ,
    ws_sales_price            decimal(7,2)                  ,
    ws_ext_discount_amt       decimal(7,2)                  ,
    ws_ext_sales_price        decimal(7,2)                  ,
    ws_ext_wholesale_cost     decimal(7,2)                  ,
    ws_ext_list_price         decimal(7,2)                  ,
    ws_ext_tax                decimal(7,2)                  ,
    ws_coupon_amt             decimal(7,2)                  ,
    ws_ext_ship_cost          decimal(7,2)                  ,
    ws_net_paid               decimal(7,2)                  ,
    ws_net_paid_inc_tax       decimal(7,2)                  ,
    ws_net_paid_inc_ship      decimal(7,2)                  ,
    ws_net_paid_inc_ship_tax  decimal(7,2)                  ,
    ws_net_profit             decimal(7,2)                  ,
    primary key (ws_item_sk, ws_order_number)
);
  """,
  "web_site" -> 
  """create table web_site
(
    web_site_sk               integer               not null,
    web_site_id               varchar(16)              not null,
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
    web_tax_percentage        decimal(5,2)                  ,
    primary key (web_site_sk)
);
  """
)

//Fact Tables partition keys
val partitionKeys = Map (
  "catalog_returns" -> "cr_returned_date_sk",
  "catalog_sales" -> "cs_sold_date_sk",
  "inventory" -> "inv_date_sk",
  "store_returns" -> "sr_returned_date_sk",
  "store_sales" -> "ss_sold_date_sk",
  "web_returns" -> "wr_returned_date_sk",
  "web_sales" ->"ws_sold_date_sk"
)

//Denormalization queries for the tables catalog_sales, store_sales & web_sales
val denormQueries = Map (
"catalog_sales" ->
  """select catalog_sales.*,
sold_date_dim.d_date_sk as sold_d_date_sk,
sold_date_dim.d_date_id as sold_d_date_id,
sold_date_dim.d_date as sold_d_date,
sold_date_dim.d_month_seq as sold_d_month_seq,
sold_date_dim.d_week_seq as sold_d_week_seq,
sold_date_dim.d_quarter_seq as sold_d_quarter_seq,
sold_date_dim.d_year as sold_d_year,
sold_date_dim.d_dow as sold_d_dow,
sold_date_dim.d_moy as sold_d_moy,
sold_date_dim.d_dom as sold_d_dom,
sold_date_dim.d_qoy as sold_d_qoy,
sold_date_dim.d_fy_year as sold_d_fy_year,
sold_date_dim.d_fy_quarter_seq as sold_d_fy_quarter_seq,
sold_date_dim.d_fy_week_seq as sold_d_fy_week_seq,
sold_date_dim.d_day_name as sold_d_day_name,
sold_date_dim.d_quarter_name as sold_d_quarter_name,
sold_date_dim.d_holiday as sold_d_holiday,
sold_date_dim.d_weekend as sold_d_weekend,
sold_date_dim.d_following_holiday as sold_d_following_holiday,
sold_date_dim.d_first_dom as sold_d_first_dom,
sold_date_dim.d_last_dom as sold_d_last_dom,
sold_date_dim.d_same_day_ly as sold_d_same_day_ly,
sold_date_dim.d_same_day_lq as sold_d_same_day_lq,
sold_date_dim.d_current_day as sold_d_current_day,
sold_date_dim.d_current_week as sold_d_current_week,
sold_date_dim.d_current_month as sold_d_current_month,
sold_date_dim.d_current_quarter as sold_d_current_quarter,
sold_date_dim.d_current_year as sold_d_current_year,
ship_date_dim.d_date_sk as ship_d_date_sk,
ship_date_dim.d_date_id as ship_d_date_id,
ship_date_dim.d_date as ship_d_date,
ship_date_dim.d_month_seq as ship_d_month_seq,
ship_date_dim.d_week_seq as ship_d_week_seq,
ship_date_dim.d_quarter_seq as ship_d_quarter_seq,
ship_date_dim.d_year as ship_d_year,
ship_date_dim.d_dow as ship_d_dow,
ship_date_dim.d_moy as ship_d_moy,
ship_date_dim.d_dom as ship_d_dom,
ship_date_dim.d_qoy as ship_d_qoy,
ship_date_dim.d_fy_year as ship_d_fy_year,
ship_date_dim.d_fy_quarter_seq as ship_d_fy_quarter_seq,
ship_date_dim.d_fy_week_seq as ship_d_fy_week_seq,
ship_date_dim.d_day_name as ship_d_day_name,
ship_date_dim.d_quarter_name as ship_d_quarter_name,
ship_date_dim.d_holiday as ship_d_holiday,
ship_date_dim.d_weekend as ship_d_weekend,
ship_date_dim.d_following_holiday as ship_d_following_holiday,
ship_date_dim.d_first_dom as ship_d_first_dom,
ship_date_dim.d_last_dom as ship_d_last_dom,
ship_date_dim.d_same_day_ly as ship_d_same_day_ly,
ship_date_dim.d_same_day_lq as ship_d_same_day_lq,
ship_date_dim.d_current_day as ship_d_current_day,
ship_date_dim.d_current_week as ship_d_current_week,
ship_date_dim.d_current_month as ship_d_current_month,
ship_date_dim.d_current_quarter as ship_d_current_quarter,
ship_date_dim.d_current_year as ship_d_current_year,
time_dim.*,
bill_customer.c_customer_sk as bill_c_customer_sk,
bill_customer.c_customer_id as bill_c_customer_id,
bill_customer.c_current_cdemo_sk as bill_c_current_cdemo_sk,
bill_customer.c_current_hdemo_sk as bill_c_current_hdemo_sk,
bill_customer.c_current_addr_sk as bill_c_current_addr_sk,
bill_customer.c_first_shipto_date_sk as bill_c_first_shipto_date_sk,
bill_customer.c_first_sales_date_sk as bill_c_first_sales_date_sk,
bill_customer.c_salutation as bill_c_salutation,
bill_customer.c_first_name as bill_c_first_name,
bill_customer.c_last_name as bill_c_last_name,
bill_customer.c_preferred_cust_flag as bill_c_preferred_cust_flag,
bill_customer.c_birth_day as bill_c_birth_day,
bill_customer.c_birth_month as bill_c_birth_month,
bill_customer.c_birth_year as bill_c_birth_year,
bill_customer.c_birth_country as bill_c_birth_country,
bill_customer.c_login as bill_c_login,
bill_customer.c_email_address as bill_c_email_address,
bill_customer.c_last_review_date as bill_c_last_review_date,
ship_customer.c_customer_sk as ship_c_customer_sk,
ship_customer.c_customer_id as ship_c_customer_id,
ship_customer.c_current_cdemo_sk as ship_c_current_cdemo_sk,
ship_customer.c_current_hdemo_sk as ship_c_current_hdemo_sk,
ship_customer.c_current_addr_sk as ship_c_current_addr_sk,
ship_customer.c_first_shipto_date_sk as ship_c_first_shipto_date_sk,
ship_customer.c_first_sales_date_sk as ship_c_first_sales_date_sk,
ship_customer.c_salutation as ship_c_salutation,
ship_customer.c_first_name as ship_c_first_name,
ship_customer.c_last_name as ship_c_last_name,
ship_customer.c_preferred_cust_flag as ship_c_preferred_cust_flag,
ship_customer.c_birth_day as ship_c_birth_day,
ship_customer.c_birth_month as ship_c_birth_month,
ship_customer.c_birth_year as ship_c_birth_year,
ship_customer.c_birth_country as ship_c_birth_country,
ship_customer.c_login as ship_c_login,
ship_customer.c_email_address as ship_c_email_address,
ship_customer.c_last_review_date as ship_c_last_review_date,
bill_cd.cd_demo_sk as bill_cd_demo_sk,
bill_cd.cd_gender as bill_cd_gender,
bill_cd.cd_marital_status as bill_cd_marital_status,
bill_cd.cd_education_status as bill_cd_education_status,
bill_cd.cd_purchase_estimate as bill_cd_purchase_estimate,
bill_cd.cd_credit_rating as bill_cd_credit_rating,
bill_cd.cd_dep_count as bill_cd_dep_count,
bill_cd.cd_dep_employed_count as bill_cd_dep_employed_count,
bill_cd.cd_dep_college_count as bill_cd_dep_college_count,
ship_cd.cd_demo_sk as ship_cd_demo_sk,
ship_cd.cd_gender as ship_cd_gender,
ship_cd.cd_marital_status as ship_cd_marital_status,
ship_cd.cd_education_status as ship_cd_education_status,
ship_cd.cd_purchase_estimate as ship_cd_purchase_estimate,
ship_cd.cd_credit_rating as ship_cd_credit_rating,
ship_cd.cd_dep_count as ship_cd_dep_count,
ship_cd.cd_dep_employed_count as ship_cd_dep_employed_count,
ship_cd.cd_dep_college_count as ship_cd_dep_college_count,
bill_hd.hd_demo_sk as bill_hd_demo_sk,
bill_hd.hd_income_band_sk as bill_hd_income_band_sk,
bill_hd.hd_buy_potential as bill_hd_buy_potential,
bill_hd.hd_dep_count as bill_hd_dep_count,
bill_hd.hd_vehicle_count as bill_hd_vehicle_count,
ship_hd.hd_demo_sk as ship_hd_demo_sk,
ship_hd.hd_income_band_sk as ship_hd_income_band_sk,
ship_hd.hd_buy_potential as ship_hd_buy_potential,
ship_hd.hd_dep_count as ship_hd_dep_count,
ship_hd.hd_vehicle_count as ship_hd_vehicle_count,
bill_ca.ca_address_sk as bill_ca_address_sk,
bill_ca.ca_address_id as bill_ca_address_id,
bill_ca.ca_street_number as bill_ca_street_number,
bill_ca.ca_street_name as bill_ca_street_name,
bill_ca.ca_street_type as bill_ca_street_type,
bill_ca.ca_suite_number as bill_ca_suite_number,
bill_ca.ca_city as bill_ca_city,
bill_ca.ca_county as bill_ca_county,
bill_ca.ca_state as bill_ca_state,
bill_ca.ca_zip as bill_ca_zip,
bill_ca.ca_country as bill_ca_country,
bill_ca.ca_gmt_offset as bill_ca_gmt_offset,
bill_ca.ca_location_type as bill_ca_location_type,
ship_ca.ca_address_sk as ship_ca_address_sk,
ship_ca.ca_address_id as ship_ca_address_id,
ship_ca.ca_street_number as ship_ca_street_number,
ship_ca.ca_street_name as ship_ca_street_name,
ship_ca.ca_street_type as ship_ca_street_type,
ship_ca.ca_suite_number as ship_ca_suite_number,
ship_ca.ca_city as ship_ca_city,
ship_ca.ca_county as ship_ca_county,
ship_ca.ca_state as ship_ca_state,
ship_ca.ca_zip as ship_ca_zip,
ship_ca.ca_country as ship_ca_country,
ship_ca.ca_gmt_offset as ship_ca_gmt_offset,
ship_ca.ca_location_type as ship_ca_location_type,
call_center.*, catalog_page.*, ship_mode.*, warehouse.*, promotion.*, item.*
from
catalog_sales, date_dim sold_date_dim, date_dim ship_date_dim, time_dim,
customer bill_customer, customer ship_customer, customer_demographics bill_cd, customer_demographics ship_cd,
household_demographics bill_hd, household_demographics ship_hd,
customer_address bill_ca, customer_address ship_ca, 
call_center, catalog_page, ship_mode, warehouse, promotion, item
where
cs_sold_date_sk = sold_date_dim.d_date_sk and
cs_sold_time_sk = t_time_sk and
cs_ship_date_sk = ship_date_dim.d_date_sk and
cs_bill_customer_sk = bill_customer.c_customer_sk and
cs_bill_cdemo_sk = bill_cd.cd_demo_sk and
cs_bill_hdemo_sk = bill_hd.hd_demo_sk and
cs_bill_addr_sk = bill_ca.ca_address_sk and
cs_ship_customer_sk = ship_customer.c_customer_sk and
cs_ship_cdemo_sk = ship_cd.cd_demo_sk and
cs_ship_hdemo_sk = ship_hd.hd_demo_sk and
cs_ship_addr_sk = ship_ca.ca_address_sk and
cs_call_center_sk = cc_call_center_sk and
cs_catalog_page_sk = cp_catalog_page_sk and
cs_ship_mode_sk = sm_ship_mode_sk and
cs_warehouse_sk = w_warehouse_sk and
cs_item_sk = i_item_sk and
cs_promo_sk = p_promo_sk""",
  
"store_sales" ->
"""select * from
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
ss_promo_sk = p_promo_sk""",
  
"web_sales" ->
"""select web_sales.*,
sold_date_dim.d_date_sk as sold_d_date_sk,
sold_date_dim.d_date_id as sold_d_date_id,
sold_date_dim.d_date as sold_d_date,
sold_date_dim.d_month_seq as sold_d_month_seq,
sold_date_dim.d_week_seq as sold_d_week_seq,
sold_date_dim.d_quarter_seq as sold_d_quarter_seq,
sold_date_dim.d_year as sold_d_year,
sold_date_dim.d_dow as sold_d_dow,
sold_date_dim.d_moy as sold_d_moy,
sold_date_dim.d_dom as sold_d_dom,
sold_date_dim.d_qoy as sold_d_qoy,
sold_date_dim.d_fy_year as sold_d_fy_year,
sold_date_dim.d_fy_quarter_seq as sold_d_fy_quarter_seq,
sold_date_dim.d_fy_week_seq as sold_d_fy_week_seq,
sold_date_dim.d_day_name as sold_d_day_name,
sold_date_dim.d_quarter_name as sold_d_quarter_name,
sold_date_dim.d_holiday as sold_d_holiday,
sold_date_dim.d_weekend as sold_d_weekend,
sold_date_dim.d_following_holiday as sold_d_following_holiday,
sold_date_dim.d_first_dom as sold_d_first_dom,
sold_date_dim.d_last_dom as sold_d_last_dom,
sold_date_dim.d_same_day_ly as sold_d_same_day_ly,
sold_date_dim.d_same_day_lq as sold_d_same_day_lq,
sold_date_dim.d_current_day as sold_d_current_day,
sold_date_dim.d_current_week as sold_d_current_week,
sold_date_dim.d_current_month as sold_d_current_month,
sold_date_dim.d_current_quarter as sold_d_current_quarter,
sold_date_dim.d_current_year as sold_d_current_year,
ship_date_dim.d_date_sk as ship_d_date_sk,
ship_date_dim.d_date_id as ship_d_date_id,
ship_date_dim.d_date as ship_d_date,
ship_date_dim.d_month_seq as ship_d_month_seq,
ship_date_dim.d_week_seq as ship_d_week_seq,
ship_date_dim.d_quarter_seq as ship_d_quarter_seq,
ship_date_dim.d_year as ship_d_year,
ship_date_dim.d_dow as ship_d_dow,
ship_date_dim.d_moy as ship_d_moy,
ship_date_dim.d_dom as ship_d_dom,
ship_date_dim.d_qoy as ship_d_qoy,
ship_date_dim.d_fy_year as ship_d_fy_year,
ship_date_dim.d_fy_quarter_seq as ship_d_fy_quarter_seq,
ship_date_dim.d_fy_week_seq as ship_d_fy_week_seq,
ship_date_dim.d_day_name as ship_d_day_name,
ship_date_dim.d_quarter_name as ship_d_quarter_name,
ship_date_dim.d_holiday as ship_d_holiday,
ship_date_dim.d_weekend as ship_d_weekend,
ship_date_dim.d_following_holiday as ship_d_following_holiday,
ship_date_dim.d_first_dom as ship_d_first_dom,
ship_date_dim.d_last_dom as ship_d_last_dom,
ship_date_dim.d_same_day_ly as ship_d_same_day_ly,
ship_date_dim.d_same_day_lq as ship_d_same_day_lq,
ship_date_dim.d_current_day as ship_d_current_day,
ship_date_dim.d_current_week as ship_d_current_week,
ship_date_dim.d_current_month as ship_d_current_month,
ship_date_dim.d_current_quarter as ship_d_current_quarter,
ship_date_dim.d_current_year as ship_d_current_year,
time_dim.*,
bill_customer.c_customer_sk as bill_c_customer_sk,
bill_customer.c_customer_id as bill_c_customer_id,
bill_customer.c_current_cdemo_sk as bill_c_current_cdemo_sk,
bill_customer.c_current_hdemo_sk as bill_c_current_hdemo_sk,
bill_customer.c_current_addr_sk as bill_c_current_addr_sk,
bill_customer.c_first_shipto_date_sk as bill_c_first_shipto_date_sk,
bill_customer.c_first_sales_date_sk as bill_c_first_sales_date_sk,
bill_customer.c_salutation as bill_c_salutation,
bill_customer.c_first_name as bill_c_first_name,
bill_customer.c_last_name as bill_c_last_name,
bill_customer.c_preferred_cust_flag as bill_c_preferred_cust_flag,
bill_customer.c_birth_day as bill_c_birth_day,
bill_customer.c_birth_month as bill_c_birth_month,
bill_customer.c_birth_year as bill_c_birth_year,
bill_customer.c_birth_country as bill_c_birth_country,
bill_customer.c_login as bill_c_login,
bill_customer.c_email_address as bill_c_email_address,
bill_customer.c_last_review_date as bill_c_last_review_date,
ship_customer.c_customer_sk as ship_c_customer_sk,
ship_customer.c_customer_id as ship_c_customer_id,
ship_customer.c_current_cdemo_sk as ship_c_current_cdemo_sk,
ship_customer.c_current_hdemo_sk as ship_c_current_hdemo_sk,
ship_customer.c_current_addr_sk as ship_c_current_addr_sk,
ship_customer.c_first_shipto_date_sk as ship_c_first_shipto_date_sk,
ship_customer.c_first_sales_date_sk as ship_c_first_sales_date_sk,
ship_customer.c_salutation as ship_c_salutation,
ship_customer.c_first_name as ship_c_first_name,
ship_customer.c_last_name as ship_c_last_name,
ship_customer.c_preferred_cust_flag as ship_c_preferred_cust_flag,
ship_customer.c_birth_day as ship_c_birth_day,
ship_customer.c_birth_month as ship_c_birth_month,
ship_customer.c_birth_year as ship_c_birth_year,
ship_customer.c_birth_country as ship_c_birth_country,
ship_customer.c_login as ship_c_login,
ship_customer.c_email_address as ship_c_email_address,
ship_customer.c_last_review_date as ship_c_last_review_date,
bill_cd.cd_demo_sk as bill_cd_demo_sk,
bill_cd.cd_gender as bill_cd_gender,
bill_cd.cd_marital_status as bill_cd_marital_status,
bill_cd.cd_education_status as bill_cd_education_status,
bill_cd.cd_purchase_estimate as bill_cd_purchase_estimate,
bill_cd.cd_credit_rating as bill_cd_credit_rating,
bill_cd.cd_dep_count as bill_cd_dep_count,
bill_cd.cd_dep_employed_count as bill_cd_dep_employed_count,
bill_cd.cd_dep_college_count as bill_cd_dep_college_count,
ship_cd.cd_demo_sk as ship_cd_demo_sk,
ship_cd.cd_gender as ship_cd_gender,
ship_cd.cd_marital_status as ship_cd_marital_status,
ship_cd.cd_education_status as ship_cd_education_status,
ship_cd.cd_purchase_estimate as ship_cd_purchase_estimate,
ship_cd.cd_credit_rating as ship_cd_credit_rating,
ship_cd.cd_dep_count as ship_cd_dep_count,
ship_cd.cd_dep_employed_count as ship_cd_dep_employed_count,
ship_cd.cd_dep_college_count as ship_cd_dep_college_count,
bill_hd.hd_demo_sk as bill_hd_demo_sk,
bill_hd.hd_income_band_sk as bill_hd_income_band_sk,
bill_hd.hd_buy_potential as bill_hd_buy_potential,
bill_hd.hd_dep_count as bill_hd_dep_count,
bill_hd.hd_vehicle_count as bill_hd_vehicle_count,
ship_hd.hd_demo_sk as ship_hd_demo_sk,
ship_hd.hd_income_band_sk as ship_hd_income_band_sk,
ship_hd.hd_buy_potential as ship_hd_buy_potential,
ship_hd.hd_dep_count as ship_hd_dep_count,
ship_hd.hd_vehicle_count as ship_hd_vehicle_count,
bill_ca.ca_address_sk as bill_ca_address_sk,
bill_ca.ca_address_id as bill_ca_address_id,
bill_ca.ca_street_number as bill_ca_street_number,
bill_ca.ca_street_name as bill_ca_street_name,
bill_ca.ca_street_type as bill_ca_street_type,
bill_ca.ca_suite_number as bill_ca_suite_number,
bill_ca.ca_city as bill_ca_city,
bill_ca.ca_county as bill_ca_county,
bill_ca.ca_state as bill_ca_state,
bill_ca.ca_zip as bill_ca_zip,
bill_ca.ca_country as bill_ca_country,
bill_ca.ca_gmt_offset as bill_ca_gmt_offset,
bill_ca.ca_location_type as bill_ca_location_type,
ship_ca.ca_address_sk as ship_ca_address_sk,
ship_ca.ca_address_id as ship_ca_address_id,
ship_ca.ca_street_number as ship_ca_street_number,
ship_ca.ca_street_name as ship_ca_street_name,
ship_ca.ca_street_type as ship_ca_street_type,
ship_ca.ca_suite_number as ship_ca_suite_number,
ship_ca.ca_city as ship_ca_city,
ship_ca.ca_county as ship_ca_county,
ship_ca.ca_state as ship_ca_state,
ship_ca.ca_zip as ship_ca_zip,
ship_ca.ca_country as ship_ca_country,
ship_ca.ca_gmt_offset as ship_ca_gmt_offset,
ship_ca.ca_location_type as ship_ca_location_type,
web_page.*, web_site.*, ship_mode.*, warehouse.*, promotion.*, item.*
from
web_sales, date_dim sold_date_dim, date_dim ship_date_dim, time_dim,
customer bill_customer, customer ship_customer, customer_demographics bill_cd, customer_demographics ship_cd,
household_demographics bill_hd, household_demographics ship_hd,
customer_address bill_ca, customer_address ship_ca, 
web_page, web_site, ship_mode, warehouse, promotion, item
where
ws_sold_date_sk = sold_date_dim.d_date_sk and
ws_sold_time_sk = t_time_sk and
ws_ship_date_sk = ship_date_dim.d_date_sk and
ws_item_sk = i_item_sk and
ws_bill_customer_sk = bill_customer.c_customer_sk and
ws_bill_cdemo_sk = bill_cd.cd_demo_sk and
ws_bill_hdemo_sk = bill_hd.hd_demo_sk and
ws_bill_addr_sk = bill_ca.ca_address_sk and
ws_ship_customer_sk = ship_customer.c_customer_sk and
ws_ship_cdemo_sk = ship_cd.cd_demo_sk and
ws_ship_hdemo_sk = ship_hd.hd_demo_sk and
ws_ship_addr_sk = ship_ca.ca_address_sk and
ws_web_page_sk = wp_web_page_sk and
ws_web_site_sk = web_site_sk and
ws_ship_mode_sk = sm_ship_mode_sk and
ws_warehouse_sk = w_warehouse_sk and
ws_promo_sk = p_promo_sk""")

val readTestQueries = Array[Tuple2[String, String]](
("q3",
"""select  d_year 
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
 limit 100"""),
("q6",
"""select  s.ca_state state, count(*) cnt
 from store_sales_denorm<SUFFIX> s
 where       s.d_month_seq = 
 	     (select distinct (d_month_seq)
 	      from store_sales_denorm<SUFFIX>
               where d_year = 2000
 	        and d_moy = 2 )
 	and s.i_current_price > 1.2 * 
             (select avg(j.i_current_price) 
 	     from store_sales_denorm<SUFFIX> j 
 	     where j.i_category = s.i_category)
 group by s.ca_state
 having count(*) >= 10
 order by cnt, s.ca_state 
 limit 100"""),
("q7",
"""select  i_item_id, 
        avg(ss_quantity) agg1,
        avg(ss_list_price) agg2,
        avg(ss_coupon_amt) agg3,
        avg(ss_sales_price) agg4 
 from store_sales_denorm<SUFFIX>
 where cd_gender = 'F' and 
       cd_marital_status = 'W' and
       cd_education_status = 'Primary' and
       (p_channel_email = 'N' or p_channel_event = 'N') and
       d_year = 1998 
 group by i_item_id
 order by i_item_id
 limit 100"""),
("q8",
"""select  s_store_name
      ,sum(ss_net_profit)
 from store_sales_denorm<SUFFIX>
     ,(select ca_zip
     from (
      SELECT substr(ca_zip,1,5) ca_zip
      FROM store_sales_denorm<SUFFIX>
      WHERE substr(ca_zip,1,5) IN (
                          '89436','30868','65085','22977','83927','77557',
                          '58429','40697','80614','10502','32779',
                          '91137','61265','98294','17921','18427',
                          '21203','59362','87291','84093','21505',
                          '17184','10866','67898','25797','28055',
                          '18377','80332','74535','21757','29742',
                          '90885','29898','17819','40811','25990',
                          '47513','89531','91068','10391','18846',
                          '99223','82637','41368','83658','86199',
                          '81625','26696','89338','88425','32200',
                          '81427','19053','77471','36610','99823',
                          '43276','41249','48584','83550','82276',
                          '18842','78890','14090','38123','40936',
                          '34425','19850','43286','80072','79188',
                          '54191','11395','50497','84861','90733',
                          '21068','57666','37119','25004','57835',
                          '70067','62878','95806','19303','18840',
                          '19124','29785','16737','16022','49613',
                          '89977','68310','60069','98360','48649',
                          '39050','41793','25002','27413','39736',
                          '47208','16515','94808','57648','15009',
                          '80015','42961','63982','21744','71853',
                          '81087','67468','34175','64008','20261',
                          '11201','51799','48043','45645','61163',
                          '48375','36447','57042','21218','41100',
                          '89951','22745','35851','83326','61125',
                          '78298','80752','49858','52940','96976',
                          '63792','11376','53582','18717','90226',
                          '50530','94203','99447','27670','96577',
                          '57856','56372','16165','23427','54561',
                          '28806','44439','22926','30123','61451',
                          '92397','56979','92309','70873','13355',
                          '21801','46346','37562','56458','28286',
                          '47306','99555','69399','26234','47546',
                          '49661','88601','35943','39936','25632',
                          '24611','44166','56648','30379','59785',
                          '11110','14329','93815','52226','71381',
                          '13842','25612','63294','14664','21077',
                          '82626','18799','60915','81020','56447',
                          '76619','11433','13414','42548','92713',
                          '70467','30884','47484','16072','38936',
                          '13036','88376','45539','35901','19506',
                          '65690','73957','71850','49231','14276',
                          '20005','18384','76615','11635','38177',
                          '55607','41369','95447','58581','58149',
                          '91946','33790','76232','75692','95464',
                          '22246','51061','56692','53121','77209',
                          '15482','10688','14868','45907','73520',
                          '72666','25734','17959','24677','66446',
                          '94627','53535','15560','41967','69297',
                          '11929','59403','33283','52232','57350',
                          '43933','40921','36635','10827','71286',
                          '19736','80619','25251','95042','15526',
                          '36496','55854','49124','81980','35375',
                          '49157','63512','28944','14946','36503',
                          '54010','18767','23969','43905','66979',
                          '33113','21286','58471','59080','13395',
                          '79144','70373','67031','38360','26705',
                          '50906','52406','26066','73146','15884',
                          '31897','30045','61068','45550','92454',
                          '13376','14354','19770','22928','97790',
                          '50723','46081','30202','14410','20223',
                          '88500','67298','13261','14172','81410',
                          '93578','83583','46047','94167','82564',
                          '21156','15799','86709','37931','74703',
                          '83103','23054','70470','72008','49247',
                          '91911','69998','20961','70070','63197',
                          '54853','88191','91830','49521','19454',
                          '81450','89091','62378','25683','61869',
                          '51744','36580','85778','36871','48121',
                          '28810','83712','45486','67393','26935',
                          '42393','20132','55349','86057','21309',
                          '80218','10094','11357','48819','39734',
                          '40758','30432','21204','29467','30214',
                          '61024','55307','74621','11622','68908',
                          '33032','52868','99194','99900','84936',
                          '69036','99149','45013','32895','59004',
                          '32322','14933','32936','33562','72550',
                          '27385','58049','58200','16808','21360',
                          '32961','18586','79307','15492')
     intersect
      select ca_zip
      from (SELECT substr(ca_zip,1,5) ca_zip,count(*) cnt
            FROM store_sales_denorm<SUFFIX>
            WHERE c_preferred_cust_flag='Y'
            group by ca_zip
            having count(*) > 10)A1)A2) V1
 where d_qoy = 1 and d_year = 2002
  and (substr(s_zip,1,2) = substr(V1.ca_zip,1,2))
 group by s_store_name
 order by s_store_name
 limit 100"""),
("q9",
"""select case when (select count(*) 
                  from store_sales_denorm<SUFFIX> 
                  where ss_quantity between 1 and 20) > 48409437
            then (select avg(ss_ext_discount_amt) 
                  from store_sales_denorm<SUFFIX> 
                  where ss_quantity between 1 and 20) 
            else (select avg(ss_net_profit)
                  from store_sales_denorm<SUFFIX>
                  where ss_quantity between 1 and 20) end bucket1 ,
       case when (select count(*)
                  from store_sales_denorm<SUFFIX>
                  where ss_quantity between 21 and 40) > 24804257
            then (select avg(ss_ext_discount_amt)
                  from store_sales_denorm<SUFFIX>
                  where ss_quantity between 21 and 40) 
            else (select avg(ss_net_profit)
                  from store_sales_denorm<SUFFIX>
                  where ss_quantity between 21 and 40) end bucket2,
       case when (select count(*)
                  from store_sales_denorm<SUFFIX>
                  where ss_quantity between 41 and 60) > 128048939
            then (select avg(ss_ext_discount_amt)
                  from store_sales_denorm<SUFFIX>
                  where ss_quantity between 41 and 60)
            else (select avg(ss_net_profit)
                  from store_sales_denorm<SUFFIX>
                  where ss_quantity between 41 and 60) end bucket3,
       case when (select count(*)
                  from store_sales_denorm<SUFFIX>
                  where ss_quantity between 61 and 80) > 56503968
            then (select avg(ss_ext_discount_amt)
                  from store_sales_denorm<SUFFIX>
                  where ss_quantity between 61 and 80)
            else (select avg(ss_net_profit)
                  from store_sales_denorm<SUFFIX>
                  where ss_quantity between 61 and 80) end bucket4,
       case when (select count(*)
                  from store_sales_denorm<SUFFIX>
                  where ss_quantity between 81 and 100) > 43571537
            then (select avg(ss_ext_discount_amt)
                  from store_sales_denorm<SUFFIX>
                  where ss_quantity between 81 and 100)
            else (select avg(ss_net_profit)
                  from store_sales_denorm<SUFFIX>
                  where ss_quantity between 81 and 100) end bucket5
from reason
where r_reason_sk = 1"""),
("q13",
"""select avg(ss_quantity)
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
     ))"""),
("q19",
"""select  i_brand_id brand_id, i_brand brand, i_manufact_id, i_manufact,
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
limit 100"""),
("q27",
"""select  i_item_id,
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
 limit 100"""),
("q28",
"""select  *
from (select avg(ss_list_price) B1_LP
            ,count(ss_list_price) B1_CNT
            ,count(distinct ss_list_price) B1_CNTD
      from store_sales_denorm<SUFFIX>
      where ss_quantity between 0 and 5
        and (ss_list_price between 11 and 11+10 
             or ss_coupon_amt between 460 and 460+1000
             or ss_wholesale_cost between 14 and 14+20)) B1,
     (select avg(ss_list_price) B2_LP
            ,count(ss_list_price) B2_CNT
            ,count(distinct ss_list_price) B2_CNTD
      from store_sales_denorm<SUFFIX>
      where ss_quantity between 6 and 10
        and (ss_list_price between 91 and 91+10
          or ss_coupon_amt between 1430 and 1430+1000
          or ss_wholesale_cost between 32 and 32+20)) B2,
     (select avg(ss_list_price) B3_LP
            ,count(ss_list_price) B3_CNT
            ,count(distinct ss_list_price) B3_CNTD
      from store_sales_denorm<SUFFIX>
      where ss_quantity between 11 and 15
        and (ss_list_price between 66 and 66+10
          or ss_coupon_amt between 920 and 920+1000
          or ss_wholesale_cost between 4 and 4+20)) B3,
     (select avg(ss_list_price) B4_LP
            ,count(ss_list_price) B4_CNT
            ,count(distinct ss_list_price) B4_CNTD
      from store_sales_denorm<SUFFIX>
      where ss_quantity between 16 and 20
        and (ss_list_price between 142 and 142+10
          or ss_coupon_amt between 3054 and 3054+1000
          or ss_wholesale_cost between 80 and 80+20)) B4,
     (select avg(ss_list_price) B5_LP
            ,count(ss_list_price) B5_CNT
            ,count(distinct ss_list_price) B5_CNTD
      from store_sales_denorm<SUFFIX>
      where ss_quantity between 21 and 25
        and (ss_list_price between 135 and 135+10
          or ss_coupon_amt between 14180 and 14180+1000
          or ss_wholesale_cost between 38 and 38+20)) B5,
     (select avg(ss_list_price) B6_LP
            ,count(ss_list_price) B6_CNT
            ,count(distinct ss_list_price) B6_CNTD
      from store_sales_denorm<SUFFIX>
      where ss_quantity between 26 and 30
        and (ss_list_price between 28 and 28+10
          or ss_coupon_amt between 2513 and 2513+1000
          or ss_wholesale_cost between 42 and 42+20)) B6
limit 100"""),
("q36",
"""select  
    sum(ss_net_profit)/sum(ss_ext_sales_price) as gross_margin
   ,i_category
   ,i_class
   ,grouping(i_category)+grouping(i_class) as lochierarchy
   ,rank() over (
 	partition by grouping(i_category)+grouping(i_class),
 	case when grouping(i_class) = 0 then i_category end 
 	order by sum(ss_net_profit)/sum(ss_ext_sales_price) asc) as rank_within_parent
 from
    store_sales_denorm<SUFFIX>
 where
    d_year = 1999 
 and s_state in ('NE','IN','SD','MN',
                 'TX','MN','MI','LA')
 group by rollup(i_category,i_class)
 order by
   lochierarchy desc
  ,case when lochierarchy = 0 then i_category end
  ,rank_within_parent
  limit 100"""))

}

object Main extends App {
  val tpcdsQueries = new TPCDSQueries()
  val tpcdsSchemas = tpcdsQueries.tpcdsSchemas
  println(tpcdsSchemas("call_center"))
  val partitionKeys = tpcdsQueries.partitionKeys
  println(partitionKeys("inventory"))
  val denormQueries = tpcdsQueries.denormQueries
  println(denormQueries("store_sales"))
  val readTestQueries = tpcdsQueries.readTestQueries
  println(readTestQueries(0)._2)
}


