--Execute as azureuser (must run only the create schema statement below)
CREATE SCHEMA tpcds_databricks_76_1gb_1_1622492355parquetnopart;

--Execute as azureuser
ALTER USER tpcds_user WITH DEFAULT_SCHEMA = tpcds_databricks_76_1gb_1_1622492355parquetnopart;
GRANT ALTER ON SCHEMA :: tpcds_databricks_76_1gb_1_1622492355parquetnopart TO tpcds_user;

--Execute as azureuser
CREATE EXTERNAL DATA SOURCE tpcds_datasets_parquet1gb WITH (
    LOCATION = 'wasbs://tpcds-datasets@bsctpcdsnew.blob.core.windows.net/',
    CREDENTIAL = tpcds_credential,
    TYPE = HADOOP
);

--Execute as azureuser
CREATE EXTERNAL FILE FORMAT tpcds_datasets_parquet1gb_ff
WITH
(  
    FORMAT_TYPE = PARQUET,
    DATA_COMPRESSION = 'org.apache.hadoop.io.compress.SnappyCodec'
)

--Execute as azureuser
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
    DATA_SOURCE = tpcds_datasets_parquet1gb,  
    FILE_FORMAT = tpcds_datasets_parquet1gb_ff
)
GO 


SELECT * FROM call_center


