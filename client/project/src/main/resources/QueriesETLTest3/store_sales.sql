MERGE INTO store_sales_denorm
USING
(select i_item_sk from tpcds_sf3000_delta.item where i_color in
('peach','misty','drab','chocolate','almond','saddle')) s
on store_sales_denorm.ss_item_sk = s.i_item_sk
WHEN MATCHED THEN UPDATE SET
ss_cdemo_sk = ss_cdemo_sk + 1;

