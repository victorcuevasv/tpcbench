DELETE FROM catalog_sales_denorm_<FORMAT>
WHERE bill_c_customer_sk = <CUSTOMER_SK> OR
ship_c_customer_sk = <CUSTOMER_SK>
