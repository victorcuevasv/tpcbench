SELECT * FROM catalog_sales_denorm_hudi_ro
WHERE bill_c_customer_sk = <CUSTOMER_SK> OR
ship_c_customer_sk = <CUSTOMER_SK>
