SELECT * FROM web_sales_denorm_hudi_rt
WHERE bill_c_customer_sk = <CUSTOMER_SK> OR
ship_c_customer_sk = <CUSTOMER_SK>
