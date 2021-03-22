MERGE INTO store_sales_denorm_<FORMAT> AS a
USING (SELECT * FROM store_sales_denorm_<FORMAT> WHERE c_customer_sk = <CUSTOMER_SK>) AS b
ON a.c_customer_sk = b.c_customer_sk
WHEN MATCHED THEN DELETE
