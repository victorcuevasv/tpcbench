--Execute as azureuser
CREATE DATABASE SCOPED CREDENTIAL [tpcds_credential]
WITH IDENTITY='SHARED ACCESS SIGNATURE',  
SECRET = 'odoo...yg=='
GO
