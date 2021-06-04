--Execute as azureuser
CREATE MASTER KEY ENCRYPTION BY PASSWORD = 'M...F9'

--Execute as azureuser
--For dedicated pool use only the key
CREATE DATABASE SCOPED CREDENTIAL [tpcds_credential]
WITH IDENTITY='SHARED ACCESS SIGNATURE',  
SECRET = 'odoo...yg=='
GO

--Execute as azureuser
--For serverless, use sas token with permissions to read and list
CREATE DATABASE SCOPED CREDENTIAL [tpcds_credential]
WITH IDENTITY='SHARED ACCESS SIGNATURE',  
SECRET = '?sv=2020-02-10&ss=bfqt&srt=...sWiyw15odpToGs8EPbFk%3D'
GO
