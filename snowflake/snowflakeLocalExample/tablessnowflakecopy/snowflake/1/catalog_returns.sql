COPY INTO catalog_returns FROM '@%catalog_returns' 
FILE_FORMAT = (TYPE = CSV FIELD_DELIMITER = '\\001' ENCODING = 'ISO88591')
