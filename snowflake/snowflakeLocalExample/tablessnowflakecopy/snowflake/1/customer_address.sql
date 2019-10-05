COPY INTO customer_address FROM '@%customer_address' 
FILE_FORMAT = (TYPE = CSV FIELD_DELIMITER = '\\001' ENCODING = 'ISO88591')
