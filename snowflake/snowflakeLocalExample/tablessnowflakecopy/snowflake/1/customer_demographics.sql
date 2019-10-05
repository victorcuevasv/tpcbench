COPY INTO customer_demographics FROM '@%customer_demographics' 
FILE_FORMAT = (TYPE = CSV FIELD_DELIMITER = '\\001' ENCODING = 'ISO88591')
