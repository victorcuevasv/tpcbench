COPY INTO catalog_page FROM '@%catalog_page' 
FILE_FORMAT = (TYPE = CSV FIELD_DELIMITER = '\\001' ENCODING = 'ISO88591')
