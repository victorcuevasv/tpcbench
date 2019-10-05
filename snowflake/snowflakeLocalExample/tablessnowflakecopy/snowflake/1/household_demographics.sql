COPY INTO household_demographics FROM '@%household_demographics' 
FILE_FORMAT = (TYPE = CSV FIELD_DELIMITER = '\\001' ENCODING = 'ISO88591')
