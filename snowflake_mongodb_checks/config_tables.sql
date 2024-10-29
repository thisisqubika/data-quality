-- Table to store the differences between MongoDB and Snowflake
CREATE OR REPLACE TABLE XXX.XXX.SNOWFLAKE_MONGO_DIFFERENCES (
    OBJECT_ID STRING,
    Value_Mongo STRING,
    Value_Snowflake STRING,
    Column_Difference STRING,
    TIMESTAMP_CHECKED TIMESTAMP_NTZ,
    SOURCE STRING
);

-- Table to store the column mappings between MongoDB and Snowflake
CREATE OR REPLACE TABLE XXX.XXX.COLLECTION_COLUMN_MAPPINGS (
    COLLECTION_NAME STRING,
    MONGO_COLUMN STRING,
    SNOWFLAKE_COLUMN STRING
);

-- Insert the mappings for vouchers
INSERT INTO XXX.XXX.COLLECTION_COLUMN_MAPPINGS 
(COLLECTION_NAME, MONGO_COLUMN, SNOWFLAKE_COLUMN)
VALUES 
('XXX', 'XXX', 'XXX');