create schema gold;

CREATE VIEW gold.calendar
AS
select * from OPENROWSET(
    BULK 'https://mstoragedatalake.blob.core.windows.net/silver/AdventureWorks_Calendar/',
    FORMAT = 'PARQUET'
) q2

CREATE DATABASE SCOPED CREDENTIAL cred1
WITH
    IDENTITY = 'Managed Identity'

CREATE EXTERNAL DATA SOURCE source_silver
WITH
(
    LOCATION = 'https://mstoragedatalake.blob.core.windows.net/silver',
    CREDENTIAL = cred1
)

CREATE EXTERNAL DATA SOURCE source_gold
WITH
(
    LOCATION = 'https://mstoragedatalake.blob.core.windows.net/gold',
    CREDENTIAL = cred1
)

CREATE EXTERNAL FILE FORMAT format_parquet
WITH
(
    FORMAT_TYPE = PARQUET,
    DATA_COMPRESSION = 'org.apache.hadoop.io.compress.SnappyCodec'
)

CREATE EXTERNAL TABLE gold.extcalendar
WITH
(
    LOCATION = 'extcalendar',
    DATA_SOURCE = source_gold,
    FILE_FORMAT = format_parquet
)
AS
SELECT * FROM gold.calendar

