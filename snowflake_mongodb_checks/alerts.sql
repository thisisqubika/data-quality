CREATE OR REPLACE ALERT XXX.XXX.SNOWFLAKE_MONGO_CHECKS_VOUCHERS_ALERT
WAREHOUSE = 'DQ_XS_WH'
SCHEDULE = 'USING CRON 30 9 * * * America/Montevideo'
IF (
    EXISTS(
        SELECT TIMESTAMP_CHECKED, SOURCE
        FROM XXX.XXX.SNOWFLAKE_MONGO_DIFFERENCES
        WHERE SOURCE = 'XXX' AND TIMESTAMP_CHECKED BETWEEN SNOWFLAKE.ALERT.LAST_SUCCESSFUL_SCHEDULED_TIME() AND SNOWFLAKE.ALERT.SCHEDULED_TIME()
        GROUP BY TIMESTAMP_CHECKED, SOURCE
    )
)
THEN
CALL SYSTEM$SEND_EMAIL( 
'my_email_int', 
'XXX', 
'DQ issues', 
CONCAT('Differences have been identified between Snowflake and the Mongo vouchers collection, with ', 
(SELECT count(*)
FROM XXX.XXX.SNOWFLAKE_MONGO_DIFFERENCES
WHERE (source, timestamp_checked) IN (
    SELECT source, MAX(timestamp_checked) AS max_timestamp
    FROM XXX.XXX.SNOWFLAKE_MONGO_DIFFERENCES
    WHERE source = 'XXX'
    GROUP BY source
)),' cases showing inconsistent information. For more details, please review the XXX.XXX.SNOWFLAKE_MONGO_DIFFERENCES table.'));