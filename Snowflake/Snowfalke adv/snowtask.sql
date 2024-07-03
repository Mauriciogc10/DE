//Currently, a task can execute a single SQL statement, including a call to a stored procedure.

//In summary tasks are very handy in Snowflake, they can be combined with streams, snowpipe and other techniques to make them extremely powerful.


--drop database if required
drop database if exists ramu;
--Create Database
create database if not exists ramu;
--use the database
use ramu;



// Prepare table
CREATE OR REPLACE TABLE video_demo (
    ID INT AUTOINCREMENT START = 1 INCREMENT =1,
    NAME VARCHAR(40) DEFAULT 'DemoYoutube' ,
    CREATE_DATE timestamp);
    
    
// Create task
CREATE OR REPLACE TASK INSERT_Data_with_one_min_interval
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = '1 MINUTE'
    AS 
    INSERT INTO video_demo(CREATE_DATE) VALUES(CURRENT_TIMESTAMP);
    

SHOW TASKS;

// Task starting and suspending
ALTER TASK INSERT_Data_with_one_min_interval RESUME;
ALTER TASK INSERT_Data_with_one_min_interval SUSPEND;


SELECT * FROM video_demo;