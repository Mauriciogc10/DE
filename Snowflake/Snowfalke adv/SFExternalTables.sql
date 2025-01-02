--  External Tables, Iceberg Tables, Dynamic tables, Hybrid Tables, Event Tables, Directory Tables  -- 
--             All commands/statements can be found via the link in the video description           --

-- <<<<<<<<<<<<< External Tables >>>>>>>>>>>>> --

CREATE OR REPLACE STAGE oms_ext_stg
  URL='s3://s3explore/'
  CREDENTIALS=(AWS_KEY_ID='SDFEDU5ADSAF34YEDASDF0' AWS_SECRET_KEY='hxR5q/k2Gp8l3vTnDUkk6jDWH9EAmGk4VZcOy7')
--You can refer to our previous videos to learn more about creating stages.

CREATE OR REPLACE FILE FORMAT my_csv_format
    TYPE = 'CSV'
    SKIP_HEADER = 1;

CREATE OR REPLACE EXTERNAL TABLE my_external_table (
    employeeid VARCHAR AS (VALUE:c1::VARCHAR),
    firstname VARCHAR AS (VALUE:c2::VARCHAR),
    lastname VARCHAR AS (VALUE:c3::VARCHAR),
    email VARCHAR AS (VALUE:c4::VARCHAR)
)
WITH LOCATION = @oms_ext_stg/employees
FILE_FORMAT = (FORMAT_NAME = my_csv_format);

select  METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, Value, employeeid, firstname, lastname, email from my_external_table;

-- Alternative for external tables
SELECT METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, $1, $2, $3, $4
FROM @oms_ext_stg/employees
(FILE_FORMAT => my_csv_format);

-- Note: You cannot create an external table on top of internal stages; however, you can directly query internal stages as above
-- Advantages of External tables over direct stage queries:
    -- Performance: External tables are faster than direct stage queries due to the metadata they collect and store.
    -- Management: Easier to manage and automate refreshes to reflect changes in the data.
    -- Functionality: Supports advanced features like partitioning and materialized views for further optimization.
    -- Security: Leverages Snowflake's robust table access control mechanisms for enhanced data security.


-- <<<<<<<<<<<<< Iceberg Tables >>>>>>>>>>>>> --

-- Create an External Volume to hold Parquet and Iceberg data
create or replace external volume my_ext_vol
STORAGE_LOCATIONS = 
(
    (
        NAME = 'my-s3-us-east-1'
        STORAGE_PROVIDER = 'S3'
        STORAGE_BASE_URL = 's3://my-s3-bucket/data/snowflake_extvol/'
        STORAGE_AWS_ROLE_ARN = '****'
    )
); 

-- Create an Iceberg Table using my External Volume
create or replace iceberg table my_iceberg_table
    with EXTERNAL_VOLUME = 'my_ext_vol'
    as select id, date, first_name, last_name, address, region, order_number, invoice_amount from sales;
    

-- <<<<<<<<<<<<< Dynamic Tables >>>>>>>>>>>>> --

-- WITHOUT DYNAMIC TABLE -- [Source > Transform > Target]
-- Source Table Creation
-- Please assume frequent data loading into the source table by another process.
CREATE OR REPLACE TABLE raw
(var VARIANT);

-- Target Table Creation
CREATE OR REPLACE TABLE names
(id INT,
first_name STRING,
last_name STRING);

-- Stream Creation
CREATE OR REPLACE STREAM rawstream1
ON TABLE raw;

-- Task Creation
CREATE OR REPLACE TASK raw_to_names
WAREHOUSE = mywh
SCHEDULE = '1 minute'
WHEN
SYSTEM$STREAM_HAS_DATA('rawstream1')
AS
MERGE INTO names n
USING (
SELECT var:id id, var:fname fname,
var:lname lname FROM rawstream1
) r1 ON n.id = TO_NUMBER(r1.id)
WHEN MATCHED AND metadata$action = 'DELETE' THEN
DELETE
WHEN MATCHED AND metadata$action = 'INSERT' THEN
UPDATE SET n.first_name = r1.fname, n.last_name = r1.lname
WHEN NOT MATCHED AND metadata$action = 'INSERT' THEN
INSERT (id, first_name, last_name)
VALUES (r1.id, r1.fname, r1.lname);

-- WITH DYNAMIC TABLE -- [Source > Transform > Target]
-- Source Table Creation
-- Please assume frequent data loading into the source table by another process.
CREATE OR REPLACE TABLE raw
(var VARIANT);

-- Dynamic Target Table Creation
CREATE OR REPLACE DYNAMIC TABLE names
TARGET_LAG = '1 minute'
WAREHOUSE = mywh
AS
SELECT var:id::int id, var:fname::string first_name,
var:lname::string last_name FROM raw;

-- <<<<<<<<<<<<< Hybrid Tables >>>>>>>>>>>>> --

-- Create hybrid table for Product Inventory
CREATE OR REPLACE HYBRID TABLE ProductInventory (
  product_id INT PRIMARY KEY,
  product_name VARCHAR(100) NOT NULL,
  category VARCHAR(50) NOT NULL,
  quantity INT NOT NULL,
  price DECIMAL(10, 2) NOT NULL
);

-- Insert data into the Product Inventory table
INSERT INTO ProductInventory VALUES(1001, 'Laptop', 'Electronics', 10, 999.99);
INSERT INTO ProductInventory VALUES(1002, 'Smartphone', 'Electronics', 20, 599.99);
INSERT INTO ProductInventory VALUES(1003, 'Printer', 'Office Supplies', 5, 299.99);
INSERT INTO ProductInventory VALUES(1004, 'Chair', 'Furniture', 30, 49.99);

-- Update quantity of a product
UPDATE ProductInventory SET quantity = 25 WHERE product_id = 1002;

-- Delete a product from inventory
DELETE FROM ProductInventory WHERE product_id = 1004;

-- Note: Hybrid tables are currently not available to trial accounts.


-- <<<<<<<<<<<<< Event Tables >>>>>>>>>>>>> --

-- Create Event Table
CREATE OR REPLACE EVENT TABLE SLEEK_OMS.L1_LANDING.oms_event_table;

-- Associate Event Table with Account
ALTER ACCOUNT SET EVENT_TABLE = SLEEK_OMS.L1_LANDING.oms_event_table;

-- Push logs (info,warning, errors) / events
ALTER SESSION SET LOG_LEVEL = INFO;
CREATE OR REPLACE FUNCTION log_trace_data()
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = 3.8
HANDLER = 'run'
AS $$
import logging
logger = logging.getLogger("tutorial_logger")

def run():
  logger.info("Logging from Python function.")
  return "SUCCESS"
$$;
SELECT log_trace_data();

-- Display/Verify the contents of the Event table (litle delay/lag expected)
select * from SLEEK_OMS.L1_LANDING.oms_event_table;


-- <<<<<<<<<<<<< Directory Tables >>>>>>>>>>>>> -- 
-- TO enable Directory Table for a stage
CREATE OR REPLACE STAGE my_int_stg
DIRECTORY = (ENABLE = TRUE);
-- OR
ALTER STAGE my_int_stg SET DIRECTORY = ( ENABLE = TRUE );


-- PUT needs to be executed in SnowSQL. If you haven't set SnowSQL up yet, please refer to the initial videos in this playlist.
PUT 'file://C:/Users/mamba/Desktop/csvfiles/dates.csv' @my_int_stg;
PUT 'file://C:/Users/mamba/Desktop/csvfiles/customers.csv' @my_int_stg;
PUT 'file://C:/Users/mamba/Desktop/csvfiles/employees.csv' @my_int_stg;
PUT 'file://C:/Users/mamba/Desktop/csvfiles/stores.csv' @my_int_stg;

-- TO REFRESH directory table metadata
ALTER STAGE my_int_stg REFRESH;

-- To query directory table contents
SELECT * FROM DIRECTORY( @my_int_stg )