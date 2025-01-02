-----------------------------------------------------------------------------------------------------------
--               All commands/statements can be found via the link in the Video Description              --
-----------------------------------------------------------------------------------------------------------
----PRIMARY-----

-- Create Database & Schema
USE ROLE accountadmin;
CREATE DATABASE employee_db;
use employee_db;
CREATE SCHEMA employee_schema;

-- Create Tables
CREATE TABLE employee_schema.employee (
    employee_id INT PRIMARY KEY,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    email VARCHAR(100)
);

CREATE TABLE employee_schema.employee_salary (
    employee_id INT PRIMARY KEY,
    basic DECIMAL(10, 2),
    allowances DECIMAL(10, 2),
    deductions DECIMAL(10, 2)
);

-- Insert Sample Data
INSERT INTO employee_schema.employee (employee_id, first_name, last_name, email)
VALUES
    (1, 'John', 'Doe', 'john.doe@example.com'),
    (2, 'Jane', 'Smith', 'jane.smith@example.com'),
    (3, 'Michael', 'Johnson', 'michael.johnson@example.com'),
    (4, 'Emily', 'Brown', 'emily.brown@example.com'),
    (5, 'David', 'Lee', 'david.lee@example.com'),
    (6, 'Sarah', 'Clark', 'sarah.clark@example.com'),
    (7, 'Jason', 'Anderson', 'jason.anderson@example.com');

INSERT INTO employee_schema.employee_salary (employee_id, basic, allowances, deductions)
VALUES
    (1, 60000.00, 5000.00, 3000.00),
    (2, 65000.00, 5500.00, 3500.00),
    (3, 70000.00, 6000.00, 4000.00),
    (4, 55000.00, 4500.00, 2800.00),
    (5, 72000.00, 7000.00, 4500.00),
    (6, 58000.00, 4800.00, 3200.00),
    (7, 63000.00, 5200.00, 3400.00);

-----------------------------You can run ABOVE commands if you wish to set up objects for practice----------------------



ALTER DATABASE employee_db ENABLE REPLICATION TO ACCOUNTS PFFZVFJ.RS99128;         -- Multiple accounts can be added using commas


ALTER DATABASE employee_db ENABLE FAILOVER TO ACCOUNTS PFFZVFJ.RS99128;            -- Multiple accounts can be added using commas

--For Failback
ALTER DATABASE employee_db PRIMARY;


-----------------------------------------------------------------------------------------------------------
--               All commands/statements can be found via the link in the Video Description              --
-----------------------------------------------------------------------------------------------------------
-----SECONDARY----

SHOW REPLICATION DATABASES;

CREATE DATABASE employee_db
  AS REPLICA OF PFFZVFJ.TB99128.employee_db;


ALTER DATABASE employee_db REFRESH;



USE my_local_db;

CREATE TASK refresh_employee_db_task
  WAREHOUSE = my_wh
  SCHEDULE = '10 MINUTES'
AS
  ALTER DATABASE employee_db REFRESH;


--For Failover
ALTER DATABASE employee_db PRIMARY;