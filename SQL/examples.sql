Problem:Several friends at a cinema ticket would like to reserve consecutive availableseats.
Can you help to query all the consecutive available seats order bt the seat_id using the following cinema table?
Solution:
select * from cinema;
with cte as(
select seat_id , row_number() over ( order by seat_id) as rn , (seat_id - row_number() over ( order by seat_id)) as diff
from cinema where free = 1
), cte2 as(
select seat_id , count(seat_id) over (partition by diff) as grp
from cte 
)
select seat_id from cte2 where grp >1

𝐋𝐞𝐞𝐭𝐜𝐨𝐝𝐞 : 𝟑𝟐𝟑𝟔 : 𝐂𝐞𝐨 𝐒𝐮𝐛𝐨𝐫𝐝𝐢𝐧𝐚𝐭𝐞 𝐇𝐢𝐞𝐫𝐚𝐫𝐜𝐡𝐲

-- Create table statement
CREATE TABLE employees (
 employee_id INT PRIMARY KEY,
 employee_name VARCHAR(50),
 manager_id INT,
 salary INT,
 FOREIGN KEY (manager_id) REFERENCES employees(employee_id)
);

-- Insert statements
INSERT INTO employees (employee_id, employee_name, manager_id, salary) VALUES
(1, 'Alice', NULL, 150000),
(2, 'Bob', 1, 120000),
(3, 'Charlie', 1, 110000),
(4, 'David', 2, 105000),
(5, 'Eve', 2, 100000),
(6, 'Frank', 3, 95000),
(7, 'Grace', 3, 98000),
(8, 'Helen', 5, 90000);

with recursive cte as (
    select employee_id, employee_name, manager_id, salary, 0 as hierarchy_level from employees
    where employee_id = 1
    union all
    select ti.employee_id, t1.employee_name, t1.manager_id, t1. salary , hierarchy_level + 1 as hierarchy_level 
    from employees as t1
    inner join cte as t2
    on t1. manager_id = t2.employee_id
--where t1. employee_id in (1,2, 3,4,5,6,7,8)
),manager_salary as (
select salary as boss_salary from employees where manager_id is null
)
select employee_id as subordinate_id, employee_name as subordinate_ name , 
hierarchy_level, (salary - boss_salary) as salary_difference 
from cte as t1
join manager_salary as t2
on hierarchy_level != 0
order by hierarchy_level,subordinate_id ;


with rec_cte (EMPLOYEE_ID, EMPLOYEE_NAME, MANAGER_ID, salary, lvl, salary_mgr) as
(
  select EMPLOYEE_ID, EMPLOYEE_NAME, MANAGER_ID, salary, 0 as lvl, salary from employees 
    where manager_id is null
union all
  Select e. EMPLOYEE_ID, e. EMPLOYEE_NAME, e.MANAGER_ID, e.salary, lvl + 1 as lvl, c.salary_mgn from rec_cte c 
  join employees e
  on e.manager_id = c.EMPLOYEE_ID
)
select EMPLOYEE_ID as subordinate_id, EMPLOYEE_NAME as Subordinate_name
, LVL as hierarchy_level, SALARY- SALARY_MGR as salary_diff from rec_cte
where MANAGER_ID is not null

CREATE TABLE events (
ID int,
event varchar(255),
YEAR INt,
GOLD varchar(255),
SILVER varchar(255),
BRONZE varchar(255)
);

delete from events;

INSERT INTO events VALUES (1,'100m',2016, 'Amthhew Mcgarray','donald','barbara');
INSERT INTO events VALUES (2,'200m',2016, 'Nichole','Alvaro Eaton','janet Smith');
INSERT INTO events VALUES (3,'500m',2016, 'Charles','Nichole','Susana');
INSERT INTO events VALUES (4,'100m',2016, 'Ronald','maria','paula');
INSERT INTO events VALUES (5,'200m',2016, 'Alfred','carol','Steven');
INSERT INTO events VALUES (6,'500m',2016, 'Nichole','Alfred','Brandon');
INSERT INTO events VALUES (7,'100m',2016, 'Charles','Dennis','Susana');
INSERT INTO events VALUES (8,'200m',2016, 'Thomas','Dawn','catherine');
INSERT INTO events VALUES (9,'500m',2016, 'Thomas','Dennis','paula');
INSERT INTO events VALUES (10,'100m',2016, 'Charles','Dennis','Susana');
INSERT INTO events VALUES (11,'200m',2016, 'jessica','Donald','Stefeney');
INSERT INTO events VALUES (12,'500m',2016,'Thomas','Steven','Catherine');

SELECT * FROM events
--Write a Query to find Number of gold medals per swimmer for swimmer who has won only gold medals
SELECT e1. GOLD as player, count(e1. GOLD) as No_of_gold
FROM events el
JOIN events e2
ON e1. id = e2. ID
WHERE el. GOLD NOT IN ( SELECT SILVER FROM events UNION ALL SELECT BRONZE FROM events)
GROUP BY e1. GOLD