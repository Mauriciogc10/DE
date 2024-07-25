1. Write a solution to find the people who have the most friends and the most friends number.
The test cases are generated so that only one person has the most friends.
The result format is in the following example.

Example 1:
Input: 
Request Accepted table:
+--------------+-------------+-------------+
| requester_id | accepter_id | accept_date |
+--------------+-------------+-------------+
| 1      | 2      | 2016/06/03 |
| 1      | 3      | 2016/06/08 |
| 2      | 3      | 2016/06/08 |
| 3      | 4      | 2016/06/09 |
+--------------+-------------+-------------+
Output: 
+----+-----+
| id | num |
+----+-----+
| 3 | 3  |
+----+-----+

CREATE TABLE FriendRequests (
 requester_id INT,
 accepter_id INT,
 accept_date DATE
);

INSERT INTO FriendRequests (requester_id, accepter_id, accept_date) VALUES
(1, 2, '2016-06-03'),
(1, 3, '2016-06-08'),
(2, 3, '2016-06-08'),
(3, 4, '2016-06-09');


Explanation: 
The person with id 3 is a friend of people 1, 2, and 4, so he has three friends in total, which is the most number than any others. 
Follow up: In the real world, multiple people could have the same most number of friends. Could you find all these people in this case?

Solutions
MySQL
# Write your MySQL query statement below
WITH
 T AS (
 SELECT requester_id, accepter_id FROM FriendRequests
 UNION ALL
 SELECT accepter_id, requester_id FROM FriendRequests
 )
SELECT requester_id AS id, COUNT(1) AS num
FROM T
GROUP BY 1
ORDER BY 2 DESC
LIMIT 1;
------------------------------------------------------------------------------------------------------------------

Snowflake Data Engineering SQL Interview Question. Asked in Cisco for 4 - 6 years of experience.

Table: Seats
+-------------+------+
| Column Name | Type |
+-------------+------+
| seat_id | int |
| free | int |
+-------------+------+
seat_id is an auto-increment column for this table.
Each row of this table indicates whether the ith seat is free or not. 1 means free while 0 means occupied.
Find all the consecutive available seats in the cinema.
Return the result table ordered by seat_id in ascending order.
The test cases are generated so that more than two seats are consecutively available.

-- Sample data creation
CREATE TABLE seats (
 seat_id INT,
 free INT
);

INSERT INTO seats (seat_id, free) VALUES
(1, 1), (2, 0), (3, 1), (4, 1), (5, 1),
(6, 0), (7, 1), (8, 1), (9, 0), (10, 1),
(11, 0), (12, 1), (13, 0), (14, 1), (15, 1),
(16, 0), (17, 1), (18, 1), (19, 1), (20, 1);

-- SQL query to get all consecutive free seats
WITH seat_window AS (
 SELECT
 seat_id,
 free,
 LAG(free, 1) OVER (ORDER BY seat_id) AS prev_seat,
 LEAD(free, 1) OVER (ORDER BY seat_id) AS next_seat
 FROM
 seats
)
SELECT
 seat_id
FROM
 seat_window
WHERE
 (free = 1 AND (prev_seat = 1 OR next_seat = 1));
