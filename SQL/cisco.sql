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

SELECT s1.seat_id
FROM seats s1
LEFT JOIN seats s2
ON s1.seat_id = s2.seat_id - 1
LEFT JOIN seats s3
ON s1.seat_id = s3.seat_id + 1
WHERE s1.free - s2.free = 0 OR s1.free - s3.free = 0