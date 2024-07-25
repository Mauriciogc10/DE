➡ Problem Statement: Retrieve information about consecutive login streaks for users who have logged in for at least two consecutive days. For each user, provide the UserID, the number of consecutive days logged in (ConsecutiveDays), the start date of the streak (StartDate), and the end date of the streak (EndDate). 

➡Step by step solution: Refer the first comment, but first try it yourself. If you like the solution and would like to have more interview questions, please subscribe to the channel.

Scripts to create tables and insert the data:

CREATE TABLE AttendanceLogs (
 UserID INT,
 LogDate DATE,
 LoggedIn CHAR
);

INSERT INTO AttendanceLogs (LoggedIn, LogDate, UserID)
VALUES
 ('Y', '2023-01-01', 101),
 ('N', '2023-01-01', 102),
 ('N', '2023-01-01', 103),
 ('Y', '2023-01-01', 104),
 ('Y', '2023-01-01', 105),
 ('N', '2023-01-02', 101),
 ('Y', '2023-01-02', 102),
 ('N', '2023-01-02', 103),
 ('Y', '2023-01-02', 104),
 ('N', '2023-01-02', 105),
 ('Y', '2023-01-03', 101),
 ('Y', '2023-01-03', 102),
 ('N', '2023-01-03', 103),
 ('Y', '2023-01-03', 104),
 ('N', '2023-01-03', 105),
 ('N', '2023-01-04', 101),
 ('N', '2023-01-04', 102),
 ('N', '2023-01-04', 103),
 ('Y', '2023-01-04', 104),
 ('Y', '2023-01-04', 105),
 ('Y', '2023-01-05', 101),
 ('Y', '2023-01-05', 102),
 ('Y', '2023-01-05', 103),
 ('N', '2023-01-05', 104),
 ('N', '2023-01-05', 105),
 ('N', '2023-01-06', 101),
 ('Y', '2023-01-06', 102),
 ('Y', '2023-01-06', 103),
 ('Y', '2023-01-06', 104),
 ('N', '2023-01-06', 105),
 ('N', '2023-01-07', 101),
 ('Y', '2023-01-07', 102),
 ('N', '2023-01-07', 103),
 ('N', '2023-01-07', 104),
 ('Y', '2023-01-07', 105);

----------------------------
WITH NumberedLogs AS (
    SELECT
        UserID,
        LogDate,
        ROW_NUMBER() OVER (PARTITION BY UserID ORDER BY LogDate) AS RowNum
    FROM
        AttendanceLogs
    WHERE
        LoggedIn = 'Y'
),
Streaks AS (
    SELECT
        UserID,
        LogDate,
        RowNum - CAST(LogDate AS INT) AS GroupNum
    FROM
        NumberedLogs
),
ConsecutiveDays AS (
    SELECT
        UserID,
        MIN(LogDate) AS StartDate,
        MAX(LogDate) AS EndDate,
        COUNT(*) AS ConsecutiveDays
    FROM
        Streaks
    GROUP BY
        UserID, GroupNum
    HAVING
        COUNT(*) >= 2
)
SELECT
    UserID,
    ConsecutiveDays,
    StartDate,
    EndDate
FROM
    ConsecutiveDays;
