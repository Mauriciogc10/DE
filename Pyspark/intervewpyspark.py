PROBLEM STATEMENT:

1. You have the following dataframe and table below, with marks for physics,chemistry and maths separated by dot(.) RESPECTIVELY, get the marks in respected columns using PYSPARK AND SQL 


+---+----+---+----------+
| id|name|age|   marks|
+---+----+---+----------+
| 1|  A| 20| 31.325.34|
| 2|  B| 21| 21.32.436|
| 3|  C| 22|215.324.11|
| 4|  D| 23| 10.12.12|
+---+----+---+----------+

PYSPARK-
sampleData=[(1,"A",20,"31.325.34"),
(2,"B",21,"21.32.436"),
(3,"C",22,"215.324.11"),
(4,"D",23,"10.12.12")]

schema=["id","name","age","marks"]
df=spark.createDataFrame(sampleData,schema)

from pyspark. sql.functions import *
df=df.withColumn ("physics", split(df.marks,"\.")[0])\
     •withColumn ("chemistry", split(df-marks, "\.")[1])\
       •withColumn ("maths", split(df-marks, "\.")[2])\
          •drop ("marks")
df. show(truncate=False)


SQL-

CREATE TABLE StudentScores (
 id INT,
 name VARCHAR(50),
 age INT,
 marks VARCHAR(50)
);

INSERT INTO StudentScores (id, name, age, marks)
VALUES
(1, 'A', 20, '31.325.34'),
(2, 'B', 21, '21.32.436'),
(3, 'C', 22, '215.324.11'),
(4, 'D', 23, '10.12.12');

select 1d, name, age,
    substring(marks, 1, CHARINDEX(' •',marks,1) -1) as physics,
    substring (marks,charindex('',marks)+1,charindex('.',marks,charindex('.',marks)+1)-CHARINDEX(' .',marks,1)-1) as chemistry,
    substring(marks, charindex('',marks,charindex('*',marks)+1)+1, len(marks)-charindex('.',marks,charindex('.',marks)+1)) as maths
from StudentScors