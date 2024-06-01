

Table: Weather
+---------------+---------+
| Column Name | Type |
+---------------+---------+
| id | int |
| recordDate | date |
| temperature | int |
+---------------+---------+
id is the column with unique values for this table.
This table contains information about the temperature on a certain day.
 
Write a solution to find all dates' Id with higher temperatures compared to its previous dates (yesterday).
Return the result table in any order.
The result format is in the following example.
 
Example 1:
Input:
Weather table:
+----+------------+-------------+
| id | recordDate | temperature |
+----+------------+-------------+
| 1 | 2015-01-01 | 10 |
| 2 | 2015-01-02 | 25 |
| 3 | 2015-01-03 | 20 |
| 4 | 2015-01-04 | 30 |
+----+------------+-------------+
Output:
+----+
| id |
+----+
| 2 |
| 4 |
+----+
Explanation:
In 2015-01-02, the temperature was higher than the previous day (10 -> 25).
In 2015-01-04, the temperature was higher than the previous day (20 -> 30).

df.withColumn("lastmax",coalesce(max("temperature").over(Window.partitionBy(lit(0)). \
 orderBy("id").rangeBetween(Window.unboundedPreceding,-1)),col("temperature"))). \
 where("temperature > lastmax"). \
 select("id"). \
 show()

############################
Picture this: A data analysis project, and you need to summarize vast datasets quickly and efficiently. 
PySpark's aggregation functions are your secret sauce, helping you distill valuable insights from mountains of data.

🔑 Step 1️⃣: The Data Landscape
A dataset filled with e-commerce transactions, a goldmine of information. Your mission? Summarize sales data by product and category. PySpark's aggregation functions, like "sum" and "avg," will be your guiding light.

🛠️ Step 2️⃣: PySpark Sorcery

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
spark = SparkSession.builder.appName("DataSummarization").getOrCreate()

# Sample sales data
data = [("Product A", "Category X", 100),
("Product B", "Category Y", 150),
("Product A", "Category X", 200),
("Product B", "Category Y", 250),
("Product A", "Category Z", 300)]

# Create DataFrame
columns = ["product", "category", "sales"]
df = spark.createDataFrame(data, columns)

# Use PySpark's aggregation functions
df_summary = df.groupBy("product", "category").agg(F.sum("sales").alias("total_sales"))
df_summary.show(truncate=False)

PySpark's aggregation functions enable you to perform data summarization effortlessly. You can crunch numbers, calculate totals, averages, and more, transforming raw data into actionable insights.