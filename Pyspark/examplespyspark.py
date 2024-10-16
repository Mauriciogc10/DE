# Define schema for tbl_maxval
schema = StructType([
StructField("col1", String Type(), True), StructField("col2", IntegerType(), True),
StructField("col3", Integer Type(), True)
])
# Insert records into tbl_maxval
data = [
('a', 10, 20), ('b', 50, 30)
]
# Create a DataFrame from the data and schema
df = spark.createDataFrame(data, schema)

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType,
IntegerType
from pyspark.sql. functions import col, max
# Initialize Spark session
spark = SparkSession.builder \
•appName ("Create Table tbl
maxval") \
• getOrCreate()
# Define the schema for the tbl maxval table
schema = StructType([
  StructField ("col1", IntegerType(), True),
  StructField("col2", IntegerType(), True),
  StructField ("col3", IntegerType(), True),
])
# Insert records into tbl_maxval
data = [('a', 10, 20), ('b', 50, 30)]
# Create a DataFrame from the data and schema
df = spark.createDataFrame (data, schema)
# Show the DataFrame contents
df. display ()
# Optionally, register the DataFrame as a temporary view to run SQL
queries df. createOrReplaceTempView("tbl_maxval")
# Example SQL query (optional)
spark.sql ("SELECT * FROM tbl_maxval"). display()
%sql
select coll, col2
Trom tol _maxval
union all select coll, col3 from tbl _maxval