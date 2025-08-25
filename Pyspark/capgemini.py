# Input data section
# Define the data as a list of tuples, each tuple is a row
# Format: (Id, Name, Age, Gender, Marks)
data = [
    (1, "Sagar", 23, "Male", 68.0),            # Row 1: tuple with Id, Name, Age, Gender, Marks
    (2, "Kim", 35, "Female", 90.2),            # Row 2: tuple with Id, Name, Age, Gender, Marks
    (3, "Alex", 40, "Male", 79.1),             # Row 3: tuple with Id, Name, Age, Gender, Marks
]
# Define the schema for the DataFrame
schema = "Id int,Name string,Age int,Gender string,Marks float"
# Create a Spark DataFrame with the provided data and schema
df = spark.createDataFrame(data, schema)

# Import the column function from PySpark (not used in this script)
from pyspark.sql.functions import col
# Get the unique set of data types present in the DataFrame
set_of_dtypes = set(i[1] for i in df.dtypes)
# Iterate over each unique data type
for i in set_of_dtypes:
    cols = []  # Initialize an empty list to hold column names of current data type
    for j in df.dtypes:  # Loop through each column and its data type
        if i == j[1]:  # If the column data type matches the current type
            cols.append(j[0])  # Add the column name to the list
    # Save selected columns to a directory based on data type
    df.select(cols).write.mode('overwrite').save(f'/FileStore/tables/output_capegmini/{i}')
# End of script
