Input 
data = [
    (1, "Sagar", 23, "Male", 68.0),
    (2, "Kim", 35, "Female", 90.2),
    (3, "Alex", 40, "Male", 79.1),
]
schema = "Id int,Name string,Age int,Gender string,Marks float"
df = spark.createDataFrame(data, schema)


Solution: 
from pyspark.sql.functions import col
set_of_dtypes=set(i[1] for i in df.dtypes)
for i in set_of_dtypes:
    cols=[]
    for j in df.dtypes:
        if(i==j[1]):
           cols.append(j[0])
    df.select(cols).write.mode('overwrite').save(f'/FileStore/tables/output_capegmini/{i}')     
