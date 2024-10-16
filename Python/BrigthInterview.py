Part 1: SQL test
##################################################################################################
Product (product_id, product_name, product_type, product_version, product_price)
Customer (customer_id, customer_first_name, customer_last_name, phone_number )
Sales (transaction_id, customer_id, product_id, timestamp, total_amount, total_quantity )
Refund (refund_id, original_transaction_id, customer_id, product_id, timestamp, refund_amount, refund_quantity)
 

1. Calculate the total amount of all transactions that happened in year 2013 and have not been refunded as of today.

SELECT SUM(s.total_amount) AS total_amount_2013
FROM Sales s
LEFT JOIN Refund r ON s.transaction_id = r.original_transaction_id
WHERE YEAR(s.timestamp) = 2013
  AND r.original_transaction_id IS NULL;

2. Display the customer name who made the second most purchases in the month of May 2013. Refunds should be excluded.

WITH CustomerPurchases AS (
    SELECT 
        s.customer_id, 
        COUNT(s.transaction_id) AS purchase_count
    FROM Sales s
    LEFT JOIN Refund r ON s.transaction_id = r.original_transaction_id
    WHERE YEAR(s.timestamp) = 2013
      AND MONTH(s.timestamp) = 5
      AND r.original_transaction_id IS NULL
    GROUP BY s.customer_id
)
SELECT 
    c.customer_first_name, 
    c.customer_last_name
FROM CustomerPurchases cp
JOIN Customer c ON cp.customer_id = c.customer_id
ORDER BY cp.purchase_count DESC
LIMIT 1 OFFSET 1;

3. Find a product that has not been sold at least once (if any).

SELECT p.product_id, p.product_name
FROM Product p
LEFT JOIN Sales s ON p.product_id = s.product_id
WHERE s.product_id IS NULL;


Part 2: Python test
##################################################################################################
application_id  Interview_date  Status  gender  Department  Title Offered_Salary
383422  2014-05-01 11:40:00 Hired   Male    Service Department  c8  56553.0
907518  2014-05-06 08:08:00 Hired   Female  Service Department  c5  22075.0
176719  2014-05-06 08:08:00 Rejected    Male    Service Department  c5  70069.0
429799  2014-05-02 16:28:00 Rejected    Female  Operations Department   i4  3207.0
253651  2014-05-02 16:32:00 Hired   Male    Operations Department   i4  29668.0
493131  2014-08-28 17:32:00 Hired   Male    Service Department  c9  49282.0
214261  2014-08-31 01:36:00 Hired   Female  Service Department  c5  57742.0
932441  2014-08-31 01:37:00 Hired   Male    Service Department  c5  69932.0
39010   2014-08-31 01:38:00 Rejected    Male    Service Department  c5  14489.0
686055  2014-08-26 12:14:00 Hired   Male    Operations Department   c5  54201.0
 
1. average salary of the Female vs Male
2. Number of people with less than average salary of all hires
using Pyspark

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg

# Initialize a Spark session
spark = SparkSession.builder.appName("SalaryAnalysis").getOrCreate()

# Data
data = [
    (383422, '2014-05-01 11:40:00', 'Hired', 'Male', 'Service Department', 'c8', 56553.0),
    (907518, '2014-05-06 08:08:00', 'Hired', 'Female', 'Service Department', 'c5', 22075.0),
    (176719, '2014-05-06 08:08:00', 'Rejected', 'Male', 'Service Department', 'c5', 70069.0),
    (429799, '2014-05-02 16:28:00', 'Rejected', 'Female', 'Operations Department', 'i4', 3207.0),
    (253651, '2014-05-02 16:32:00', 'Hired', 'Male', 'Operations Department', 'i4', 29668.0),
    (493131, '2014-08-28 17:32:00', 'Hired', 'Male', 'Service Department', 'c9', 49282.0),
    (214261, '2014-08-31 01:36:00', 'Hired', 'Female', 'Service Department', 'c5', 57742.0),
    (932441, '2014-08-31 01:37:00', 'Hired', 'Male', 'Service Department', 'c5', 69932.0),
    (39010, '2014-08-31 01:38:00', 'Rejected', 'Male', 'Service Department', 'c5', 14489.0),
    (686055, '2014-08-26 12:14:00', 'Hired', 'Male', 'Operations Department', 'c5', 54201.0)
]

columns = ["application_id", "Interview_date", "Status", "gender", "Department", "Title", "Offered_Salary"]

# Create DataFrame
df = spark.createDataFrame(data, columns)

# Filter out only the hired employees
hired_df = df.filter(col("Status") == "Hired")

# Calculate the average salary by gender
avg_salary_by_gender = hired_df.groupBy("gender").agg(avg("Offered_Salary").alias("avg_salary"))
avg_salary_by_gender.show()

# Calculate the overall average salary for all hires
overall_avg_salary = hired_df.agg(avg("Offered_Salary").alias("avg_salary")).collect()[0]["avg_salary"]
print(f"Overall average salary: {overall_avg_salary}")

# Count the number of people with less than the overall average salary
less_than_avg_salary_count = hired_df.filter(col("Offered_Salary") < overall_avg_salary).count()
print(f"Number of people with less than average salary: {less_than_avg_salary_count}")
