# Importing the necessary modules
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col, lit, array_construct, concat, when
from datetime import date

# Function to create a Snowpark session
def snowpark_session_create():
    connection_parameters = {
        "account": "your_account_id",
        "user": "your_username",
        "password": "your_password",
        "role": "your_role",
        "warehouse": "your_warehouse"
    }
    session = Session.builder.configs(connection_parameters).create()
    return session

# Create the Snowpark session
demo_session = snowpark_session_create()

# Set the database and schema for the session
demo_session.use_database("SNOWFLAKE_SAMPLE_DATA")
demo_session.use_schema("TPCH_SF10")

# Load a data frame using SQL query
df = demo_session.sql("SELECT * FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF10.LINEITEM LIMIT 1000")

# Show the data frame content
df.show()

# Example of collecting data
rows = df.collect()
for row in rows:
    print(row)

# Selecting specific columns and filtering data based on ship date in 1997
df_filtered = df.select(
    col("L_SHIPDATE"), col("L_EXTENDEDPRICE"), col("L_SHIPMODE"), col("L_QUANTITY"), col("L_DISCOUNT")
).filter(
    col("L_SHIPDATE").between(date(1997, 1, 1), date(1997, 12, 31))
)

df_filtered.show()

# Creating a new column for total revenue
df_with_revenue = df_filtered.with_column(
    "REVENUE",
    col("L_EXTENDEDPRICE") - (col("L_EXTENDEDPRICE") * col("L_DISCOUNT")) * col("L_QUANTITY")
)

df_with_revenue.show()

# Grouping data and aggregating the total revenue by ship mode
total_1997 = df_with_revenue.group_by(col("L_SHIPMODE")).agg(
    col("REVENUE").sum().alias("TOTAL_REVENUE")
)

total_1997.show()

# Loading data for 1998 and repeating similar steps
df2_filtered = df.select(
    col("L_SHIPDATE"), col("L_EXTENDEDPRICE"), col("L_SHIPMODE"), col("L_QUANTITY"), col("L_DISCOUNT")
).filter(
    col("L_SHIPDATE").between(date(1998, 1, 1), date(1998, 12, 31))
)

df2_with_revenue = df2_filtered.with_column(
    "REVENUE",
    col("L_EXTENDEDPRICE") - (col("L_EXTENDEDPRICE") * col("L_DISCOUNT")) * col("L_QUANTITY")
)

total_1998 = df2_with_revenue.group_by(col("L_SHIPMODE")).agg(
    col("REVENUE").sum().alias("TOTAL_REVENUE_1998")
)

# Renaming the revenue column in the 1997 dataset
total_1997_renamed = total_1997.with_column_renamed("TOTAL_REVENUE", "TOTAL_REVENUE_1997")

# Performing a join between the 1997 and 1998 datasets
joined_dfs = total_1997_renamed.join(
    total_1998, total_1997_renamed["L_SHIPMODE"] == total_1998["L_SHIPMODE"]
).select(
    total_1997_renamed.col("L_SHIPMODE").alias("L_SHIPMODE"),
    total_1997_renamed.col("TOTAL_REVENUE_1997"),
    total_1998.col("TOTAL_REVENUE_1998")
)

joined_dfs.show()

# Adding additional columns using functions
joined_dfs = joined_dfs.with_column(
    "TOTALS", array_construct(col("TOTAL_REVENUE_1997"), col("TOTAL_REVENUE_1998"))
).with_column(
    "COMMENT", concat(lit("Logic was created by "), col("CURRENT_USER"))
).with_column(
    "TRUST_LEVEL", when(col("L_SHIPMODE") == "REG AIR", 0.4).otherwise(1)
)

joined_dfs.show()

# Dropping unnecessary columns
joined_dfs = joined_dfs.drop(
    col("TOTAL_REVENUE_1997"), col("TOTAL_REVENUE_1998")
)

joined_dfs.show()

# Closing the Snowflake session
demo_session.close()