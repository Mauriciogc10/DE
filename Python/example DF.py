Let's go through each question with sample solutions using Python's `pandas` library.

First, set up the DataFrame:

```python
import pandas as pd
from datetime import datetime

data = {
    'EmployeeID': [1, 2, 3, 4, 5],
    'Name': ['Alice', 'Bob', 'Charlie', 'David', 'Eva'],
    'Department': ['HR', 'IT', 'IT', 'HR', 'Finance'],
    'Salary': [60000, 70000, 80000, 65000, 75000],
    'JoiningDate': ['2020-01-15', '2019-06-20', '2018-07-23', '2020-02-10', '2021-03-15'],
    'PerformanceScore': [3, 4, 2, 5, 3]
}

df = pd.DataFrame(data)
df['JoiningDate'] = pd.to_datetime(df['JoiningDate'])
```

1. **Calculate the average salary for each department:**

```python
average_salary = df.groupby('Department')['Salary'].mean()
print(average_salary)
```

2. **Function to find the employee with the highest performance score in each department:**

```python
def highest_performance_employee(df):
    return df.loc[df.groupby('Department')['PerformanceScore'].idxmax()]

highest_performance = highest_performance_employee(df)
print(highest_performance)
```

3. **Add a new column for the number of years each employee has been with the company:**

```python
df['YearsWithCompany'] = (pd.to_datetime('today') - df['JoiningDate']).dt.days // 365
print(df)
```

4. **Create a pivot table for total salary and average performance score by department:**

```python
pivot_table = df.pivot_table(values=['Salary', 'PerformanceScore'], 
                             index='Department', 
                             aggfunc={'Salary': 'sum', 'PerformanceScore': 'mean'})
print(pivot_table)
```

5. **Create a new DataFrame for IT employees with a performance score greater than 3:**

```python
it_high_perf = df[(df['Department'] == 'IT') & (df['PerformanceScore'] > 3)]
print(it_high_perf)
```

6. **Perform an inner merge with another DataFrame containing employee bonus information:**

```python
bonus_data = {
    'EmployeeID': [1, 2, 3, 4, 5],
    'Bonus': [5000, 6000, 7000, 8000, 9000]
}

bonus_df = pd.DataFrame(bonus_data)
merged_df = pd.merge(df, bonus_df, on='EmployeeID', how='inner')
print(merged_df)
```

7. **Calculate the cumulative sum of the 'PerformanceScore' column grouped by 'Department':**

```python
df['CumulativePerformanceScore'] = df.groupby('Department')['PerformanceScore'].cumsum()
print(df)
```

8. **Function to rank employees within each department based on their 'Salary':**

```python
def rank_employees_by_salary(df):
    df['SalaryRank'] = df.groupby('Department')['Salary'].rank(ascending=False)
    return df

ranked_df = rank_employees_by_salary(df)
print(ranked_df)
```

9. **Filter the DataFrame for employees with more than 2 years with the company:**

```python
more_than_2_years = df[df['YearsWithCompany'] > 2]
print(more_than_2_years)
```

These solutions use `pandas` to manipulate and analyze the data according to the specified requirements. Make sure you have the `pandas` library installed to run these examples.