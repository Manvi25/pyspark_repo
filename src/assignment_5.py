from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, lit, when, lower, current_date
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder.appName('spark-assignment').getOrCreate()

# 1. create all 3 data frames as employee_df, department_df, country_df with custom schema defined in dynamic way
print(
    "1. create all 3 data frames as employee_df, department_df, country_df with custom schema defined in dynamic way ")
employee_schema = StructType([
    StructField("employee_id", IntegerType(), False),
    StructField("employee_name", StringType(), True),
    StructField("department", StringType(), True),
    StructField("State", StringType(), True),
    StructField("salary", IntegerType(), True),
    StructField("Age", IntegerType(), True)
])
employee_data = [
    (11, "james", "D101", "ny", 9000, 34),
    (12, "michel", "D101", "ny", 8900, 32),
    (13, "robert", "D102", "ca", 7900, 29),
    (14, "scott", "D103", "ca", 8000, 36),
    (15, "jen", "D102", "ny", 9500, 38),
    (16, "jeff", "D103", "uk", 9100, 35),
    (17, "maria", "D101", "ny", 7900, 40)
]

employee_df = spark.createDataFrame(employee_data, employee_schema)
employee_df.show()

department_schema = StructType([
     StructField("dept_id", StringType(), False),
     StructField("dept_name", StringType(), True)
 ])

department_data = [
    ("D101", "sales"),
  ("D102", "finance"),
("D103", "marketing"),
 ("D104", "hr"),
    ("D105", "support")
 ]

department_df = spark.createDataFrame(department_data, department_schema)
department_df.show()

country_schema = StructType([
     StructField("country_code", StringType(), True),
     StructField("country_name", StringType(), True)
])
country_data = [
   ("ny", "newyork"),
    ("ca", "California"),
   ("uk", "Russia")
 ]
country_df = spark.createDataFrame(country_data, country_schema)

# 2. Find avg salary of each department
print("2. Find avg salary of each department ")
avg_salary = employee_df.groupby("department").avg("salary").alias("Average Salary")
avg_salary.show()

# 3. Find the employee’s name and department name whose name starts with ‘m’
print("3. Find the employee’s name and department name whose name starts with ‘m’")
employee_starts_with_m = employee_df.filter(employee_df.employee_name.startswith('m'))
starts_with_m = employee_starts_with_m.join(department_df,
                                            employee_starts_with_m["department"] == department_df["dept_id"], "inner") \
  .select(employee_starts_with_m.employee_name, department_df.dept_name)
starts_with_m.show()

#4. Create another new column in  employee_df as a bonus by multiplying employee salary *2
print("4. Create another new column in  employee_df as a bonus by multiplying employee salary *2")
employee_bonus_df = employee_df.withColumn("bonus", employee_df.salary * 2)
employee_bonus_df.show()

 # 5. Reorder the column names of employee_df columns as (employee_id,employee_name,salary,State,Age,department)
employee_df = employee_df.select("employee_id", "employee_name", "salary", "State", "Age", "department")
employee_df.show()
#
# # 6. Give the result of an inner join, left join, and right join when joining employee_df with department_df in a
# # dynamic way
print("6. Give the result of an inner join, left join, and right join when joining employee_df with department_df in "
       "a dynamic way")

def dynamic_join(df1, df2, how):
    return df1.join(df2, df1.department == df2.dept_id, how)


inner_join_df = dynamic_join(employee_df, department_df, "inner")
inner_join_df.show()
left_join_df = dynamic_join(employee_df, department_df, "left")
left_join_df.show()
right_join_df = dynamic_join(employee_df, department_df, "right")
right_join_df.show()

# 7. Derive a new data frame with country_name instead of State in employee_df
print("7. Derive a new data frame with country_name instead of State in employee_df")


def update_country_name(dataframe):
    country_dataframe = dataframe.withColumn("State", when(dataframe["State"] == "uk", "United Kingdom")
                                             .when(dataframe["State"] == "ny", "New York")
                                             .when(dataframe["State"] == "ca", "Canada"))
    new_df = country_dataframe.withColumnRenamed("State", "country_name")
    return new_df


country_name_df = update_country_name(employee_df)
country_name_df.show()

# 8. convert all the column names into lowercase from the result of question 7in a dynamic way, add the load_date
# column with the current date
print("8. convert all the column names into lowercase from the result of question 7in a dynamic way, "
      "add the load_date column with the current date")


def column_to_lower(dataframe):
    for column in dataframe.columns:
        dataframe = dataframe.withColumnRenamed(column, column.lower())
    return dataframe


lower_case_column_df = column_to_lower(country_name_df)
date_df = lower_case_column_df.withColumn("load_date", current_date())
date_df.show()
