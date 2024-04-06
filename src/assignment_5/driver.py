from pyspark_repo.src.assignment_5.utils import *

spark = spark_session()

# Create data frames with custom schema
employee_df = create_df(spark, employee_schema, employee_data)
department_df = create_df(spark, department_schema, department_data)
country_df = create_df(spark, country_schema, country_data)

# Find average salary of each department
avg_salary = find_avg_salary_employee(employee_df)
avg_salary.show()

# Find employees whose name starts with 'm' and their department names
employees_starts_with_m = find_employee_name_starts_with_m(employee_df, department_df)
employees_starts_with_m.show()

# Create a new column in employee_df as bonus by multiplying salary * 2
employee_bonus_df = add_bonus_times_2(employee_df)
employee_bonus_df.show()

# Reorder the columns of employee_df
rearranged_employee_df = rearrange_columns_employee_df(employee_df)
rearranged_employee_df.show()

# Perform inner join, left join, and right join dynamically
inner_join_result = dynamic_join(employee_df, department_df, "inner")
inner_join_result.show()
left_join_result = dynamic_join(employee_df, department_df, "left")
left_join_result.show()
right_join_result = dynamic_join(employee_df, department_df, "right")
right_join_result.show()

# Derive a new data frame with country_name instead of State in employee_df
updated_employee_df = update_country_name(employee_df)
updated_employee_df.show()

# Convert all column names into lowercase and add load_date column with current date
lower_case_column_df = column_to_lower(updated_employee_df)
date_df = lower_case_column_df.withColumn("load_date", current_date())
date_df.show()