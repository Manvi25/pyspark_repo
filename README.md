### **Question_1**
1. imported necessary modules and establishing a SparkSession for Spark interaction.
2. Define schemas for the purchase and product data, and create corresponding DataFrames.
3. Display the purchase and product data to inspect their contents.
4. Filter the purchase data to identify customers who bought only the iPhone 13.
5. Perform an inner join operation to find customers who upgraded from iPhone 13 to iPhone 14.
6. Group the purchase data by customer and count the distinct product models.
7. Filter out customers who bought all models present in the new product data.
8. Display the resulting DataFrame to showcase customers who bought all models.

### **Question_2**
1. Initialize a SparkSession to facilitate interactions with Spark.
2. Define a dataset as a list of tuples containing credit card numbers.
3. Specify a schema using StructType and StructField to structure the dataset.
4. Create a DataFrame named `credit_card_df` using the defined dataset and schema.
5. Read the CSV file named "card_number.csv" while automatically inferring the schema.
6. Generate another DataFrame named `credit_card_df1` from the CSV file with the inferred schema.
7. Create an additional DataFrame named `credit_card_df_schema` with a custom schema applied to the same CSV file.
8. Implement partition management to dynamically adjust the number of partitions as needed.

### **Question_3**
1. Define user activity data as a list of tuples.
2. Structure the DataFrame schema using StructType and StructField based on the provided data.
3. Create a DataFrame using the defined schema and provided data.
4. Define a function `rename_columns` to rename columns in the DataFrame.
5. Utilize the `rename_columns` function to rename DataFrame columns accordingly.
6. Convert the timestamp column to TimestampType using withColumn and cast.
7. Filter the data to include only entries within the last 7 days using datediff and filter.
8. Perform grouping and counting actions to calculate the number of actions performed by each user within the specified period.

### **Question_4**
1. Import necessary PySpark modules and initialize a Spark session.
2. Define a custom schema to match the JSON structure of the data.
3. Create a function to read a JSON file with the provided schema.
4. Read the JSON file into a DataFrame using the defined function.
5. Flatten the DataFrame by selecting necessary columns and exploding the employees array.
6. Count the records before and after flattening to observe the difference.
7. Apply explode and posexplode functions to the DataFrame to understand their effects.
8. Filter the DataFrame to find records with a specific empId.
9. Convert column names to snake case using a custom function.
10. Add a new column with the current date and extract year, month, and day from it.
11. Display the DataFrame after each transformation to observe changes.

### **Question_5**
1. Initialize a Spark session and define custom schemas for employee_df, department_df, and country_df.
2. Create DataFrames employee_df, department_df, and country_df with the defined schemas and sample data.
3. Group employee_df by department and calculate the average salary for each department.
4. Filter employee_df to retrieve employee names starting with 'm' and join with department_df to get corresponding department names.
5. Create a new column 'bonus' in employee_df by multiplying 'salary' by 2.
6. Reorder columns in employee_df to match the desired order of column names.
7. Define a function `dynamic_join` to perform inner, left, and right joins between employee_df and department_df based on the specified join type.
8. Update the 'State' column in employee_df with corresponding country names and rename it to 'country_name'.
9. Define a function `column_to_lower` to convert column names to lowercase and apply it to the DataFrame.
10. Add a new column 'load_date' with the current date to the DataFrame.