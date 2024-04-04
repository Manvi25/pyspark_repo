from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, explode_outer,posexplode, current_date, year, month, day
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, ArrayType, LongType

spark = SparkSession.builder.appName('nested_json_file').getOrCreate()
# Define the schema for the main JSON structure
json_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("properties", StructType([
        StructField("name", StringType(), True),
        StructField("storeSize", StringType(), True)
    ]), True),
    StructField("employees", ArrayType(
        StructType([
            StructField("empId", LongType(), True),
            StructField("empName", StringType(), True)
        ])
    ), True)
])

json_path = "../resource/nested_json_file.json"


def read_json(path, schema):
    json_df = spark.read.json(path, multiLine=True, schema=schema)
    return json_df

#read json file provided in the attachment using the dynamic function
print("1. Read JSON file provided in the attachment using the dynamic function")
json_df = read_json(json_path, json_schema)
json_df.printSchema()
json_df.show(truncate=False)

#flatten the dataframe which is a custom schema
print(" flatten the data frame which is a custom schema ")
flatten_json_df = json_df.select("*", explode("employees").alias("employee")) \
    .select("*", "employee.empId", "employee.empName") \
    .select("*", "properties.name", "properties.storeSize").drop("properties", "employees", "employee")
flatten_json_df.show()

#find out the record count when flattened and when it's not flattened
# (find out the difference why you are getting more count)
print("find out the record count when flattened and when it's not flattened(find out the difference why you are getting more count) ")
print("Before flatten:", json_df.count())
print(("After Flatten: ", flatten_json_df.count()))

#Differentiate the difference using explode, explode outer, posexplode functions
exploded_df = json_df.select("id","properties", explode("employees").alias("employee"))
print("Exploded DataFrame:")
exploded_df.show()

# Explode outer the 'numbers' array column
exploded_outer_df = json_df.select("id","properties", explode_outer("employees").alias("employee"))
print("Exploded Outer DataFrame:")
exploded_outer_df.show()

# PosExplode the 'numbers' array column
pos_exploded_df = json_df.select("id","properties", posexplode("employees").alias("pos", "number"))
print("PosExploded DataFrame:")
pos_exploded_df.show()

#Filter the id which is equal to 1001
print("5. Filter the id which is equal to 1001")
filter_df = flatten_json_df.filter(flatten_json_df["id"]==1001)
filter_df.show()

#convert the column names from camel case to snake case
print("6. convert the column names from camel case to snake case")


def toSnakeCase(dataframe):
    for column in dataframe.columns:
        snake_case_col = ''
        for char in column:
            if char.isupper():
                snake_case_col += '_' + char.lower()
            else:
                snake_case_col += char
        dataframe = dataframe.withColumnRenamed(column, snake_case_col)
    return dataframe


snake_case_df = toSnakeCase(flatten_json_df)
snake_case_df.show()

#Add a new column named load_date with the current date
print("7. Add a new column named load_date with the current date")
load_date_df = snake_case_df.withColumn("load_date", current_date())
load_date_df.show()

#create 3 new columns as year, month, and day from the load_date column
print("8. create 3 new columns as year, month, and day from the load_date column")
year_month_day_df = load_date_df.withColumn("year", year(load_date_df.load_date))\
    .withColumn("month", month(load_date_df.load_date))\
    .withColumn("day", day(load_date_df.load_date))
year_month_day_df.show()
