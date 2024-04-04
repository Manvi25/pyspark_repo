from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,IntegerType,StringType,TimestampType
from pyspark.sql.functions import col,datediff,expr,to_date

spark = SparkSession.builder.appName('assignment 3').getOrCreate()

Data = [
 (1, 101, 'login', '2023-09-05 08:30:00'),
 (2, 102, 'click', '2023-09-06 12:45:00'),
 (3, 101, 'click', '2023-09-07 14:15:00'),
 (4, 103, 'login', '2023-09-08 09:00:00'),
 (5, 102, 'logout', '2023-09-09 17:30:00'),
 (6, 101, 'click', '2023-09-10 11:20:00'),
 (7, 103, 'click', '2023-09-11 10:15:00'),
 (8, 102, 'click', '2023-09-12 13:10:00')
]

Schema = StructType([
    StructField("log_id", IntegerType()),
    StructField("user$id",IntegerType()),
    StructField("action",StringType()),
    StructField("timestamp",StringType())
])

df = spark.createDataFrame(data=Data , schema= Schema)


#created a function to rename the columns in dataframe

def rename_columns(dataframe, new_column_names):
    for old_col, new_col in zip(dataframe.columns, new_column_names):
        dataframe = dataframe.withColumnRenamed(old_col, new_col)
    return dataframe

new_column_names = ["log_id", "user_id", "user_activity", "time_stamp"]
#renamed columns in the dataframe
rename_columns_df = rename_columns(df, new_column_names)
rename_columns_df.show()

# query to calculate the number of actions performed by each user in the last 7 days
df_filter= rename_columns_df.filter(datediff(expr("date('2023-09-05')"), expr("date(timestamp)")) <= 7)
actions_performed= df_filter.groupby("user_id").count()
print("Number of actions performed by each user in last 7 days")
actions_performed.show()

#Convert the time stamp column to the login_date column with YYYY-MM-DD format with date type as its data type
print("Convert the time stamp column to the login_date column with YYYY-MM-DD format with date type as its data type")
login_date_df= rename_columns_df.select("log_id","user_id","user_activity",to_date("time_stamp")
                                        .alias("login_date"))
login_date_df.printSchema()
login_date_df.show()

