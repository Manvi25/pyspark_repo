from pyspark.sql import SparkSession
from pyspark.sql.functions import datediff, expr, col, to_date
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType


def spark_session():
    spark = SparkSession.builder.appName('spark-assignment').getOrCreate()
    return spark


log_data = [
    (1, 101, 'login', '2023-09-05 08:30:00'),
    (2, 102, 'click', '2023-09-06 12:45:00'),
    (3, 101, 'click', '2023-09-07 14:15:00'),
    (4, 103, 'login', '2023-09-08 09:00:00'),
    (5, 102, 'logout', '2023-09-09 17:30:00'),
    (6, 101, 'click', '2023-09-10 11:20:00'),
    (7, 103, 'click', '2023-09-11 10:15:00'),
    (8, 102, 'click', '2023-09-12 13:10:00')
]
log_schema = StructType([
    StructField("log id", IntegerType(), True),
    StructField("user$id", IntegerType(), True),
    StructField("action", StringType(), True),
    StructField("timestamp", StringType(), True)
])


def create_df(spark, data, schema):
    df = spark.createDataFrame(data, schema)
    df = df.withColumn("timestamp", col("timestamp").cast(TimestampType()))
    return df


# 2.Column names should be log_id, user_id, user_activity, time_stamp using dynamic function
def updateColumnName(dataframe):
    count = 0
    new_column_names = ["log_id", "user_id", "user_activity", "time_stamp"]
    for column in dataframe.columns:
        dataframe = dataframe.withColumnRenamed(column, new_column_names[count])
        count += 1
    return dataframe


# 3. Write a query to calculate the number of actions performed by each user in the last 7 days
def action_performed_last_7(df):
    df_filtered = df.filter(datediff(expr("date('2023-09-05')"), expr("date(timestamp)")) <= 7)
    actions_performed = df_filtered.groupby("user_id").count()
    return actions_performed


# 4. Convert the time stamp column to the login_date column with YYYY-MM-DD format with date type as its data type
def convert_timestamp_login_date(df):
    login_date_df = df.select("log_id", "user_id", "user_activity", to_date("time_stamp").alias("login_date"))
    return login_date_df