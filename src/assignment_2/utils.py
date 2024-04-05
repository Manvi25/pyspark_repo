from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, StructType, StructField, IntegerType


def spark_session():
    spark = SparkSession.builder.appName('spark-assignment').getOrCreate()
    return spark


credit_card_data = [("1234567891234567",),
                    ("5678912345671234",),
                    ("9123456712345678",),
                    ("1234567812341122",),
                    ("1234567812341342",)]
credit_card_schema = ["credit_card"]
credit_card_custom_schema = StructType([
    StructField("card_number", StringType(), True)
])


def create_df(spark, df_data, df_schema):
    df = spark.createDataFrame(df_data, df_schema)
    return df


def create_df_custom_schema(spark, data, custom_schema):
    df = spark.createDataFrame(data, custom_schema)
    return df


def create_df_csv(spark, path_csv, schema):
    df = spark.read.option("header", "true").format("csv").schema(schema).load(path_csv)
    return df


def create_df_csv_custom_schema(spark, path_csv, custom_schema):
    df = spark.read.format("csv").option("header", "true").schema(custom_schema).load(path_csv)
    return df


def create_df_json(spark, path_json):
    df = spark.read.option("multiline", "true").json(path_json)
    return df


def get_no_of_partitions(df):
    total_partitions = df.rdd.getNumPartitions()
    return total_partitions


def increase_partition_by_5(df):
    total_partition = get_no_of_partitions(df)
    new_total_partition = df.rdd.repartition(total_partition + 5)
    new_total_partition_size = new_total_partition.getNumPartitions()
    return new_total_partition_size


def decrease_partition_by_5(df):
    new_total_partition = increase_partition_by_5(df)
    original_partition = df.rdd.repartition(new_total_partition - 5)
    original_partition_size = original_partition.getNumPartitions()
    return original_partition_size


def masked_card_number(cardNumber):
    masked_number = '*' * (len(cardNumber) - 4) + cardNumber[-4:]
    return masked_number


masked_card_number_udf = udf(masked_card_number)