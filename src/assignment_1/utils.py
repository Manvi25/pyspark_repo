from pyspark.sql import SparkSession
from pyspark.sql.functions import col, countDistinct
from pyspark.sql.types import StructType, IntegerType, StructField, StringType


purchase_schema = StructType([
    StructField("customer", IntegerType(), True),
    StructField("product_model", StringType(), True)
])
purchase_data = [(1, "iphone13"),
                 (1, "dell i5 core"),
                 (2, "iphone13"),
                 (2, "dell i5 core"),
                 (3, "iphone13"),
                 (3, "dell i5 core"),
                 (1, "dell i3 core"),
                 (1, "hp i5 core"),
                 (1, "iphone14"),
                 (3, "iphone14"),
                 (4, "iphone13")]

product_schema = StructType([
    StructField("product_model", StringType(), True)
])
product_data = [("iphone13",), ("dell i5 core",), ("dell i3 core",), ("hp i5 core",), ("iphone14",)]


def create_spark_session():
    spark = SparkSession.builder.appName('assignment_1').getOrCreate()
    return spark


def create_dataframe(spark, data, schema):
    df = spark.createDataFrame(data, schema)
    return df


def find_in_df(df, column_name, product_name):
    result = df.filter(df[column_name] == product_name)
    return result


def find_iphone14(df, only_iphone13):
    only_iphone14 = df.filter(col("product_model") == "iphone14")
    result = only_iphone13.join(only_iphone14, only_iphone13["customer"] == only_iphone14["customer"], "inner")\
        .drop(only_iphone14.customer, only_iphone14.product_model)
    return result


def find_bought_all(df1, df2):
    customer_models_count = df1.groupBy("customer").agg(countDistinct("product_model").alias("model_count"))
    all_models = customer_models_count.filter(customer_models_count.model_count == df2.count()).select("customer")
    return all_models