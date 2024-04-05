from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, StructType, StructField

def spark_session():
    spark= SparkSession.builder.appName("Assignment 2").getOrCreate()
    return spark

data= [
 ("1234567891234567",),
 ("5678912345671234",),
 ("9123456712345678",),
 ("1234567812341122",),
 ("1234567812341342",)
]
schema= StructType([
    StructField("card_number", StringType(), True)
])

#Create a Dataframe as credit_card_df with different read methods
def create_df(spark, data, schema):
    credit_card_df= spark.createDataFrame(data, schema)
    return credit_card_df

#reading csv file through custom schema
def read_csv_with_custom_schema(spark, path_csv, custom_schema):
    credit_card_df=spark.read.format("csv").option("header", "true").schema(custom_schema).load(path_csv)
    return credit_card_df

#reading csv file through inferschema
def read_csv_with_custom_schema(spark):
    credit_card_df= spark.read.csv("../resource/card_number.csv" ,header=True ,inferSchema=True)
    return credit_card_df

#print number of partitions
def get_no_of_partitions(credit_card_df):
    number_of_partition = credit_card_df.rdd.getNumPartitions()
    return number_of_partition

#increasing the partition size to 5
def increase_partition_by_5(credit_card_df):
    repartition= credit_card_df.rdd.repartition(5).getNumPartitions()
    return repartition

#Decrease the partition size back to its original partition size
def decrease_partition_by_5(credit_card_df):
    df_coalesced= credit_card_df.rdd.coalesce(1).getNumPartitions()
    return df_coalesced

#Create a UDF to print only the last 4 digits marking the remaining digits as *
def mask_card_number(card_number):
 return "********" + str(card_number)[-4:]

credit_card_udf = udf(mask_card_number, StringType())
