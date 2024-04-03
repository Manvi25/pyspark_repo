from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, StructType, StructField

spark= SparkSession.builder.appName("Assignment 2").getOrCreate()

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
print("Method 1: Using Dataframe functions")
credit_card_df= spark.createDataFrame(data, schema)
credit_card_df.show()

#reading csv file through custom schema
print("Method 2:reading through csv file using custom schema")
credit_card_df=spark.read.format("csv").option("header","true").schema(schema).load("../resource/card_number.csv")
credit_card_df.show()
credit_card_df.printSchema()

#reading csv file through inferschema
print("reading csv file through inferschema")
credit_card_df= spark.read.csv("../resource/card_number.csv" ,header=True ,inferSchema=True)
credit_card_df.show()
credit_card_df.printSchema()

#print number of partitions
print("Print number of partitions ")
number_of_partition = credit_card_df.rdd.getNumPartitions()
print("Number of partitions before repartitioning:", number_of_partition)

#increasing the partition size to 5
repartition= credit_card_df.rdd.repartition(5).getNumPartitions()
print("Number of partitions after doing partitioning:",repartition)

#Decrease the partition size back to its original partition size
print("Decreasing the partition size to original partition size")
df_coalesced= credit_card_df.rdd.coalesce(1).getNumPartitions()
print("Number of partition after using coalesce:",df_coalesced)

#Create a UDF to print only the last 4 digits marking the remaining digits as *
def mask_card_number(card_number):
 return "********" + str(card_number)[-4:]

credit_card_udf = udf(mask_card_number, StringType())
credit_card_df_udf = credit_card_df.withColumn("masked_card_number",
                      credit_card_udf(credit_card_df['card_number']))
credit_card_df_udf.show()