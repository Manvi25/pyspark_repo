from pyspark_repo.src.assignment_2.utils import *
import logging

logging.basicConfig(level=logging.INFO, format='%(message)s')

# Initialize SparkSession
spark = spark_session()

# Create DataFrame with different read methods
logging.info("Method 1: Using DataFrame functions")
credit_card_df = create_df(spark, data, schema)
credit_card_df.show()

# Reading CSV file through custom schema
credit_card_custom_schema = StructType([
    StructField("card_number", StringType(), True)
])
csv_path_custom_schema = "../resource/card_number.csv"
credit_card_df_custom_schema = read_csv_with_custom_schema(spark, csv_path_custom_schema, credit_card_custom_schema)
credit_card_df_custom_schema.show()
credit_card_df_custom_schema.printSchema()

# Reading CSV file through inferred schema
csv_path_infer_schema = "../resource/card_number.csv"
credit_card_df_infer_schema = read_csv_with_custom_schema(spark, csv_path_infer_schema)
credit_card_df_infer_schema.show()
credit_card_df_infer_schema.printSchema()

# Print number of partitions
logging.info("Print number of partitions")
number_of_partitions = get_no_of_partitions(credit_card_df_infer_schema)
logging.info("Number of partitions before repartitioning: {}".format(number_of_partitions))

# Increase the partition size to 5
logging.info("Increasing the partition size to 5")
repartition = increase_partition_by_5(credit_card_df_infer_schema)
logging.info("Number of partitions after repartitioning: {}".format(repartition))

# Decrease the partition size back to its original partition size
logging.info("Decreasing the partition size to original partition size")
df_coalesced = decrease_partition_by_5(credit_card_df_infer_schema)
logging.info("Number of partitions after coalescing: {}".format(df_coalesced))

# Create a UDF to mask the card number
logging.info("Creating a UDF to mask the card number")
credit_card_udf = udf(mask_card_number, StringType())
credit_card_df_udf = credit_card_df_infer_schema.withColumn("masked_card_number", credit_card_udf(credit_card_df_infer_schema['card_number']))
credit_card_df_udf.show()
