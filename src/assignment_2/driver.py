from pyspark_repo.src.assignment_2.utils import *

spark = spark_session()
credit_card_df = create_df_custom_schema(spark, credit_card_data, credit_card_custom_schema)
credit_card_df.show()

spark.udf.register("masked_card_number_udf", masked_card_number_udf)
masked_df = credit_card_df.withColumn("masked_card_number", masked_card_number_udf("card_number"))
masked_df.show()