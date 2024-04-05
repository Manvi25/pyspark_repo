from pyspark_repo.src.assignment_1.utils import *
import logging

logging.basicConfig(level=logging.INFO, format='%(message)s')

spark = create_spark_session()

logging.info("Creating Dataframe")
purchase_data_df = create_dataframe(spark, purchase_data, purchase_schema)
logging.info("Purchase data")
purchase_data_df.show(truncate=False)
product_data_df = create_dataframe(spark, product_data, product_schema)
logging.info("product data")
product_data_df.show(truncate=False)

iphone13_df = find_in_df(purchase_data_df, "product_model", "iphone13")
logging.info("2. Find the customers who have bought only iphone13")
iphone13_df.show()

iphone13_upgrade_iphone14 = find_iphone14(purchase_data_df, iphone13_df)
logging.info("3. Find customers who upgraded from iphone13 to iphone14")
iphone13_upgrade_iphone14.show()

bought_all_product = find_bought_all(purchase_data_df, product_data_df)
logging.info("4. Find customers who have bought all models in the product Data")
bought_all_product.show(truncate=False)