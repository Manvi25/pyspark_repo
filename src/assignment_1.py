from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import col, countDistinct

spark = SparkSession.builder \
    .appName("Custom Schema Example") \
    .getOrCreate()

# schema for purchase data
purchase_schema = StructType([
    StructField("customer", IntegerType(), True),
    StructField("product_model", StringType(), True)
])

product_schema = StructType([
    StructField("product_model", StringType(), True)
])

# purchase data
purchase_data = [
    (1, "iphone13"),
    (1, "dell i5 core"),
    (2, "iphone13"),
    (2, "dell i5 core"),
    (3, "iphone13"),
    (3, "dell i5 core"),
    (1, "dell i3 core"),
    (1, "hp i5 core"),
    (1, "iphone14"),
    (3, "iphone14"),
    (4, "iphone13")
]

# product data
product_data = [
    ("iphone13",),
    ("dell i5 core",),
    ("dell i3 core",),
    ("hp i5 core",),
    ("iphone14",)
]

purchase_data_df = spark.createDataFrame(purchase_data, schema=purchase_schema)
product_data_df = spark.createDataFrame(product_data, schema=product_schema)

purchase_data_df.show()
product_data_df.show()

# Find the customers who have bought only iphone13
print("Customers who have bought only iphone13")
iphone13_customers= purchase_data_df.filter("product_model== 'iphone13'")
iphone13_customers.show()

# customers who upgraded from product iphone13 to product iphone14
print("Customers who upgraded from product iphone13 to iphone14")
iphone14_customers = purchase_data_df.filter("product_model = 'iphone13'") \
    .join(purchase_data_df.filter("product_model = 'iphone14'"), "customer") \
    .select("customer").distinct()
iphone14_customers.show()

# customers who have bought all models in the new Product Data
print("Customers who have bought all models in the new product data")
customer_models_count = purchase_data_df.groupBy("customer").agg(countDistinct("product_model").alias("model_count"))
all_models = customer_models_count.filter(customer_models_count.model_count == product_data_df.count()).select("customer")
all_models.show()