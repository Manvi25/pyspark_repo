from pyspark_repo.src.assignment_1.utils import *
import unittest


class TestAssignment1(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.appName("PySpark Assignment").getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_create_dataframe(self):
        test_purchase_schema = StructType([
            StructField("customer", IntegerType(), True),
            StructField("product_model", StringType(), True)
        ])
        test_purchase_data = [(1, "iphone13"), (1, "dell i5 core"), (2, "iphone13"), (2, "dell i5 core"),
                              (3, "iphone13"), (3, "dell i5 core"), (1, "dell i3 core"), (1, "hp i5 core"),
                              (1, "iphone14"), (3, "iphone14"), (4, "iphone13")]
        test_product_schema = StructType([StructField("product_name", StringType(), True)])
        test_product_data = [("iphone13",), ("dell i5 core",), ("dell i3 core",), ("hp i5 core",), ("iphone14",)]
        test_purchase_df = create_dataframe(self.spark, test_purchase_data, test_purchase_schema)
        test_product_df = create_dataframe(self.spark, test_product_data, test_product_schema)

        self.assertEqual(test_purchase_df.count(), 11)
        self.assertEqual(test_product_df.count(), 5)

    def test_find_in_df(self):
        test_purchase_schema = StructType([
            StructField("customer", IntegerType(), True),
            StructField("product_name", StringType(), True)
        ])

        test_purchase_data = [(1, "iphone13"), (1, "dell i5 core"), (2, "iphone13"), (2, "dell i5 core"),
                              (3, "iphone13"), (3, "dell i5 core"), (1, "dell i3 core"), (1, "hp i5 core"),
                              (1, "iphone14"), (3, "iphone14"), (4, "iphone13")]

        test_purchase_df = create_dataframe(self.spark, test_purchase_data, test_purchase_schema)

        result_df = find_in_df(test_purchase_df, "product_name", "iphone13")

        expected_schema = StructType([
            StructField("customer", IntegerType(), True),
            StructField("product_model", StringType(), True)
        ])

        expected_data = [
            (1, "iphone13"),
            (2, "iphone13"),
            (3, "iphone13"),
            (4, "iphone13")
        ]

        expected_df = create_dataframe(self.spark, expected_data, expected_schema)

        self.assertEqual(sorted(result_df.collect()), sorted(expected_df.collect()))

    def test_find_iphone14(self):
        test_purchase_schema = StructType([
            StructField("customer", IntegerType(), True),
            StructField("product_model", StringType(), True)
        ])

        test_purchase_data = [(1, "iphone13"), (1, "dell i5 core"), (2, "iphone13"), (2, "dell i5 core"),
                              (3, "iphone13"), (3, "dell i5 core"), (1, "dell i3 core"), (1, "hp i5 core"),
                              (1, "iphone14"), (3, "iphone14"), (4, "iphone13")]

        test_purchase_df = create_dataframe(self.spark, test_purchase_data, test_purchase_schema)

        only_iphone13 = find_in_df(test_purchase_df, "product_model", "iphone13")

        result_df = find_iphone14(test_purchase_df, only_iphone13)

        expected_schema = StructType([
            StructField("customer", IntegerType(), True),
            StructField("product_model", StringType(), True)
        ])
        expected_data = [
            (1, "iphone14"),
            (3, "iphone14"),
        ]
        expected_df = create_dataframe(self.spark, expected_data, expected_schema)
        self.assertEqual(result_df.collect(), expected_df.collect())

    def test_find_bought_all(self):
        test_purchase_schema = StructType([
            StructField("customer", IntegerType(), True),
            StructField("product_model", StringType(), True)
        ])
        test_purchase_data = [(1, "iphone13"), (1, "dell i5 core"), (2, "iphone13"), (2, "dell i5 core"),
                         (3, "iphone13"), (3, "dell i5 core"), (1, "dell i3 core"), (1, "hp i5 core"),
                         (1, "iphone14"), (3, "iphone14"), (4, "iphone13")]

        test_product_schema = StructType([
            StructField("product_model", StringType(), True)
        ])
        test_product_data = [("iphone13",), ("dell i5 core",), ("dell i3 core",), ("hp i5 core",), ("iphone14",)]

        test_purchase_df = create_dataframe(self.spark, test_purchase_data, test_purchase_schema)
        test_product_df = create_dataframe(self.spark, test_product_data, test_product_schema)

        result_df = find_bought_all(test_purchase_df, test_product_df)
        expected_schema = StructType([StructField("customer", IntegerType(), True)])
        expected_data = [
            (1,)
        ]
        expected_df = create_dataframe(self.spark, expected_data, expected_schema)
        self.assertEqual(result_df.collect(), expected_df.collect())


if __name__ == '__main__':
    unittest.main()