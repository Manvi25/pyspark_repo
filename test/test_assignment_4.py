import unittest

from pyspark_repo.src.assignment_4.utils import *


class TestAssignment4(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.appName("spark-assignment").getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_read_json(self):
        df = read_json(self.spark, json_path)
        self.assertEqual(df.count(), 1)

    def test_flatten_json(self):
        df = read_json(self.spark, json_path)
        schema = StructType([
            StructField("employees", ArrayType(StructType([
                StructField("empId", IntegerType(), True),
                StructField("empName", StringType(), True)
            ])), True),
            StructField("id", IntegerType(), True),
            StructField("properties", StructType([
                StructField("name", StringType(), True),
                StructField("storeSize", StringType(), True)
            ]), True)
        ])

        data = [
            (
                [
                    (1001, 'Divesh'),
                    (1002, 'Rajesh'),
                    (1003, 'David')
                ],
                1001,
                ("ABC Pvt Ltd", "Medium")
            )
        ]

        expected_df = create_df(self.spark, data, schema)
        self.assertEqual(df.collect(), expected_df.collect())

    def test_count_before_after_flatten(self):
        schema = StructType([
            StructField("employees", ArrayType(StructType([
                StructField("empId", IntegerType(), True),
                StructField("empName", StringType(), True)
            ])), True),
            StructField("id", IntegerType(), True),
            StructField("properties", StructType([
                StructField("name", StringType(), True),
                StructField("storeSize", StringType(), True)
            ]), True)
        ])

        data = [
            (
                [
                    (1001, 'Divesh'),
                    (1002, 'Rajesh'),
                    (1003, 'David')
                ],
                1001,
                ("ABC Pvt Ltd", "Medium")
            )
        ]
        df = create_df(self.spark, data, schema)
        flattened_df = flatten_df(df)
        self.assertEqual(df.count(), 1)
        self.assertEqual(flattened_df.count(), 3)

    def test_diff_explode_outer_posexplode(self):
        self.assertEqual(diff_explode_outer_posexplode(self.spark), None)

    def test_filter_employee_with_id(self):
        schema = StructType([
            StructField("employees", ArrayType(StructType([
                StructField("empId", IntegerType(), True),
                StructField("empName", StringType(), True)
            ])), True),
            StructField("id", IntegerType(), True),
            StructField("properties", StructType([
                StructField("name", StringType(), True),
                StructField("storeSize", StringType(), True)
            ]), True)
        ])

        data = [
            (
                [
                    (1001, 'Divesh'),
                    (1002, 'Rajesh'),
                    (1003, 'David')
                ],
                1001,
                ("ABC Pvt Ltd", "Medium")
            )
        ]
        df = create_df(self.spark, data, schema)
        df = flatten_df(df)
        filtered_df = filter_employee_with_id(df, 1001)
        self.assertEqual(filtered_df.count(), 1)

    def test_to_snake_case(self):
        schema = StructType([
            StructField("employees", ArrayType(StructType([
                StructField("empId", IntegerType(), True),
                StructField("empName", StringType(), True)
            ])), True),
            StructField("id", IntegerType(), True),
            StructField("properties", StructType([
                StructField("name", StringType(), True),
                StructField("storeSize", StringType(), True)
            ]), True)
        ])

        data = [
            (
                [
                    (1001, 'Divesh'),
                    (1002, 'Rajesh'),
                    (1003, 'David')
                ],
                1001,
                ("ABC Pvt Ltd", "Medium")
            )
        ]
        df = create_df(self.spark, data, schema)
        df = flatten_df(df)
        result_df = toSnakeCase(df)
        self.assertTrue("emp_id" in result_df.columns)

    def test_add_load_date_with_current_date(self):
        schema = StructType([
            StructField("employees", ArrayType(StructType([
                StructField("empId", IntegerType(), True),
                StructField("empName", StringType(), True)
            ])), True),
            StructField("id", IntegerType(), True),
            StructField("properties", StructType([
                StructField("name", StringType(), True),
                StructField("storeSize", StringType(), True)
            ]), True)
        ])

        data = [
            (
                [
                    (1001, 'Divesh'),
                    (1002, 'Rajesh'),
                    (1003, 'David')
                ],
                1001,
                ("ABC Pvt Ltd", "Medium")
            )
        ]
        df = create_df(self.spark, data, schema)
        result_df = add_load_date_with_current_date(df)
        self.assertTrue("load_date" in result_df.columns)

    def test_add_year_month_day(self):
        schema = StructType([
            StructField("employees", ArrayType(StructType([
                StructField("empId", IntegerType(), True),
                StructField("empName", StringType(), True)
            ])), True),
            StructField("id", IntegerType(), True),
            StructField("properties", StructType([
                StructField("name", StringType(), True),
                StructField("storeSize", StringType(), True)
            ]), True)
        ])

        data = [
            (
                [
                    (1001, 'Divesh'),
                    (1002, 'Rajesh'),
                    (1003, 'David')
                ],
                1001,
                ("ABC Pvt Ltd", "Medium")
            )
        ]
        df = create_df(self.spark, data, schema)
        df = flatten_df(df)
        df = add_load_date_with_current_date(df)
        result_df = add_year_month_day(df)
        self.assertTrue("year" in result_df.columns)
        self.assertTrue("month" in result_df.columns)
        self.assertTrue("day" in result_df.columns)


if __name__ == '__main__':
    unittest.main()