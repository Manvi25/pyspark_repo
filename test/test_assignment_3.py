import unittest
from pyspark_repo.src.assignment_3.utils import *


class TestAssignment3(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.appName("PySpark Assignment").getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_rename_df(self):
        df = create_df(self.spark, log_data, log_schema)
        expected_df = updateColumnName(df)
        test_log_data = [
            (1, 101, 'login', '2023-09-05 08:30:00'),
            (2, 102, 'click', '2023-09-06 12:45:00'),
            (3, 101, 'click', '2023-09-07 14:15:00'),
            (4, 103, 'login', '2023-09-08 09:00:00'),
            (5, 102, 'logout', '2023-09-09 17:30:00'),
            (6, 101, 'click', '2023-09-10 11:20:00'),
            (7, 103, 'click', '2023-09-11 10:15:00'),
            (8, 102, 'click', '2023-09-12 13:10:00')
        ]
        test_log_schema = StructType([
            StructField("log_id", IntegerType(), True),
            StructField("user_id", IntegerType(), True),
            StructField("user_activity", StringType(), True),
            StructField("timestamp", StringType(), True)
        ])
        actual_df = create_df(self.spark, test_log_data, test_log_schema)
        self.assertEqual(expected_df.collect(), actual_df.collect())

    def test_no_of_action_performed(self):
        df = create_df(self.spark, log_data, log_schema)
        df = updateColumnName(df)
        expected_df = action_performed_last_7(df)
        self.assertEqual(expected_df.count(), 3)

    def test_login_date(self):
        df = create_df(self.spark, log_data, log_schema)
        df = updateColumnName(df)
        expected_df = convert_timestamp_login_date(df)
        test_data = [
            (1, 101, 'login', '2023-09-05'),
            (2, 102, 'click', '2023-09-06'),
            (3, 101, 'click', '2023-09-07'),
            (4, 103, 'login', '2023-09-08'),
            (5, 102, 'logout', '2023-09-09'),
            (6, 101, 'click', '2023-09-10'),
            (7, 103, 'click', '2023-09-11'),
            (8, 102, 'click', '2023-09-12')
        ]
        test_schema = ["log_id", "user_id", "user_activity", "timestamp"]
        actual_df = create_df(self.spark, test_data, test_schema)
        actual_df = updateColumnName(actual_df)
        actual_df = actual_df.select("log_id", "user_id", "user_activity", to_date("time_stamp").alias("login_date"))
        self.assertEqual(expected_df.collect(), actual_df.collect())