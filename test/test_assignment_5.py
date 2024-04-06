import unittest
from pyspark_repo.src.assignment_5.utils import *


class TestSparkFunctions(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = spark_session()

        # Create sample data frames for testing
        cls.employee_df = create_df(cls.spark, employee_schema, employee_data)
        cls.department_df = create_df(cls.spark, department_schema, department_data)
        cls.country_df = create_df(cls.spark, country_schema, country_data)

    def test_find_avg_salary_employee(self):
        avg_salary = find_avg_salary_employee(self.employee_df)
        self.assertTrue(avg_salary.count() > 0)

    def test_find_employee_name_starts_with_m(self):
        employees_starts_with_m = find_employee_name_starts_with_m(self.employee_df, self.department_df)
        self.assertTrue(employees_starts_with_m.count() > 0)

    def test_add_bonus_times_2(self):
        employee_bonus_df = add_bonus_times_2(self.employee_df)
        self.assertTrue("bonus" in employee_bonus_df.columns)

    def test_rearrange_columns_employee_df(self):
        rearranged_employee_df = rearrange_columns_employee_df(self.employee_df)
        self.assertEqual(rearranged_employee_df.columns,
                         ["employee_id", "employee_name", "salary", "State", "Age", "department"])

    def test_dynamic_join(self):
        inner_join_result = dynamic_join(self.employee_df, self.department_df, "inner")
        left_join_result = dynamic_join(self.employee_df, self.department_df, "left")
        right_join_result = dynamic_join(self.employee_df, self.department_df, "right")

        self.assertTrue(inner_join_result.count() > 0)
        self.assertTrue(left_join_result.count() > 0)
        self.assertTrue(right_join_result.count() > 0)

    def test_update_country_name(self):
        updated_employee_df = update_country_name(self.employee_df)
        self.assertTrue("country_name" in updated_employee_df.columns)

    def test_column_to_lower(self):
        lower_case_column_df = column_to_lower(self.employee_df)
        self.assertTrue(all(col.lower() in lower_case_column_df.columns for col in self.employee_df.columns))

    def test_add_load_date_with_current_date(self):
        date_df = column_to_lower(self.employee_df)
        date_df = date_df.withColumn("load_date", current_date())
        self.assertTrue("load_date" in date_df.columns)


if __name__ == '__main__':
    unittest.main()