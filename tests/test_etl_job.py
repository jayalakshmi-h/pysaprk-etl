"""
test_etl_job.py
~~~~~~~~~~~~~~~
This module contains unit tests for the data quality and transformation steps of the ETL
job defined in etl_job.py. It makes use of a local version of PySpark
that is bundled with the PySpark package.
"""
import json
import unittest

from pyspark.sql.functions import col
from pyspark.sql.types import Row

from dependencies.spark import start_spark
from jobs.etl_job import transform_data, isoformat_valid, dq_check, convert_iso_to_mins, convert_mins_to_iso


class SparkETLTests(unittest.TestCase):
    """Test suite for transformation in etl_job.py
    """

    def setUp(self):
        """Start Spark, define config and path to test data
        """
        self.file = open('../configs/etl_config.json')
        self.config = json.load(self.file)
        self.spark, self.log,*_ = start_spark()
        # self.input_df = create_testdata(self.spark)

    def tearDown(self):
        self.file.close()
        """Stop Spark
        """
        self.spark.stop()

    def test_transform_data(self):
        """Test data transformer.
        Using small chunks of input data and expected output data, we
        test the transformation step to make sure it's working as
        expected.
        """
        df = dq_check(self.input_df, self.log)

        actual_output_df = transform_data(df, self.config, self.log)
        actual_cols = len(actual_output_df.columns)
        actual_rows = actual_output_df.count()
        easy_cook_time_act = actual_output_df.filter("difficulty == 'easy'").select(
            col('avg_total_cooking_time')).collect()
        medium_cook_time_act = actual_output_df.filter("difficulty == 'medium'").select(
            col('avg_total_cooking_time')).collect()
        hard_cook_time_act = actual_output_df.filter("difficulty == 'hard'").select(
            col('avg_total_cooking_time')).collect()

        expected_df = self.spark.createDataFrame(
            [Row(difficulty="easy", avg_total_cooking_time="PT7M"),
             Row(difficulty="medium", avg_total_cooking_time="PT40M"),
             Row(difficulty="hard", avg_total_cooking_time="PT1H15M")])
        expected_df_cols = len(expected_df.columns)
        expected_df_rows = expected_df.count()
        easy_cook_time_exp = expected_df.filter("difficulty == 'easy'").select(col('avg_total_cooking_time')).collect()
        medium_cook_time_exp = expected_df.filter("difficulty == 'medium'").select(
            col('avg_total_cooking_time')).collect()
        hard_cook_time_exp = expected_df.filter("difficulty == 'hard'").select(col('avg_total_cooking_time')).collect()

        # assert
        self.assertEqual(expected_df_cols, actual_cols)
        self.assertEqual(expected_df_rows, actual_rows)
        self.assertEqual(easy_cook_time_exp, easy_cook_time_act)
        self.assertEqual(medium_cook_time_exp, medium_cook_time_act)
        self.assertEqual(hard_cook_time_exp, hard_cook_time_act)

    def test_dq_check(self):
        """Test data quality check performed on input dataset.
           It should filter data which has cookTime/prepTime in invalid ISO duration
        """
        df = dq_check(self.input_df, self.log)

        # assert
        self.assertEqual(8, df.count())

    def test_convert_iso_to_mins(self):
        """Test the iso to minutes conversion method
        """
        df = self.spark.createDataFrame([Row(cookTime="PT1H20M30S")])

        result = df.withColumn("mins", convert_iso_to_mins(df["cookTime"])).\
            select("mins").collect()

        # assert
        self.assertTrue("80.5", result)

    def test_convert_mins_to_iso(self):
        """Test the minutes to ISO format conversion method
        """
        df = self.spark.createDataFrame([Row(cookTime="80.5")])

        result = df.withColumn("iso", convert_mins_to_iso(df["cookTime"])).\
            select("iso").collect()

        # assert
        self.assertTrue("PT1H20M30S", result)


def create_testdata(spark):
    local_records = [
        Row(ingredients="Beef", cookTime="PT5M", prepTime="PT5M"),
        Row(ingredients="beef", cookTime="PT5M", prepTime="PT5M"),
        Row(ingredients="beef", cookTime="25", prepTime="PT7H5M"),
        Row(ingredients="egg", cookTime="PT20M", prepTime="76"),
        Row(ingredients="beef", cookTime="34", prepTime="66"),
        Row(ingredients="beef", cookTime="PT45M", prepTime="PT35M"),
        Row(ingredients="beef", cookTime="PT45M", prepTime="PT25M"),
        Row(ingredients="beef", cookTime="PT15M", prepTime="PT25M"),
        Row(ingredients="beef", cookTime="PT15M", prepTime="PT25M"),
        Row(ingredients="beef", cookTime="", prepTime=""),
        Row(ingredients="beef", cookTime="PT1M", prepTime=""),
        Row(ingredients="mushroom", cookTime="", prepTime="PT2M"),
        Row(ingredients="mushroom", cookTime="PT", prepTime="PT2M"),
        Row(ingredients="mushroom", cookTime="PT1M", prepTime="PT"),
        Row(ingredients="mushroom", cookTime="PT", prepTime="PT")
    ]

    df = spark.createDataFrame(local_records)
    return df


if __name__ == '__main__':
    unittest.main()
