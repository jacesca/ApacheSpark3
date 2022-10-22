# First test case
# Goal: Make sure that we are loading the data file correctly
# Steps: 1. Read data, 2. Validate the number of records.

# Second test case
# Goal: Record count by country is computed correctly
# Steps: 1. Read data, 2. Validate counting by Country
from unittest import TestCase
from pyspark.sql import SparkSession
from lib.utils import load_data_df, count_by_country

class UtilsTestCase(TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        cls.spark = SparkSession.builder \
            .appName('Hello-Spark-Test') \
            .master('local[3]') \
            .getOrCreate()

    # @classmethod
    # def tearDownClass(cls) -> None:
    #     cls.spark.stop()

    def test_datafile_location(self):
        sample_df = load_data_df(self.spark, 'data/sample.csv')
        result_count = sample_df.count()
        self.assertEqual(result_count, 9, "Record should be 9...")

    def test_country_count(self):
        sample_df = load_data_df(self.spark, 'data/sample.csv')
        count_list = count_by_country(sample_df).collect()
        count_dict = dict(count_list)
        self.assertEqual(count_dict['United Kingdom'], 1, "Count for United Kingdom should be 1...")
        self.assertEqual(count_dict['Canada'], 2, "Count for Canada should be 2...")
        self.assertEqual(count_dict['United States'], 4, "Count for United States should be 4...")
