from pyspark.sql import SparkSession
from lib.logger import log4j

if __name__ == '__main__':
    spark = SparkSession.builder.master('local[3]').appName('SparkSchemaDemo').getOrCreate()
    logger = log4j(spark)


    # Reading from CSV file
    data_csv_df = spark.read \
                       .format('csv')  \
                       .option('header', True) \
                       .option('inferSchema', True) \
                       .load('data/flight-time.csv')
    data_csv_df.show(5)
    logger.info('CSV Schema' + data_csv_df.schema.simpleString())


    # Reading from JSON file
    data_json_df = spark.read \
                       .format('json')  \
                       .load('data/flight-time.json')
    data_json_df.show(5)
    logger.info('JSON Schema' + data_json_df.schema.simpleString())


    # Reading from PARQUET file
    data_parquet_df = spark.read \
        .format('parquet') \
        .load('data/flight-time.parquet')
    data_parquet_df.show(5)
    logger.info('PARQUET Schema' + data_parquet_df.schema.simpleString())

    spark.stop()

