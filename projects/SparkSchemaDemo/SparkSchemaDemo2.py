from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, DateType, StringType, IntegerType, TimestampType
from lib.logger import log4j

if __name__ == '__main__':
    spark = SparkSession.builder.master('local[3]').appName('SparkSchemaDemo').getOrCreate()
    logger = log4j(spark)


    # Defining the schema programmatically
    data_schema = StructType([
        StructField('FL_DATE', DateType()),
        StructField('OP_CARRIER', StringType()),
        StructField('OP_CARRIER_FL_NUM', IntegerType()),
        StructField('ORIGIN', StringType()),
        StructField('ORIGIN_CITY_NAME', StringType()),
        StructField('DEST', StringType()),
        StructField('DEST_CITY_NAME', StringType()),
        StructField('CRS_DEP_TIME', IntegerType()),
        StructField('DEP_TIME', IntegerType()),
        StructField('WHEELS_ON', IntegerType()),
        StructField('TAXI_IN', IntegerType()),
        StructField('CRS_ARR_TIME', IntegerType()),
        StructField('ARR_TIME', IntegerType()),
        StructField('CANCELLED', IntegerType()),
        StructField('DISTANCE', IntegerType())
    ])

    # Reading from CSV file
    data_csv_df = spark.read \
                       .format('csv')  \
                       .option('header', True) \
                       .schema(data_schema) \
                       .option('mode', 'FAILFAST') \
                       .option('dateFormat', 'M/d/y') \
                       .load('data/flight*.csv')
    data_csv_df.show(5)
    logger.info('CSV Schema' + data_csv_df.schema.simpleString())


    # Reading from JSON file
    data_json_df = spark.read \
                       .format('json') \
                       .schema(data_schema) \
                       .option('mode', 'FAILFAST') \
                       .option('dateFormat', 'M/d/y') \
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

