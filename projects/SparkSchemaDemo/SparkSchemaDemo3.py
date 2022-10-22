from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, DateType, StringType, IntegerType, TimestampType
from lib.logger import log4j

if __name__ == '__main__':
    spark = SparkSession.builder.master('local[3]').appName('SparkSchemaDemo').getOrCreate()
    logger = log4j(spark)


    # Defining the schema DDL
    dataSchemaDDL = """FL_DATE DATE, OP_CARRIER STRING, OP_CARRIER_FL_NUM INT, ORIGIN STRING, 
              ORIGIN_CITY_NAME STRING, DEST STRING, DEST_CITY_NAME STRING, CRS_DEP_TIME INT, DEP_TIME INT, 
              WHEELS_ON INT, TAXI_IN INT, CRS_ARR_TIME INT, ARR_TIME INT, CANCELLED INT, DISTANCE INT"""

    # Reading from CSV file
    data_csv_df = spark.read \
                       .format('csv')  \
                       .option('header', True) \
                       .schema(dataSchemaDDL) \
                       .option('mode', 'FAILFAST') \
                       .option('dateFormat', 'M/d/y') \
                       .load('data/flight*.csv')
    data_csv_df.show(5)
    logger.info('CSV Schema' + data_csv_df.schema.simpleString())

    # Reading from JSON file
    data_json_df = spark.read \
        .format('json') \
        .schema(dataSchemaDDL) \
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

