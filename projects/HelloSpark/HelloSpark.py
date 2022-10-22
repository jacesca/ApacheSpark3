import sys

from pyspark import SparkConf
from pyspark.sql import SparkSession
from lib.logger import log4j
from lib.utils import get_spark_app_config, load_data_df, count_by_country

if __name__ == "__main__":
    # First method to start the spark program
    # spark = SparkSession.builder \
    #     .appName('Hello-Spark') \
    #     .master('local[3]') \
    #     .getOrCreate()

    # Second method to start the spark program
    # conf = SparkConf()
    # conf.set('spark.app.name', 'Hello-Spark')
    # conf.set('spark.master', 'local[3]')
    # spark = SparkSession.builder \
    #     .config(conf=conf) \
    #     .getOrCreate()

    # Third method to start the spark program
    conf = get_spark_app_config()
    spark = SparkSession.builder \
        .config(conf=conf) \
        .getOrCreate()

    logger = log4j(spark)
    logger.info('Starting Hello Spark.')

    # Printing the Spark configuration
    # sc = spark.sparkContext
    # # sc.setLogLevel("ERROR")
    # conf_out = sc.getConf()
    # logger.info(conf_out.toDebugString())

    # Getting the data file from command line
    if len(sys.argv) != 2:
        logger.error('Usage: HelloSpark <filename>')
        sys.exit(-1)

    # Reading the data
    logger.info('Reading data...')
    data_df = load_data_df(spark, sys.argv[1])

    # Printing the data
    logger.info('Printing data...')
    data_df.show()

    # Making some transformation
    logger.info('Applying transformations...')
    sample_data_df = count_by_country(data_df)
    logger.info(sample_data_df.collect())
    # sample_data_df = data_df.where('Age < 40') \
    #     .select('Age', 'Gender', 'Country', 'state')
    # sample_data_df.show()
    #
    # logger.info('Grouping the data...')
    # sample_data_df = sample_data_df.groupBy('Country')
    # sample_data_df.count().show()

    logger.info('Working with partitions...')
    partitioned_data_df = data_df.repartition(2)
    sample_data_df = count_by_country(partitioned_data_df)
    logger.info(sample_data_df.collect())

    # Closing the Spark instance
    input('Press Enter')
    logger.info('Finished HelloSpark')
    spark.stop()