from lib.logger import log4j
from pyspark.sql import SparkSession

if __name__ == '__main__':
    spark = SparkSession.builder\
        .master('local[3]')\
        .appName('SparkSQLTableDemo')\
        .enableHiveSupport()\
        .getOrCreate()
    logger = log4j(spark)

    data_df = spark.read.format('parquet').load('dataSource/flight-time.parquet')

    # # Write data to managed tables using the default database
    # data_df.write.mode('overwrite').saveAsTable('flight_data_tbl')

    # Creating a new database
    spark.sql('CREATE DATABASE IF NOT EXISTS AIRLINE_DB')

    # # Write data to managed tables in a specific database (first method)
    # data_df.write.mode('overwrite').saveAsTable('AIRLINE_DB.flight_data_tbl')

    # # Write data to managed tables in a new database (second method)
    # spark.catalog.setCurrentDatabase('AIRLINE_DB')
    # data_df.write\
    #     .mode('overwrite')\
    #     .saveAsTable('flight_data_tbl')

    # # Write data to managed partitioned tables  (third method)
    # spark.catalog.setCurrentDatabase('AIRLINE_DB')
    # data_df.write\
    #     .mode('overwrite')\
    #     .partitionBy('OP_CARRIER', 'ORIGIN')\
    #     .saveAsTable('flight_data_tbl')

    # Write data to managed partitioned tables with bucket configuration (fourth method)
    spark.catalog.setCurrentDatabase('AIRLINE_DB')
    data_df.write\
        .format('csv')\
        .mode('overwrite') \
        .option("header", True) \
        .bucketBy(5, 'OP_CARRIER', 'ORIGIN')\
        .sortBy('OP_CARRIER', 'ORIGIN')\
        .saveAsTable('flight_data_tbl')

    logger.info(spark.catalog.listTables('AIRLINE_DB'))

    spark.stop()
