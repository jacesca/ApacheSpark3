from pyspark.sql import SparkSession
from pyspark.sql.functions import spark_partition_id

from lib.logger import log4j

if __name__ == "__main__":
    spark = SparkSession.builder.master("local[3]").appName("SparkSchemaDemo").getOrCreate()
    logger = log4j(spark)

    # Read data source
    data_df = spark.read.format('parquet').load('dataSource/flight-time.parquet')

    # Getting number of partitions
    logger.info('Num of Partitions (Initial state): ' + str(data_df.rdd.getNumPartitions()))
    data_df.groupBy(spark_partition_id()).count().show()

    # Make transformations
    # ....

    # # Write result to some destination: avro format --> In spark-defaults.conf file, requires: spark.jars.packages                org.apache.spark:spark-avro_2.12:3.3.0
    # data_df.write.format('avro').mode('overwrite').option('path', 'dataSink/avro').save()
    # logger.info('AVRO file written...')

    # Write result to some destination: CSV format
    data_df.write.mode('overwrite').csv('dataSink/csv')
    logger.info('CSV file written (First Mode)...')

    data_df.write.format('csv').mode('overwrite').option('path', 'dataSink/csv2').save()
    logger.info('CSV file written (Second Mode)...')


    # Forcing to have more partitions
    partitioned_df = data_df.repartition(5)
    logger.info('Num of Partitions (After changes): ' + str(partitioned_df.rdd.getNumPartitions()))
    partitioned_df.groupBy(spark_partition_id()).count().show()

    partitioned_df.write.format('csv').mode('overwrite').option('path', 'dataSink/csvPartitioned').save()
    logger.info('Partitioned CSV file written ...')


    # Saving to json format and apply logic to the partition
    data_df.write.format('json').mode('overwrite').option('path', 'dataSink/json') \
           .partitionBy('OP_CARRIER', 'ORIGIN').save()
    logger.info('Partitioned JSON file written (logic partitioned applied)...')

    # Adding parameters to partitions
    data_df.write.format('json').mode('overwrite').option('path', 'dataSink/json2') \
           .partitionBy('OP_CARRIER', 'ORIGIN').option('maxRecordsPerFile', 10000).save()
    logger.info('Partitioned JSON file written (maxRecordsPerFile applied)...')

    spark.stop()