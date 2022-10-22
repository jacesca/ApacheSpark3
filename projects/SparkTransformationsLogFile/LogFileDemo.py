from lib.logger import log4j
from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract, substring_index

if __name__ == '__main__':
    spark = SparkSession.builder.master('local[3]').appName('LogFileDemo').getOrCreate()
    logger = log4j(spark)

    file_df = spark.read.text('data/apache_logs.txt')
    file_df.printSchema()

    # Structure identified after inspect the apache_logs.txt file
    # IP, client, user, datetime, cmd, request, protocol, status, bytes, referrer, userAgent
    log_reg = r'^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] "(\S+) (\S+) (\S+)" (\d{3}) (\S+) "(\S+)" "(["]*)'

    # Extracting the date we are looking for
    log_df = file_df.select(regexp_extract('value', log_reg, 1).alias('ip'),
                            regexp_extract('value', log_reg, 4).alias('date'),
                            regexp_extract('value', log_reg, 6).alias('request'),
                            regexp_extract('value', log_reg, 10).alias('referrer'))
    log_df.printSchema()
    log_df.groupBy('referrer').count().show(10, truncate=False)

    # Optimizing the last count calculation
    log_df.withColumn('referrer', substring_index('referrer', '/', 3))\
        .groupBy('referrer').count().show(100, truncate=False)

    # Filtering the unnecessary recprds
    log_df.where("trim(referrer) != '-'")\
        .withColumn('referrer', substring_index('referrer', '/', 3)) \
        .groupBy('referrer').count().show(100, truncate=False)

    # Clossing the spark session
    spark.stop()