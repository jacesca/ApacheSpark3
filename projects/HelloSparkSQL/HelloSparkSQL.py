import sys
from pyspark.sql import SparkSession
from lib.logger import log4j

if __name__ == '__main__':
    spark = SparkSession.builder.master('local[3]').appName('HelloSparkSQL').getOrCreate()

    logger = log4j(spark)

    if len(sys.argv) != 2:
        logger.error('Usage: HelloSparkSQL.py <filename>')
        sys.exit(-1)

    surveyDF = spark.read.option('header', True).option('inferSchema', True).csv(sys.argv[1])
    surveyDF.createOrReplaceTempView('survey_tbl')
    countDF = spark.sql('Select Country, count(1) as count from survey_tbl where Age<40 group by Country')
    countDF.show()

    spark.stop()