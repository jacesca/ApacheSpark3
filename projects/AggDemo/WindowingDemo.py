from lib.logger import log4j
from pyspark.sql import SparkSession, Window, functions as f

if __name__ == '__main__':
    spark = SparkSession.builder.appName('Agg Demo').master('local[3]').getOrCreate()
    logger = log4j(spark)

    summary_df = spark.read.parquet('data/summary.parquet')
    summary_df.show()
    summary_df.printSchema()

    # Windowing Aggregations --> 3 things to define: Partition, Ordering and Window Start/End
    running_total_window = Window.partitionBy('Country')\
                                 .orderBy('WeekNumber')\
                                 .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    summary_df.withColumn('RunningTotal', f.sum('InvoiceValue').over(running_total_window)).show()

    # Stop Spark Session
    spark.stop()
