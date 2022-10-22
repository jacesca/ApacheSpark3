from lib.logger import log4j
from pyspark.sql import SparkSession

# Defining a transformation function
from pyspark.sql.functions import to_date, col
from pyspark.sql.types import StructType, StructField, StringType, Row


def to_date_df(df, fmt, fld):
    return df.withColumn(fld, to_date(col(fld), fmt))

if __name__ == '__main__':
    spark = SparkSession.builder.master('local[3]').appName('RowDemo').getOrCreate()
    logger = log4j(spark)

    # Creating a database to test
    my_schema = StructType([
        StructField('ID', StringType()),
        StructField('EventDate', StringType())
    ])

    my_rows = [Row('123', '04/05/2020'),
               Row('124', '4/5/2020'),
               Row('125', '04/5/2020'),
               Row('126', '4/05/2020')]
    my_rdd = spark.sparkContext.parallelize(my_rows, 2)
    my_df = spark.createDataFrame(my_rdd, my_schema)

    # Reviewing the schema and data before transformation
    my_df.printSchema()
    my_df.show()

    # Transforming the data
    new_df = to_date_df(my_df, 'M/d/y', 'EventDate')

    # Reviewing the schema after transformation
    new_df.printSchema()
    new_df.show()

    spark.stop()


