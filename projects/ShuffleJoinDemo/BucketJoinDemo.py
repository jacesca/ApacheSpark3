from lib.logger import log4j
from pyspark.sql import SparkSession, functions as f

def create_new_database(spark, df1, df2):
    # Create a new Database
    spark.sql('CREATE DATABASE IF NOT EXISTS MY_DB')
    spark.sql('USE MY_DB')

    # We just want to save it as a bucket. coalesce-->means get together in just 1 partition again
    df1.coalesce(1).write \
        .bucketBy(3, 'id') \
        .mode('overwrite') \
        .saveAsTable('MY_DB.flight_data1')

    df2.coalesce(1).write \
        .bucketBy(3, 'id') \
        .mode('overwrite') \
        .saveAsTable('MY_DB.flight_data2')


if __name__ == '__main__':
    # Create Spark Session
    spark = SparkSession.builder\
        .appName('Shuffle Join Demo')\
        .master('local[3]')\
        .enableHiveSupport()\
        .getOrCreate()
    logger = log4j(spark)

    # Read the data
    df1 = spark.read.json('data/d1/')
    df2 = spark.read.json('data/d2/')

    # # Call the function to create the new partitioned database
    # create_new_database(spark, df1, df2)

    # Read the new partitioned database
    df3 = spark.read.table('MY_DB.flight_data1')
    df4 = spark.read.table('MY_DB.flight_data2')

    # Make the bucket join
    spark.conf.set('spark.sql.autoBroadcastJoinThreshold', -1)  # To disable the broadcast join
                                                                # We want to go for a sort merge join
                                                                # without a shuffle
    join_expr = df3.id == df4.id
    join_df = df3.join(df4, join_expr, 'inner')

    # Adding an action to explore in http://localhost:4040/jobs/
    # join_df.collect()
    join_df.show(5)
    input('Press a key to stop...')

    # Stop spark
    spark.stop()