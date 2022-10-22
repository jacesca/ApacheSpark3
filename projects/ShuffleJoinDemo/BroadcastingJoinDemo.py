from lib.logger import log4j
from pyspark.sql import SparkSession, functions as f

if __name__ == '__main__':
    # Create Spark Session
    spark = SparkSession.builder.appName('Shuffle Join Demo').master('local[3]').getOrCreate()
    logger = log4j(spark)

    # Read the data
    flight_time_df1 = spark.read.json('data/d1/')
    flight_time_df1.show(2)
    flight_time_df2 = spark.read.json('data/d2/')
    flight_time_df2.show(2)

    # Set the shuffle partition configuration
    spark.conf.set('spark.sql.shuffle.partitions', 3)

    # Making the join
    join_expr = flight_time_df1.id == flight_time_df2.id
    join_df = flight_time_df1.join(f.broadcast(flight_time_df2), join_expr, 'inner')
    join_df.show(10)

    # # Adding some dummy actions
    # join_df.foreach(lambda f: None)
    input('Press a key to stop...')

    # Stop spark
    spark.stop()