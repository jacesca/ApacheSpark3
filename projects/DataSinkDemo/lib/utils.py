import configparser

from pyspark import SparkConf

def get_spark_app_config():
    spark_conf = SparkConf()
    config = configparser.ConfigParser()
    config.read('spark.conf')

    for key, val in config.items('SPARK_APP_CONFIGS'):
        spark_conf.set(key, val)

    return spark_conf

def load_data_df(spark, data_file):
    return spark.read \
        .option('header', True) \
        .option('inferSchema', True) \
        .csv(data_file)

def count_by_country(data_df):
    return data_df.where('Age < 40') \
        .select('Age', 'Gender', 'Country', 'state') \
        .groupBy('Country') \
        .count()