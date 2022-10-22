import re
from lib.logger import log4j
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, expr
from pyspark.sql.types import StringType


def parse_gender(gender):
    female_pattern = r'^f$|f.m|w.m'
    male_pattern = r'^m$|ma|m.l'

    if re.search(female_pattern, gender.lower()):
        return 'Female'
    elif re.search(male_pattern, gender.lower()):
        return 'Male'
    else:
        return 'Unknown'


if __name__ == '__main__':
    spark = SparkSession.builder.appName('UDF Demo').master('local[2]').getOrCreate()
    logger = log4j(spark)

    survey_df = spark.read.option('header', True).option('inferSchema', True).csv('data/survey.csv')
    survey_df.show(10)

    #-------------------------------------------
    # Transformations with Column objects
    # This method will not register the UDF in the catalog.
    # It will only create a UDF and serialize the function to the executors.
    # This method is necessary when you want to use the function in a Dataframe
    # column object expressions.
    # -------------------------------------------

    parseGenderUdf2 = udf(parse_gender, StringType())
    logger.info('Catalog Entry (column exp):')
    [logger.info(f) for f in spark.catalog.listFunctions() if 'parseGenderUdf2' in f.name]

    survey_df2 = survey_df.withColumn('Gender', parseGenderUdf2('Gender'))
    survey_df2.show(10)

    #-------------------------------------------
    # Transformations with SQL expressions
    # This method register the function as a SQL function
    # This method is necessary when you want to use the function in
    # a SQL expression.
    # -------------------------------------------
    spark.udf.register('parseGenderUdf3', parse_gender)
    logger.info('Catalog Entry (SQL exp):')
    [logger.info(f) for f in spark.catalog.listFunctions() if 'parseGenderUdf3' in f.name]

    # survey_df.createOrReplaceTempView("SurveyDF")
    # spark.sql('Select parseGenderUdf3(Gender) as Gender from SurveyDF').show(10)
    survey_df3 = survey_df.withColumn('Gender', expr('parseGenderUdf3(Gender)'))
    survey_df3.show(10)
    
    spark.stop()