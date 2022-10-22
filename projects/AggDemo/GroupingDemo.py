from lib.logger import log4j
from pyspark.sql import SparkSession, functions as f

if __name__ == '__main__':
    spark = SparkSession.builder.appName('Agg Demo').master('local[3]').getOrCreate()
    logger = log4j(spark)

    invoice_df = spark.read\
        .format('csv')\
        .option('header', True)\
        .option('inferSchema', True)\
        .load('data/invoices.csv')
    invoice_df.show(5)
    invoice_df.printSchema()

    # Grouping
    NumInvoices = f.countDistinct('InvoiceNo').alias('NumInvoicesVar')
    TotalQuantity = f.sum('Quantity').alias('TotalQuantity')
    InvoiceValue = f.expr('round(sum(Quantity * UnitPrice), 2) as InvoiceValueExpr')
    summary_df = invoice_df\
        .withColumn('InvoiceDate', f.to_date(f.col('InvoiceDate'), 'dd-MM-yyyy H.mm'))\
        .where('year(InvoiceDate) == 2010')\
        .withColumn('WeekNumber', f.weekofyear(f.col('InvoiceDate')))\
        .groupBy('Country', 'WeekNumber') \
        .agg(NumInvoices, TotalQuantity, InvoiceValue)

    summary_df.coalesce(1).write.format('parquet').mode('overwrite').save('output')
    summary_df.sort('Country', 'WeekNumber').show()

    spark.stop()
