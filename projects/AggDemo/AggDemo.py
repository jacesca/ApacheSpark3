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

    # Simple Aggregations
    invoice_df.select(f.count('*').alias('Count *'),
                      f.sum('Quantity').alias('TotalQuantity'),
                      f.avg('UnitPrice').alias('AvgPrice'),
                      f.countDistinct('InvoiceNo').alias('CountDistinct')).show()

    invoice_df.selectExpr(
        'count(1) as `count 1`',
        "count(*) as `count *`",
        "count(StockCode) as `count field`",
        "sum(Quantity) as TotaQuantity",
        "avg(UnitPrice) as AvgPrice",
        "count(distinct(InvoiceNo)) as CountDistinct"
    ).show()

    # Grouping Aggregations
    invoice_df.createOrReplaceTempView("Sales")
    summary_sql = spark.sql("""
        SELECT Country, InvoiceNo,
            sum(Quantity) as TotalQuantity,
            round(sum(Quantity * UnitPrice), 2) as InvoiceValue
        FROM sales
        GROUP BY Country, InvoiceNo
    """)
    summary_sql.show(5)

    summary_df = invoice_df.groupBy('Country', 'InvoiceNo')\
        .agg(f.sum('Quantity').alias('TotalQuantity'),
             f.round(f.sum(f.expr('Quantity * UnitPrice')), 2).alias('InvoiceValue'),
             f.expr('round(sum(Quantity * UnitPrice), 2) as InvoiceValueExpr'))
    summary_df.show(5)

    # Stop spark session
    spark.stop()
