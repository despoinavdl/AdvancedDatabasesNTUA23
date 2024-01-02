from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, TimestampType, DoubleType, StringType, IntegerType
from pyspark.sql.functions import year, month, count, dense_rank, col, to_date
from pyspark.sql.window import Window
import time, datetime

start_time = time.time()

spark = SparkSession \
    .builder \
    .appName("SQL query 1") \
    .getOrCreate()

df = spark.read.csv("hdfs://okeanos-master:54310/data/total_crime.csv" \
                ,header=True)

df.createOrReplaceTempView("crime_records")


query_string = "SELECT \
    YEAR('Date Rptd') AS report_year, \
    MONTH('Date Rptd') AS report_month, \
    COUNT(*) AS crime_count, \
    RANK() OVER (PARTITION BY YEAR('Date Rptd') ORDER BY COUNT(*) DESC) AS month_rank \
FROM \
    crime_records \
GROUP BY \
    YEAR('Date Rptd'), \
    MONTH('Rate Rptd') \
HAVING \
    month_rank <= 3 \
ORDER BY \
    report_year, \
    month_rank;"

final_query = spark.sql(query_string)
final_query.show()
print('Total time for SQL: ',time.time() - start_time , 'sec')
