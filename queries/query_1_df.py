from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, TimestampType, DoubleType, StringType, IntegerType
from pyspark.sql.functions import year, month, count, dense_rank, col, to_date
from pyspark.sql.window import Window
import time, datetime

start_time = time.time()

spark = SparkSession \
    .builder \
    .appName("DF query 1") \
    .getOrCreate()

df = spark.read.csv("hdfs://okeanos-master:54310/data/total_crime.csv" \
                ,header=True)

df.createOrReplaceTempView("crime_records")

#add year and month as columns from the timestamp
df = df.withColumn("year", year("Date Rptd"))
df = df.withColumn("month", month("Date Rptd"))

#calculate the count of crimes per year and month
crime_counts = df.groupBy("year", "month").\
                        agg(count("*").alias("crime_total"))

#create a window partitioned by year to rank the months based on crime count
window_spec = Window.partitionBy("year").orderBy(col("crime_total").desc())

#add a dense rank column based on crime count within each year
crime_counts_with_rank = crime_counts.withColumn(
    "#",
    dense_rank().over(window_spec))

#filter to get the top 3 months with the highest crime counts per year
top_3_months = crime_counts_with_rank.where(col("#") <= 3)
top_3_months = top_3_months.orderBy("year", col("crime_total").desc())
top_3_months.show()
print('Total time for DF: ', time.time() - start_time, 'sec')
