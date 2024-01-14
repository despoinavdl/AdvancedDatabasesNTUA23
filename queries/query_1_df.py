from pyspark.sql import SparkSession
from pyspark.sql.functions import year, month, count, col, row_number
from pyspark.sql.window import Window
import time

start_time = time.time()

spark = SparkSession \
    .builder \
    .appName("DF query 1") \
    .getOrCreate()

df = spark.read.csv("hdfs://okeanos-master:54310/data/total_crime.csv" \
                ,header=True)

df.createOrReplaceTempView("crime_records")

#add year, month, crime_counts as columns 
df = df.withColumn("year", year("Date Rptd"))
df = df.withColumn("month", month("Date Rptd"))
crime_counts = df.groupBy("year", "month").\
                        agg(count("*").alias("crime_total"))

#window function partitioned by year to rank the months
window_spec = Window.partitionBy("year").orderBy(col("crime_total").desc())

#add rank
crime_counts_with_rank = crime_counts.withColumn(
    "#",
  row_number().over(window_spec))

top_3_months = crime_counts_with_rank.where(col("#") <= 3)

top_3_months.show()

print('Total time for DF: ', time.time() - start_time, 'sec')
