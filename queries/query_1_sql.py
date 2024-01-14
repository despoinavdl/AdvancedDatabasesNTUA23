from pyspark.sql import SparkSession
import time

start_time = time.time()

spark = SparkSession \
    .builder \
    .appName("SQL query 1") \
    .getOrCreate()

df = spark.read.csv("hdfs://okeanos-master:54310/data/total_crime.csv" \
                ,header=True)

df.createOrReplaceTempView("crime_records")


query_string = "SELECT * FROM(\
   SELECT \
    YEAR(`Date Rptd`) AS year, \
    MONTH(`Date Rptd`) AS month, \
    COUNT(*) AS crime_total, \
    RANK() OVER (PARTITION BY YEAR(`Date Rptd`) ORDER BY COUNT(*) DESC) AS rank \
FROM \
    crime_records \
GROUP BY \
    YEAR(`Date Rptd`), \
    MONTH(`Date Rptd`) \
) AS ranked_data \
WHERE \
    rank <= 3 \
ORDER BY \
    year, \
    rank;"

final_query = spark.sql(query_string)
final_query.show()
print('Total time for SQL: ',time.time() - start_time , 'sec')
