from pyspark.sql import SparkSession
import time

start_time = time.time()

spark = SparkSession \
    .builder \
    .appName("SQL query 2") \
    .getOrCreate()

df = spark.read.csv("hdfs://okeanos-master:54310/data/total_crime.csv" \
                ,header=True)

df.createOrReplaceTempView("crime_records")


query_string = """
SELECT
    part_of_day,
    COUNT(*) AS crime_count,
    DENSE_RANK() OVER (ORDER BY COUNT(*) DESC) AS rank
FROM (
    SELECT
        CASE
            WHEN (`TIME OCC`) >= 500 AND (`TIME OCC`) < 1200 THEN 'morning'
            WHEN (`TIME OCC`) >= 1200 AND (`TIME OCC`) < 1700 THEN 'afternoon'
            WHEN (`TIME OCC`) >= 1700 AND (`TIME OCC`) < 2100 THEN 'evening'
            ELSE 'night'
        END AS part_of_day
    FROM
        crime_records
    WHERE
        `Premis Desc` = 'STREET'
) tmp
GROUP BY
    part_of_day
ORDER BY
    rank;
"""

final_query = spark.sql(query_string)
final_query.show()
print('Total time for SQL: ',time.time() - start_time , 'sec')
