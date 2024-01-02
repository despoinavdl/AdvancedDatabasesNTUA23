from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, TimestampType, DoubleType, StringType, IntegerType
from pyspark.sql.functions import year, month, count, dense_rank, col, to_date
from pyspark.sql.window import Window
import time, datetime

start_time = time.time()

spark = SparkSession \
    .builder \
    .appName("SQL query 2") \
    .getOrCreate()

df = spark.read.csv("hdfs://okeanos-master:54310/data/total_crime.csv" \
                ,header=True)

df.createOrReplaceTempView("crime_records")

revgecoding = spark.read.csv("hdfs://okeanos-master:54310/data/revgecoding.csv", header=True)

revgecoding.createOrReplaceTempView("revgecoding_table")

income_2015 = spark.read.csv("hdfs://okeanos-master:54310/data/income/LA_income_2015.csv", header=True)
income_2017 = spark.read.csv("hdfs://okeanos-master:54310/data/income/LA_income_2017.csv", header=True)
income_2019 = spark.read.csv("hdfs://okeanos-master:54310/data/income/LA_income_2019.csv", header=True)
income_2021 = spark.read.csv("hdfs://okeanos-master:54310/data/income/LA_income_2021.csv", header=True)

income = income_2015.union(income_2017).union(income_2019).union(income_2021)


query_string1 = """
    SELECT
        main.*,
        revgecoding.ZIPcode AS zip
    FROM
        crime_records AS main
    LEFT JOIN
        revgecoding_table AS revgecoding
    ON
        main.LAT >= revgecoding.LAT
        AND main.LAT <= revgecoding.LAT
        AND main.LON >= revgecoding.LON
        AND main.LON <= revgecoding.LON
"""

df = spark.sql(query_string1)

income.createOrReplaceTempView("income_table")

income = income \
.withColumnRenamed("Zip Code", "zip") \
.withColumnRenamed("Community", "community") \
.withColumnRenamed("Estimated Median Income", "median_income")

join_condition = col("crime_records.zip") == col("income_table.zip")

query_string2 = spark.sql("""
    SELECT
        crime_records.*,
        income_table.median_income AS median_income
    FROM
        crime_records
    LEFT JOIN
        income_table
    ON
        {join_condition}
""".format(join_condition=join_condition))

df = spark.sql(query_string2)


print('Total time for SQL: ',time.time() - start_time , 'sec')


column_types = df.dtypes
print("Column Types for df:")
for col_name, col_type in column_types:
  print(f"{col_name}: {col_type}")

#column_types2 = income.dtypes
#print("Column Types for income:")
#for col_name, col_type in column_types2:
#  print(f"{col_name}: {col_type}")
