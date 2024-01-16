from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType
from pyspark.sql import functions as F
from pyspark.sql.functions import year, count, col, mean, cos, asin, sqrt, monotonically_increasing_id
from pyspark.sql.window import Window
import time

start_time = time.time()

spark = SparkSession \
    .builder \
    .appName("DF query 4b") \
    .getOrCreate()

#spark.sparkContext.addPyFile("/home/user/geopy-2.4.1/geopy")
#import distance

df = spark.read.csv("hdfs://okeanos-master:54310/data/total_crime.csv" \
                ,header=True)

df = df.withColumn("AREA", col("AREA").cast(IntegerType()))

police_stations = spark.read.csv("hdfs://okeanos-master:54310/data/la_police_stations" \
,header=True)

police_stations = police_stations.withColumn("PREC",
                                             col("PREC").cast(IntegerType()))

# calculate the distance between two points [lat1, long1], [lat2, long2] in km
def get_distance(lat1, lon1, lat2, lon2):
  r = 6371  # km
  p = 3.14 / 180.0

  a = 0.5 - cos((lat2 - lat1) * p) / 2 + cos(lat1 * p) * cos(lat2 * p) * (1 - cos((lon2 - lon1) * p)) / 2
  return 2 * r * asin(sqrt(a))


df = df.select(df["LAT"], df["LON"], df["DATE OCC"], df["AREA"],
               df["Weapon Used Cd"])

firearm_crimes = df.filter(df["Weapon Used Cd"].like("1__"))
firearm_crimes = firearm_crimes.withColumn("id", monotonically_increasing_id()) #to use with window function

#cartesian product
#joined_df = firearm_crimes.crossJoin(police_stations.hint("broadcast"))
joined_df = firearm_crimes.crossJoin(police_stations)
#joined_df.explain(extended=True)

#filter out NULL
filtered_df_a = joined_df.filter(((col("LAT") != 0.0) & (col("LON") != 0.0)) &
                                 (col("X").isNotNull()) &
                                 (col("Y").isNotNull())
)

distance_df = filtered_df_a.withColumn(
    "distance", get_distance(col("LAT"), col("LON"), col("Y"), col("X")))

window = Window.partitionBy("id").orderBy("distance")

closest_df = distance_df.withColumn("year", year("DATE OCC"))

closest_df = closest_df.withColumn(
    "rank",
    F.row_number().over(window))

closest_df = closest_df.filter(col("rank") == 1)

final = closest_df.groupBy("year").agg(
    count("*").alias("#"),
    mean("distance").alias("average_distance")).orderBy("year")

final = final.select("year", "average_distance", "#")

final.show()

#*******************************************************************************
#4_2_b
final_b = closest_df.groupBy("DIVISION").agg(
    count("*").alias("#"),
    mean("distance").alias("average_distance")).orderBy(
        col("#").cast("int").desc())

final_b = final_b.select("DIVISION", "average_distance", "#")

final_b.show(21, truncate=False)
