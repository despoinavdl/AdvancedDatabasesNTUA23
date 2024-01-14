from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, TimestampType, DoubleType, StringType, IntegerType, FloatType
from pyspark.sql.functions import year, month, count, dense_rank, col, to_date, row_number, mean, udf, cos, radians, sin, asin, sqrt
from pyspark.sql.window import Window
import time, datetime
import geopy.distance
#from geopy.distance import geodesic
#import pip
#pip.main(['install', 'geopy'])
#import geopy.distance

start_time = time.time()

spark = SparkSession \
    .builder \
    .appName("DF query 4") \
    .getOrCreate()

#spark.sparkContext.addPyFile("/home/user/geopy-2.4.1/geopy")
#import distance

df = spark.read.csv("hdfs://okeanos-master:54310/data/total_crime.csv" \
                ,header=True)

df = df.withColumn("AREA", col("AREA").cast(IntegerType()))

police_stations = spark.read.csv("hdfs://okeanos-master:54310/data/la_police_stations" \
,header=True)

police_stations = police_stations.withColumn("PREC", col("PREC").cast(IntegerType()))

# calculate the distance between two points [lat1, long1], [lat2, long2] in km
#def get_distance(lat1, long1, lat2, long2):
#    return distance.geodesic((lat1, long1), (lat2, long2)).km

def get_distance(lat1, lon1, lat2, lon2):
    r = 6371 # km
    p = 3.14 / 180.0

    a = 0.5 - cos((lat2-lat1)*p)/2 + cos(lat1*p) * cos(lat2*p) * (1-cos((lon2-lon1)*p))/2
    return 2 * r * asin(sqrt(a))


df = df.select(df["LAT"], df["LON"], df["DATE OCC"], df["AREA"], df["Weapon Used Cd"])

firearm_crimes = df.filter(df["Weapon Used Cd"].like("1__"))

joined_df = firearm_crimes.join(
    police_stations,
    firearm_crimes["AREA"] == police_stations["PREC"],
    "left"
)

#filter out NULL
filtered_df_a = joined_df.filter(((col("LAT") != 0.0) & (col("LON") != 0.0)) &
                                 (col("X").isNotNull()) &
                                 (col("Y").isNotNull())
)

#distance_udf = udf(get_distance, FloatType())
distance_df = filtered_df_a.withColumn("distance", get_distance(col("LAT"), col("LON"), col("Y"), col("X")))
distance_df = distance_df.withColumn("year", year("DATE OCC"))

final = distance_df.groupBy("year").agg(
    count("*").alias("#"),
    mean("distance").alias("average_distance")
).orderBy("year")
final = final.select("year", "average_distance", "#")

final.show()

#********************************************************************************************

df_b = spark.read.csv("hdfs://okeanos-master:54310/data/total_crime.csv" \
                ,header=True)

df_b = df_b.withColumn("AREA", col("AREA").cast(IntegerType()))

police_stations = spark.read.csv("hdfs://okeanos-master:54310/data/la_police_stations" \
,header=True)

police_stations = police_stations.withColumn("PREC", col("PREC").cast(IntegerType()))

df_b = df_b.select(df_b["LAT"], df_b["LON"], df_b["DATE OCC"], df_b["AREA"], df_b["Weapon Used Cd"])

joined_df_b = df_b.join(
    police_stations,
    df_b["AREA"] == police_stations["PREC"],
    "left"
)

#filter NULL weapons
filtered_df_b = joined_df_b.filter((col("Weapon Used Cd").isNotNull()) &
                                  ((col("LAT") != 0.0) & (col("LON") != 0.0)) &
                                 (col("X").isNotNull()) &
                                 (col("Y").isNotNull())
)

distance_df_b = filtered_df_b.withColumn("distance", get_distance(col("LAT"), col("LON"), col("Y"), col("X")))

final_b = distance_df_b.groupBy("DIVISION").agg(
    count("*").alias("#"),
    mean("distance").alias("average_distance")
).orderBy(col("#").cast("int").desc())

final_b = final_b.select("DIVISION", "average_distance", "#")

final_b.show(21, truncate=False)
