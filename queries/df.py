from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date
from pyspark.sql.types import IntegerType, DoubleType

spark = SparkSession.builder.appName("DF").getOrCreate()

crime_df = spark.read.csv("hdfs://okeanos-master:54310/data/crime_data_2010_2019" \
                              ,header=True)

crime2_df = spark.read.csv("hdfs://okeanos-master:54310/data/crime_data_2020_present" \
                ,header=True)

df = crime_df.union(crime2_df)

#specified columns
df = df.withColumn("Date Rptd", to_date(df["Date Rptd"], "MM/dd/yyyy hh:mm:ss a")) \
    .withColumn("DATE OCC", to_date(df["DATE OCC"], "MM/dd/yyyy hh:mm:ss a")) \
    .withColumn("Vict Age", df["Vict Age"].cast(IntegerType())) \
    .withColumn("LAT", df["LAT"].cast(DoubleType())) \
    .withColumn("LON", df["LON"].cast(DoubleType()))

row_count = df.count()
print("Number of rows:", row_count)

column_types = df.dtypes
print("Column Types:")
for col_name, col_type in column_types:
  print(f"{col_name}: {col_type}")


#create new csv: total_crime.csv and save it
path = "hdfs://okeanos-master:54310/data/total_crime.csv"
df.write.csv(path, header=True, mode="overwrite")
