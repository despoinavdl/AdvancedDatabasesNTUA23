from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import count, desc, year, row_number, col, regexp_replace
import time

start_time = time.time()

spark = SparkSession \
    .builder \
    .appName("DF query 3") \
    .getOrCreate()

income = spark.read.csv("hdfs://okeanos-master:54310/data/income/LA_income_2015.csv", header=True)
df = spark.read.csv("hdfs://okeanos-master:54310/data/total_crime.csv" \
                ,header=True)
revgecoding = spark.read.csv("hdfs://okeanos-master:54310/data/revgecoding.csv", header=True)

#filter data to include only 2015, and remove victimless crimes
df = df.filter(year(df["Date Rptd"]) == 2015)
df = df.filter(df["Vict Descent"] != "X")

#join based on longitude and latitude
joined_df = df.join(revgecoding.hint("merge"), (df.LON == revgecoding.LON) & (df.LAT == revgecoding.LAT), "left")
joined_df.explain(extended=True)

#join with revgeocoding
joined_df = joined_df.select(df["*"], revgecoding["ZIPcode"].alias("Joined_ZIP_Code"))

#join with income
joined_income_df = joined_df.join(income, joined_df["Joined_ZIP_Code"] == income["Zip Code"], "inner")

#remove dolar signs
joined_income_df = joined_income_df.withColumn(
    "Estimated Median Income",
    regexp_replace(col("Estimated Median Income"), "[^\d.]", "")
)
joined_income_df = joined_income_df.withColumn(
    "Estimated Median Income",
    col("Estimated Median Income").cast("float")
)

final_df = joined_income_df.select(joined_df["Vict Descent"], joined_df["Joined_ZIP_Code"], \
                                   joined_income_df["Estimated Median Income"])

grouped_final_df = final_df.groupBy('Vict Descent', 'Joined_ZIP_Code', 'Estimated Median Income') \
 .agg(F.count('*').alias('#'))

sorted_grouped_final_df = grouped_final_df.orderBy(desc("Estimated Median Income"))

unique_income_df = sorted_grouped_final_df.dropDuplicates(["Joined_ZIP_Code", "Estimated Median Income"])
sorted_unique_income_df_desc = unique_income_df.orderBy(desc("Estimated Median Income"))

#top 3
top_3_zip_codes = sorted_unique_income_df_desc.select("Joined_ZIP_Code")
top_3_zip_codes = top_3_zip_codes.limit(3)

#bottom 3
sorted_unique_income_df_asc = unique_income_df.orderBy("Estimated Median Income")
bot_3_zip_codes = sorted_unique_income_df_asc.select("Joined_ZIP_Code")
bot_3_zip_codes = bot_3_zip_codes.limit(3)


#filter for rows corresponding to the top 3 zip codes
filtered_top_3_df = grouped_final_df.join(
    top_3_zip_codes,
    grouped_final_df["Joined_ZIP_Code"] == top_3_zip_codes["Joined_ZIP_Code"],
    'inner'
)

#filter for rows corresponding to the bottom 3 zip codes
filtered_bot_3_df = grouped_final_df.join(
    bot_3_zip_codes,
    grouped_final_df["Joined_ZIP_Code"] == bot_3_zip_codes["Joined_ZIP_Code"],
    'inner'
)

top_result = filtered_top_3_df.groupBy('Vict Descent').agg(F.sum('#').alias('#')).orderBy(desc('#'))

bot_result = filtered_bot_3_df.groupBy('Vict Descent').agg(F.sum('#').alias('#')).orderBy(desc('#'))

#reform
map_descent = {
  "W": "White",
  "O": "Other",
  "B": "Black",
  "H": "Hispanic/Latin/Mexican",
  "A": "Other Asian",
  "C": "Chinese",
  "D": "Cambodian",
  "F": "Filipino",
  "G": "Guamanian",
  "I": "American Indian/Alaskan Native",
  "J": "Japanese",
  "L": "Laotian",
  "P": "Pacific Islander",
  "S": "Samoan",
  "U": "Hawaiian",
  "V": "Vietnamese",
  "Z": "Asian Indian"
}
map_function = F.udf(lambda x: map_descent.get(x))
top_result = top_result.withColumn("Vict Descent", map_function(df["Vict Descent"]))
bot_result = bot_result.withColumn("Vict Descent", map_function(df["Vict Descent"]))
top_result.show()
bot_result.show()
print('Total time: ',time.time() - start_time , 'sec')
