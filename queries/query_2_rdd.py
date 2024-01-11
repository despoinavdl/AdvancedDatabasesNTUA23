from pyspark.sql import SparkSession
from operator import add
import csv
from io import StringIO

# Create a SparkContext
sc = SparkSession \
.builder \
.appName("RDD query ") \
.getOrCreate() \
.sparkContext

# Read the CSV file with a CSV parser
def parse_csv(line):
    # Use the csv module to handle parsing
    reader = csv.reader(StringIO(line))
    return next(reader)

# Read the CSV file
crime_records = sc.textFile("hdfs://okeanos-master:54310/data/total_crime.csv") \
        .map(parse_csv)


# Define a function to determine the part of the day
def get_part_of_day(time_occ):
    time_occ = int(time_occ)
    if 500 <= time_occ < 1200:
        return 'morning'
    elif 1200 <= time_occ < 1700:
        return 'afternoon'
    elif 1700 <= time_occ < 2100:
        return 'evening'
    else:
        return 'night'

# Filter and map to get the part of the day
part_of_day_rdd = crime_records.filter(lambda x: x[15] == 'STREET') \
    .map(lambda x: get_part_of_day(x[3]))

# Count occurrences of each part of the day
part_of_day_counts = part_of_day_rdd.map(lambda x: (x, 1)).reduceByKey(add)

# Sort by count in descending order
sorted_part_of_day = part_of_day_counts.map(lambda x: (x[1], x[0])) \
    .sortByKey(ascending=False)

# Add rank using zipWithIndex
ranked_part_of_day = sorted_part_of_day.zipWithIndex() \
    .map(lambda x: (x[0][1], x[0][0], x[1] + 1))

# Display the result
for result in ranked_part_of_day.collect():
    print(result)

# Stop the SparkContext
sc.stop()
