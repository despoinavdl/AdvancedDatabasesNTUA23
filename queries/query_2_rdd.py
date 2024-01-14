from pyspark.sql import SparkSession
from operator import add
import csv
import time
from io import StringIO

start_time = time.time()

sc = SparkSession \
.builder \
.appName("RDD query ") \
.getOrCreate() \
.sparkContext

# CSV parser (for rows with multiple ',')
def parse_csv(line):
    reader = csv.reader(StringIO(line))
    return next(reader)

crime_records = sc.textFile("hdfs://okeanos-master:54310/data/total_crime.csv") \
        .map(parse_csv)

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

part_of_day_rdd = crime_records.filter(lambda x: x[15] == 'STREET') \
    .map(lambda x: get_part_of_day(x[3]))

#count occurrences of each part of the day
part_of_day_counts = part_of_day_rdd.map(lambda x: (x, 1)).reduceByKey(add)

#sort by count in descending order
sorted_part_of_day = part_of_day_counts.map(lambda x: (x[1], x[0])) \
    .sortByKey(ascending=False)

#add rank
ranked_part_of_day = sorted_part_of_day.zipWithIndex() \
    .map(lambda x: (x[0][1], x[0][0], x[1] + 1))

for result in ranked_part_of_day.collect():
    print(result)

print('Total time for RDD: ',time.time() - start_time , 'sec')
sc.stop()
