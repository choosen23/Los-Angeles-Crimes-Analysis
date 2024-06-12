from pyspark.sql import SparkSession
from datetime import datetime
import time

# Start time
start = time.time()

# Initialize a Spark session
spark = SparkSession.builder.appName("3TopMonthsPerYearCSV") \
    .config("spark.eventLog.dir", "file:///tmp/spark-events") \
    .config("spark.history.fs.logDirectory", "file:///tmp/spark-events") \
    .getOrCreate()

# Define the input CSV paths
input_files = [
    "hdfs://master:9000/home/user/data/la_crimes_1.csv",
    "hdfs://master:9000/home/user/data/la_crimes_2.csv"
]

# Load the CSV files into RDDs and combine them
rdd1 = spark.sparkContext.textFile(input_files[0])
rdd2 = spark.sparkContext.textFile(input_files[1])

# Extract header
header = rdd1.first()

# Remove the header from both RDDs
rdd1 = rdd1.filter(lambda row: row != header)
rdd2 = rdd2.filter(lambda row: row != header)

# Combine the RDDs
combined_rdd = rdd1.union(rdd2)

# Function to parse rows and filter rows where Premis Cd is 101
def parse_row(row):
    columns = row.split(',')
    try:
        if columns[15] == "101":
            time_occ = columns[10].zfill(4)
            hour = int(time_occ[:2])
            if 5 <= hour <= 11:
                part_day = "Morning"
            elif 12 <= hour <= 16:
                part_day = "Afternoon"
            elif 17 <= hour <= 20:
                part_day = "Evening"
            else:
                part_day = "Night"
            return (part_day, 1)
        else:
            return None
    except Exception as e:
        return None

# Parse and filter rows
parsed_rdd = combined_rdd.map(parse_row).filter(lambda x: x is not None)

# Aggregate the counts
part_day_counts = parsed_rdd.reduceByKey(lambda a, b: a + b)

# Sort by count in descending order
sorted_counts = part_day_counts.sortBy(lambda x: x[1], ascending=False)

# Collect the results
results = sorted_counts.collect()

# Print the results
for part_day, count in results:
    print(f"{part_day}: {count}")

# Stop the Spark session
spark.stop()

end = time.time()
print("Total time in seconds:", end - start)
