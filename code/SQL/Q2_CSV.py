from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp,col, hour, count, when, lpad, substring
import time

# Start time
start = time.time()

# Initialize a Spark session
spark = SparkSession.builder.appName("3TopMonthsPerYearParquet") \
    .config("spark.eventLog.dir", "file:///tmp/spark-events") \
    .config("spark.history.fs.logDirectory", "file:///tmp/spark-events") \
    .getOrCreate()


# Define the input CSV paths
input_files = [
    "hdfs://master:9000/home/user/data/la_crimes_1.csv",
    "hdfs://master:9000/home/user/data/la_crimes_2.csv"
]

# Load the CSV files into DataFrames
df1 = spark.read.csv(input_files[0], header=True, inferSchema=True)
df2 = spark.read.csv(input_files[1], header=True, inferSchema=True)

# Combine the DataFrames
df_combined = df1.union(df2)

# Filter the DataFrame to include only rows where Premis Cd is 101
df_filtered = df_combined.filter(col("Premis Cd") == "101")

# Ensure TIME OCC is a string and pad with zeros if necessary
df_filtered = df_filtered.withColumn("TIME OCC", lpad(col("TIME OCC"), 4, '0'))

# Extract the hour from TIME OCC
df_filtered = df_filtered.withColumn("Hour", substring(col("TIME OCC"), 1, 2).cast("int"))

# Define the part of day based on the hour
df_filtered = df_filtered.withColumn("part_day", 
                   when((col("Hour") >= 5) & (col("Hour") <= 11), "Morning")
                   .when((col("Hour") >= 12) & (col("Hour") <= 16), "Afternoon")
                   .when((col("Hour") >= 17) & (col("Hour") <= 20), "Evening")
                   .otherwise("Night"))




# Group by the part_day, count occurrences, and order by the count in descending order
top_part_day_df = df_filtered.groupBy("part_day").count().orderBy(col("count").desc())

# Show the result
top_part_day_df.show(truncate=False)

# Stop the Spark session
spark.stop()

end = time.time()
print("Total time in seconds:", end - start)