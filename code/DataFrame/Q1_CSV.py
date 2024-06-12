from pyspark.sql import SparkSession
from pyspark.sql.functions import year, month, col, count
from pyspark.sql.window import Window
import pyspark.sql.functions as F
import time

# Start time
start = time.time()

# Initialize a Spark session
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

# Load the CSV files into DataFrames
df1 = spark.read.csv(input_files[0], header=True, inferSchema=True)
df2 = spark.read.csv(input_files[1], header=True, inferSchema=True)

# Combine the DataFrames
df_combined = df1.union(df2)

# Extract year and month from the 'DATE OCC' column
df_combined = df_combined.withColumn("year", year(F.to_timestamp("DATE OCC", "MM/dd/yyyy hh:mm:ss a")))
df_combined = df_combined.withColumn("month", month(F.to_timestamp("DATE OCC", "MM/dd/yyyy hh:mm:ss a")))

# Aggregate the data to get the total crimes per year and month
crime_agg = df_combined.groupBy("year", "month").agg(count("*").alias("crime_total"))

# Define a window for ranking
windowSpec = Window.partitionBy("year").orderBy(col("crime_total").desc())

# Rank the months within each year
crime_ranked = crime_agg.withColumn("ranking", F.rank().over(windowSpec))

# Filter to get the top 3 months for each year
top3_crimes = crime_ranked.filter(col("ranking") <= 3)

# Sort the results by year (ascending) and crime_total (descending)
sorted_top3_crimes = top3_crimes.orderBy(col("year").asc(), col("crime_total").desc())

# Show all results
sorted_top3_crimes.show(n=sorted_top3_crimes.count(), truncate=False)

# Stop the Spark session
spark.stop()

end = time.time()
print("Total time in seconds:", end - start)
