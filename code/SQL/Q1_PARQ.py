from pyspark.sql import SparkSession
import time

# Start time
start = time.time()

# Initialize a Spark session
spark = SparkSession.builder.appName("3TopMonthsPerYearParquet") \
    .config("spark.eventLog.dir", "file:///tmp/spark-events") \
    .config("spark.history.fs.logDirectory", "file:///tmp/spark-events") \
    .getOrCreate()

# Define the input Parquet paths
input_files = [
    "hdfs://master:9000/home/user/data/la_crimes_1.parquet",
    "hdfs://master:9000/home/user/data/la_crimes_2.parquet"
]

# Load the Parquet files into DataFrames
df1 = spark.read.parquet(input_files[0])
df2 = spark.read.parquet(input_files[1])

# Combine the DataFrames
df_combined = df1.union(df2)

# Register the combined DataFrame as a temporary SQL table
df_combined.createOrReplaceTempView("crimes")

# Use Spark SQL to extract year and month, and aggregate the data
query = """
    SELECT 
        year(to_timestamp(`DATE OCC`, 'MM/dd/yyyy hh:mm:ss a')) AS year,
        month(to_timestamp(`DATE OCC`, 'MM/dd/yyyy hh:mm:ss a')) AS month,
        COUNT(*) AS crime_total
    FROM crimes
    GROUP BY year, month
"""

crime_agg = spark.sql(query)
crime_agg.createOrReplaceTempView("crime_agg")

# Use Spark SQL to rank the months within each year and get the top 3
rank_query = """
    SELECT 
        year,
        month,
        crime_total,
        RANK() OVER (PARTITION BY year ORDER BY crime_total DESC) as ranking
    FROM crime_agg
"""

ranked_crimes = spark.sql(rank_query)
ranked_crimes.createOrReplaceTempView("ranked_crimes")

# Filter to get the top 3 months for each year
top3_query = """
    SELECT 
        year,
        month,
        crime_total,
        ranking
    FROM ranked_crimes
    WHERE ranking <= 3
    ORDER BY year ASC, crime_total DESC
"""

top3_crimes = spark.sql(top3_query)

# Show all results
top3_crimes.show(n=top3_crimes.count(), truncate=False)

# Stop the Spark session
spark.stop()

end = time.time()
print("Total time in seconds", end - start)
