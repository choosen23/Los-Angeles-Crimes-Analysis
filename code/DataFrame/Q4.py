from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, count, avg, udf
from pyspark.sql.types import DoubleType
import time
import math

start = time.time()
# Initialize a Spark session
spark = SparkSession.builder.appName("Q4-DataFrame") \
    .config("spark.eventLog.dir", "file:///tmp/spark-events") \
    .config("spark.history.fs.logDirectory", "file:///tmp/spark-events") \
    .getOrCreate()

# Load datasets
input_files = [
    "hdfs://master:9000/home/user/data/la_crimes_1.csv",
    "hdfs://master:9000/home/user/data/la_crimes_2.csv",
    "hdfs://master:9000/home/user/data/LAPD_Police_Stations.csv",
    "hdfs://master:9000/home/user/data/revgeocoding.csv"
]

# Load the CSV files into DataFrames
df1 = spark.read.csv(input_files[0], header=True, inferSchema=True)
df2 = spark.read.csv(input_files[1], header=True, inferSchema=True)

# Combine the DataFrames
crime_df = df1.union(df2)

stations_df = spark.read.csv(input_files[2], header=True, inferSchema=True)

# Filter crime data for firearm incidents (Weapon Used Cd starts with '1')
firearm_crime_df = crime_df.filter(col("Weapon Used Cd").cast("string").startswith("1"))

# Rename columns for join
firearm_crime_df = firearm_crime_df.withColumnRenamed('AREA ','AREA')
stations_df = stations_df.withColumnRenamed("PREC", "AREA")
stations_df = stations_df.withColumnRenamed("X", "STATION_LAT")
stations_df = stations_df.withColumnRenamed("Y", "STATION_LON")

# Filter out records with coordinates (0, 0) || Null Island
firearm_crime_df = firearm_crime_df.filter((col("LAT") != 0) & (col("LON") != 0))
stations_df = stations_df.filter((col("STATION_LAT") != 0) & (col("STATION_LON") != 0))


def distance_km(lat1, lon1, lat2, lon2):
    """
    Calculate the great-circle distance between two points on the Earth's surface using the Haversine formula.
    """
    R = 6371.0  # Radius of the Earth in kilometers
    lat1_rad = math.radians(lat1)
    lon1_rad = math.radians(lon1)
    lat2_rad = math.radians(lat2)
    lon2_rad = math.radians(lon2)
    delta_lat = lat2_rad - lat1_rad
    delta_lon = lon2_rad - lon1_rad
    a = math.sin(delta_lat / 2)**2 + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(delta_lon / 2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    distance = R * c
    return distance

distance_udf = udf(distance_km, DoubleType())

# Perform join and calculate distance
joined_df = firearm_crime_df.join(stations_df, "AREA") \
    .withColumn("distance", distance_udf(col("LAT"), col("LON"), col("STATION_LAT"), col("STATION_LON")))

# Aggregate results
result_df = joined_df.groupBy("AREA NAME") \
    .agg(avg("distance").alias("average_distance"), count("*").alias("incidents_total")) \
    .orderBy(col("incidents_total").desc())

# Show results
print("Results:")
result_df.show(truncate=False)

# Stop Spark session
spark.stop()

end = time.time()
print("Total time in seconds:", end - start)