from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, count, avg
import time
import math
import itertools




start = time.time()

# Initialize a Spark session
spark = SparkSession.builder.appName("3TopMonthsPerYearParquet") \
    .config("spark.eventLog.dir", "file:///tmp/spark-events") \
    .config("spark.history.fs.logDirectory", "file:///tmp/spark-events") \
    .getOrCreate()

# Load datasets
input_files = [
    "hdfs://master:9000/home/user/data/la_crimes_1.csv",
    "hdfs://master:9000/home/user/data/la_crimes_2.csv",
    "hdfs://master:9000/home/user/data/LAPD_Police_Stations.csv",
    "hdfs://master:9000/home/user/data/revgecoding.csv"
]

# Load the CSV files into DataFrames
df1 = spark.read.csv(input_files[0], header=True, inferSchema=True)
df2 = spark.read.csv(input_files[1], header=True, inferSchema=True)

# Combine the DataFrames
crime_df = df1.union(df2)

stations_df = spark.read.csv(input_files[2], header=True, inferSchema=True)
geocoding_df = spark.read.csv(input_files[3], header=True, inferSchema=True)

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


# Convert DataFrames to RDDs
firearm_crime_rdd = firearm_crime_df.rdd.map(lambda row: row.asDict())
stations_rdd = stations_df.rdd.map(lambda row: row.asDict())
#OK.


def distance_km(lat1, lon1, lat2, lon2):
    """
    Calculate the great-circle distance between two points on the Earth's surface using the Haversine formula.
    
    Parameters:
    lat1 (float): Latitude of the first point.
    lon1 (float): Longitude of the first point.
    lat2 (float): Latitude of the second point.
    lon2 (float): Longitude of the second point.

    Returns:
    float: Distance between the two points in kilometers.
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

# Repartition join
def repartition_join(firearm_rdd, stations_rdd):
    # Tag and partition the firearm crime data
    tagged_firearm_rdd = firearm_rdd.map(lambda row: (row['AREA'], ('R', row)))
    # Tag and partition the police stations data
    tagged_stations_rdd = stations_rdd.map(lambda row: (row['AREA'], ('S', row)))

    # Union the datasets
    combined_rdd = tagged_firearm_rdd.union(tagged_stations_rdd)
    
    # Repartition by join key
    partitioned_rdd = combined_rdd.partitionBy(combined_rdd.getNumPartitions())

    # Perform sort-merge join within each partition
    def sort_merge_join(partition):
        partition = sorted(partition, key=lambda x: x[0])
        result = []
        for key, group in itertools.groupby(partition, lambda x: x[0]):
            group = list(group)
            r_records = [x[1][1] for x in group if x[1][0] == 'R']
            s_records = [x[1][1] for x in group if x[1][0] == 'S']
            for r in r_records:
                for s in s_records:
                    distance = distance_km(r['LAT'], r['LON'], s['STATION_LAT'], s['STATION_LON'])
                    result.append((r['AREA NAME'], (distance, 1)))
        return iter(result)

    joined_rdd = partitioned_rdd.mapPartitions(sort_merge_join)
    return joined_rdd

# Broadcast join
def broadcast_join(firearm_rdd, stations_rdd):
    stations_map = stations_rdd.map(lambda row: (row['AREA'], row)).collectAsMap()
    broadcast_stations = spark.sparkContext.broadcast(stations_map)

    def join_func(crime):
        station = broadcast_stations.value.get(crime['AREA'])
        if station:
            distance = distance_km(crime['LAT'], crime['LON'], station['STATION_LAT'], station['STATION_LON'])
            return (crime['AREA NAME'], (distance, 1))
        else:
            return (None, (0, 0))

    joined_rdd = firearm_rdd.map(lambda row: join_func(row)).filter(lambda x: x[0] is not None)
    return joined_rdd

# Aggregate results
def aggregate_results(joined_rdd):
    aggregated_rdd = joined_rdd.reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))
    result_rdd = aggregated_rdd.map(lambda x: (x[0], x[1][0] / x[1][1], x[1][1]))
    return result_rdd

# Execute repartition join
repartition_joined_rdd = repartition_join(firearm_crime_rdd, stations_rdd)
repartition_results_rdd = aggregate_results(repartition_joined_rdd)

# Convert results to DataFrame and show
repartition_results_df = repartition_results_rdd.toDF(["division", "average_distance", "incidents_total"])

print("Repartition Join Results:")
repartition_results_df.show(truncate=False)

# Execute broadcast join
broadcast_joined_rdd = broadcast_join(firearm_crime_rdd, stations_rdd)
broadcast_results_rdd = aggregate_results(broadcast_joined_rdd)

# Convert results to DataFrame and show
broadcast_results_df = broadcast_results_rdd.toDF(["division", "average_distance", "incidents_total"])

print("Broadcast Join Results:")
broadcast_results_df.show(truncate=False)


# Stop Spark session
spark.stop()

end = time.time()
print("Total time in seconds:", end - start)
