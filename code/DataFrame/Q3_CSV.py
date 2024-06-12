import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, regexp_replace, first, desc, count
from pyspark.sql.types import IntegerType


start_time = time.time()

# Initialize a Spark session
spark = SparkSession.builder.appName("3TopMonthsPerYearParquet") \
    .config("spark.eventLog.dir", "file:///tmp/spark-events") \
    .config("spark.history.fs.logDirectory", "file:///tmp/spark-events") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Paths to datasets
crimes_path = "hdfs://master:9000/home/user/data/la_crimes_1.csv"
income_by_zip_path = "hdfs://master:9000/home/user/data/LA_income_2015.csv"
reverse_geocoding_path = "hdfs://master:9000/home/user/data/revgecoding.csv"

# Load the datasets
start_time = time.time()
crimes = spark.read.csv(crimes_path, header=True, inferSchema=True)
income_by_zip = spark.read.csv(income_by_zip_path, header=True, inferSchema=True)
reverse_geocoding = spark.read.csv(reverse_geocoding_path, header=True, inferSchema=True)
end_time = time.time()

crimes = crimes.withColumn("Year_OCC", year(col("DATE OCC")))
crimes_filtered = crimes \
    .filter((col("Vict Sex").isNotNull()) & \
     (col("Vict Descent").isNotNull()) & \
     (col("TIME OCC").startswith("2015")))


#ok 
# Join the crimes data with reverse geocoding to get the zip codes
crimes_with_zip = crimes_filtered.join(reverse_geocoding,
                                       (crimes_filtered.LAT == reverse_geocoding.LAT) &
                                       (crimes_filtered.LON == reverse_geocoding.LON),
                                       "left").select(crimes_filtered["*"], reverse_geocoding["ZIPcode"])



#ok


# Rename it to do the join
income_by_zip = income_by_zip \
    .withColumnRenamed("Zip Code", "ZIPcode")

# Drop rows with null values in the ZIPcode column
income_by_zip = income_by_zip.dropna(subset=["ZIPcode"])

# Join the crimes_with_zip data with income_by_zip to get income data
crimes_with_income = crimes_with_zip \
    .join(income_by_zip, "ZIPCode", "left")

#ok
# Explain the execution plan.
print("Explain 1st JOIN")
crimes_with_income.explain(mode="extended")

# Remove the dollar sign and commas from 'Estimated Median Income' and cast it to integer
income_by_zip = income_by_zip.withColumn("Estimated Median Income",
                                         regexp_replace(col("Estimated Median Income"), "[$,]", "").cast(IntegerType()))


# Join the crimes_with_zip data with income_by_zip to get income data
crimes_with_income = crimes_with_zip.join(income_by_zip, "ZIPcode", "left")

# Explain the execution plan.
print("Explain 2nd JOIN")
crimes_with_income.explain(mode="extended")


descent_code_mapping = {
    "A": "Other Asian", "B": "Black", "C": "Chinese", "D": "Cambodian",
    "F": "Filipino", "G": "Guamanian", "H": "Hispanic/Latin/Mexican",
    "I": "American Indian/Alaskan Native", "J": "Japanese", "K": "Korean",
    "L": "Laotian", "O": "Other", "P": "Pacific Islander", "S": "Samoan",
    "U": "Hawaiian", "V": "Vietnamese", "W": "White", "X": "Unknown", "Z": "Asian Indian"
}

#-------------
# Get the top 3 
top_3_zips = crimes_with_income \
    .groupBy("ZIPcode") \
    .agg(first("Estimated Median Income").alias("median_income")) \
    .orderBy(desc("median_income"))

# Filter crimes for these zip codes
top_3_crimes = crimes_with_income \
    .join(top_3_zips, "ZIPcode") 

print("Explain 3rd JOIN")
top_3_crimes.explain(mode="extended")

# Group by descent and count the number of victims for top 3 zip codes
top_3_victims_by_descent = top_3_crimes.replace(to_replace=descent_code_mapping, subset=["Vict Descent"]) \
                                       .groupBy("Vict Descent").agg(count("*").alias("total_victims")) \
                                       .orderBy(desc("total_victims"))

top_3_victims_by_descent.show()


#----------
# Get bottom 3
bottom_3_zips = crimes_with_income \
    .groupBy("ZIPcode") \
    .agg(first("Estimated Median Income").alias("median_income")) \
    .orderBy("median_income") \
    .limit(3)

# Filter crimes for these zip codes
bottom_3_crimes = crimes_with_income \
    .join(bottom_3_zips, "ZIPcode") 

# Group by descent and count the number of victims for top 3 zip codes
bottom_3_victims_by_descent = bottom_3_crimes.replace(to_replace=descent_code_mapping, subset=["Vict Descent"]) \
                                             .groupBy("Vict Descent").agg(count("*").alias("total_victims")) \
                                             .orderBy(desc("total_victims"))

bottom_3_victims_by_descent.show()






# # Experiment with different join strategies
# strategies = ["broadcast", "merge", "shuffle_hash", "shuffle_replicate_nl"]

# for strategy in strategies:
#     print(f"Using {strategy} join strategy")
    
#     # Join the crimes data with reverse geocoding using the specified strategy
#     start_time = time.time()
#     crimes_with_zip_strategy = crimes_filtered.join(reverse_geocoding.hint(strategy),
#                                                     (crimes_filtered.LAT == reverse_geocoding.latitude) &
#                                                     (crimes_filtered.LON == reverse_geocoding.longitude),
#                                                     "left").select(crimes_filtered["*"], reverse_geocoding["zip_code"])
#     end_time = time.time()
#     print(f"Join with reverse geocoding ({strategy}) time: {end_time - start_time} seconds")

#     # Join the crimes_with_zip_strategy data with income_by_zip to get income data
#     start_time = time.time()
#     crimes_with_income_strategy = crimes_with_zip_strategy.join(income_by_zip, "zip_code", "left")
#     end_time = time.time()
#     print(f"Join with income data ({strategy}) time: {end_time - start_time} seconds")

#     # Convert median_income to integer for proper comparison
#     crimes_with_income_strategy = crimes_with_income_strategy.withColumn("median_income", col("median_income").cast(IntegerType()))

#     # Filter crimes for these zip codes
#     start_time = time.time()
#     top_3_crimes_strategy = crimes_with_income_strategy.join(top_3_zips, "zip_code").filter(col("DATE OCC").startswith("2015"))
#     end_time = time.time()
#     print(f"Filter top 3 zip codes crimes ({strategy}) time: {end_time - start_time} seconds")

#     start_time = time.time()
#     bottom_3_crimes_strategy = crimes_with_income_strategy.join(bottom_3_zips, "zip_code").filter(col("DATE OCC").startswith("2015"))
#     end_time = time.time()
#     print(f"Filter bottom 3 zip codes crimes ({strategy}) time: {end_time - start_time} seconds")

#     # Group by descent and count the number of victims for top 3 zip codes
#     start_time = time.time()
#     top_3_victims_by_descent_strategy = top_3_crimes_strategy.groupBy("Vict Descent").agg(count("*").alias("total_victims")).orderBy(desc("total_victims"))
#     end_time = time.time()
#     print(f"Top 3 victims by descent calculation ({strategy}) time: {end_time - start_time} seconds")

#     # Group by descent and count the number of victims for bottom 3 zip codes
#     start_time = time.time()
#     bottom_3_victims_by_descent_strategy = bottom_3_crimes_strategy.groupBy("Vict Descent").agg(count("*").alias("total_victims")).orderBy(desc("total_victims"))
#     end_time = time.time()
#     print(f"Bottom 3 victims by descent calculation ({strategy}) time: {end_time - start_time} seconds")

#     # Show the results for the specified strategy
#     print(f"Results using {strategy} join strategy for Top 3 ZIP Codes:")
#     top_3_victims_by_descent_strategy.show(truncate=False)

#     print(f"Results using {strategy} join strategy for Bottom 3 ZIP Codes:")
#     bottom_3_victims_by_descent_strategy.show(truncate=False)

# Stop the Spark session
spark.stop()

end_time = time.time()
print("Total Time in seconds ",end_time - start_time)