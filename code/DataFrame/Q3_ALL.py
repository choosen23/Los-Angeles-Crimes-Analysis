import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, regexp_replace, first, desc, count
from pyspark.sql.types import IntegerType


start_time = time.time()

# Initialize a Spark session
spark = SparkSession.builder.appName("AllStrategies") \
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
strategies = ["broadcast", "merge", "shuffle_hash", "shuffle_replicate_nl"]

for strategy in strategies:
    
    crimes = spark.read.csv(crimes_path, header=True, inferSchema=True)
    income_by_zip = spark.read.csv(income_by_zip_path, header=True, inferSchema=True)
    reverse_geocoding = spark.read.csv(reverse_geocoding_path, header=True, inferSchema=True)
    end_time = time.time()

    crimes = crimes.withColumn("Year_OCC", year(col("DATE OCC")))
    crimes_filtered = crimes \
        .filter((col("Vict Sex").isNotNull()) & \
        (col("Vict Descent").isNotNull()) & \
        (col("TIME OCC").startswith("2015")))


    # Experiment with different join strategies
    print(f"Using {strategy} join strategy")
    start_time_strategy = time.time()
    

    #ok 
    # Join the crimes data with reverse geocoding to get the zip codes
    crimes_with_zip = crimes_filtered.join(reverse_geocoding.hint(strategy),
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
        .join(income_by_zip.hint(strategy), "ZIPCode", "left")

    #ok
    # Explain the execution plan.
    # print("Explain 1st JOIN")
    # crimes_with_income.explain(mode="extended")

    # Remove the dollar sign and commas from 'Estimated Median Income' and cast it to integer
    income_by_zip = income_by_zip.withColumn("Estimated Median Income",
                                            regexp_replace(col("Estimated Median Income"), "[$,]", "").cast(IntegerType()))


    # Join the crimes_with_zip data with income_by_zip to get income data
    crimes_with_income = crimes_with_zip.join(income_by_zip.hint(strategy), "ZIPcode", "left")

    # Explain the execution plan.
    # print("Explain 2nd JOIN")
    # crimes_with_income.explain(mode="extended")


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

    # print("Explain 3rd JOIN")
    # top_3_crimes.explain(mode="extended")

    # Group by descent and count the number of victims for top 3 zip codes
    top_3_victims_by_descent = top_3_crimes.replace(to_replace=descent_code_mapping, subset=["Vict Descent"]) \
                                        .groupBy("Vict Descent").agg(count("*").alias("total_victims")) \
                                        .orderBy(desc("total_victims"))

    


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

    print("Top 3")
    top_3_victims_by_descent.show()

    print("Bottom 3")
    bottom_3_victims_by_descent.show()

    # bottom_3_victims_by_descent.show()
    end_time_strategy = time.time()
    print(f"Strategy {strategy} took {end_time_strategy - start_time_strategy} second.")

# Stop the Spark session
spark.stop()

end_time = time.time()
print("Total Time in seconds ",end_time - start_time)