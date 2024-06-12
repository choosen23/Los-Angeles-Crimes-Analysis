from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import os

# Initialize a Spark session
spark = SparkSession \
    .builder \
    .appName("CSV_to_Parquet") \
    .config("spark.eventLog.dir", "file:///tmp/spark-events") \
    .config("spark.history.fs.logDirectory", "file:///tmp/spark-events") \
    .getOrCreate()

#MINIMIZE LOGGING OUTPUT
spark.sparkContext.setLogLevel("WARN")

# Define the input directory containing the CSV files and the output directory for the Parquet files
input_directory = "hdfs://master:9000/home/user/data"
output_directory = "hdfs://master:9000/home/user/data"

# Use Hadoop FileSystem API to list all CSV files in the input directory
fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
path = spark._jvm.org.apache.hadoop.fs.Path(input_directory)
file_statuses = fs.listStatus(path)

csv_files = [f.getPath().toString() for f in file_statuses if f.getPath().toString().endswith('.csv')]

# Read each CSV file, convert to Parquet, and save to the output directory
for csv_file in csv_files:
    # Read the CSV file into a DataFrame
    df = spark.read.csv(csv_file, header=True, inferSchema=True)
    
    # Generate the output Parquet file path
    file_name = os.path.basename(csv_file)
    parquet_file_name = file_name.replace('.csv', '.parquet')
    output_path = os.path.join(output_directory, parquet_file_name)
    
    # Write the DataFrame to Parquet format
    # df.write.mode("overwrite").parquet(output_path)
    df.write.parquet(output_path)
