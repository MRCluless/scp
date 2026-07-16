from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, max as spark_max,min as spark_min, sum as spark_sum, when,count,round
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, BooleanType, TimestampType
from pathlib import Path
import os
from pyspark.sql.functions import round
import shutil

spark_temp_dir = r"C:\SparkTemp"

# Create the folder if it doesn't exist
if not os.path.exists(spark_temp_dir):
    os.makedirs(spark_temp_dir)

# Override the temp paths for this process
os.environ["TMPDIR"] = spark_temp_dir
os.environ["TEMP"] = spark_temp_dir
os.environ["TMP"] = spark_temp_dir
os.environ['HADOOP_HOME'] = r'C:\hadoop'
os.environ['PATH'] = r'C:\hadoop\bin' + os.pathsep + os.environ.get('PATH', '')

def main():
    # 1. Initialize the Spark Session
    # In a real EMR environment, this automatically connects to the cluster
    spark = SparkSession.builder \
        .appName("F1_Historical_Batch_Analytics") \
        .getOrCreate()

    print("Spark Session created. Loading historical lap data...")

    lap_schema = StructType([
    StructField("Date", TimestampType(), True),
    StructField("SessionTime", StringType(), True),  # Kept as string due to '0 days...' format
    StructField("DriverAhead", StringType(), True),
    StructField("DistanceToDriverAhead", DoubleType(), True),
    StructField("Time", StringType(), True),         # Kept as string due to '0 days...' format
    StructField("RPM", DoubleType(), True),
    StructField("Speed", DoubleType(), True),
    StructField("nGear", IntegerType(), True),
    StructField("Throttle", DoubleType(), True),
    StructField("Brake", BooleanType(), True),
    StructField("DRS", IntegerType(), True),
    StructField("Source", StringType(), True),
    StructField("Distance", DoubleType(), True),
    StructField("RelativeDistance", DoubleType(), True),
    StructField("Status", StringType(), True),
    StructField("X", DoubleType(), True),
    StructField("Y", DoubleType(), True),
    StructField("Z", DoubleType(), True),
    StructField("Lap", IntegerType(), True)
])

    # 2. Load the JSON files
    # PySpark can read a single file, a wildcard (*), or a whole S3 bucket directory
    # Update this path to where your JSON files are stored (e.g., "s3://your-bucket/laps/*.json")
    s3_dir = "s3://25140990-scp-f1-data/*/*.json"
    # s3_dir = Path(__file__).resolve().parent.parent / "s3"
    # historical_df = spark.read.load(str(s3_dir))
    historical_df = spark.read \
    .format("json") \
    .option("multiLine", "true") \
    .schema(lap_schema) \
    .load(str(s3_dir))

    # Optional: Print the schema to prove Spark understood your JSON structure
    print("Inferred Schema:")
    historical_df.printSchema()


      #Lap Analysis
    print("\n ==================LAP PERFORMANCE ANALYSIS=============")
    lap_analysis = (historical_df.groupBy("Lap").agg(round(avg("Speed"),2).alias("Average_Speed"),spark_max("Speed").alias("Maximum_Speed"),spark_min("Speed").alias("Minimum_Speed"),round(avg("RPM"),2).alias("Average_RPM"),spark_max("RPM").alias("Average_RPM"),spark_max("RPM"),round(avg("Throttle"),2).alias("Average_TRhrottle"),spark_sum(when(col("Brake")==True,1).otherwise(0)).alias("Brake_Events"),spark_sum(when(col("DRS")>0,1).otherwise(0)).alias("DRS_Activations"),spark_max("Distance").alias("Lap_Distance"),count("*").alias("Telemetry_Points")).orderBy("Lap"))
    lap_analysis.show(100, truncate= False)


    print("\n =============Fastest Laps========")
    lap_analysis.orderBy(
         col("Average_Speed").desc()
    ).show(10)

    # 3. Compute the Meaningful Aggregate (Batch Processing)
    # We group by Lap and calculate key performance metrics
    batch_results = historical_df.groupBy("Lap").agg(
    spark_max("Speed").alias("Historical_Max_Speed"),
    avg("Speed").alias("Historical_Avg_Speed"),
    # Count how many telemetry ticks had the brake applied (Boolean True)
    spark_sum(when(col("Brake"), 1).otherwise(0)).alias("Braking_Events")
).orderBy("Lap")

    # 4. Show the results (Data is processed and parallelized at this point)
    print("Batch Aggregation Complete:")
    batch_results.show()

    # 5. Stop the session to free up resources
    spark.stop()



if __name__ == "__main__":
    main()