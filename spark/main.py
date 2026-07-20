from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, max as spark_max,min as spark_min, sum as spark_sum, when,count,round
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, BooleanType, TimestampType
from pathlib import Path
import os
from pyspark.sql.functions import round
import shutil

spark_temp_dir = r"C:\SparkTemp"


if not os.path.exists(spark_temp_dir):
    os.makedirs(spark_temp_dir)


os.environ["TMPDIR"] = spark_temp_dir
os.environ["TEMP"] = spark_temp_dir
os.environ["TMP"] = spark_temp_dir
os.environ['HADOOP_HOME'] = r'C:\hadoop'
os.environ['PATH'] = r'C:\hadoop\bin' + os.pathsep + os.environ.get('PATH', '')

def main():
 
    spark = SparkSession.builder \
        .appName("F1_Historical_Batch_Analytics") \
        .getOrCreate()

    print("Spark Session created. Loading historical lap data...")

    lap_schema = StructType([
    StructField("Date", TimestampType(), True),
    StructField("SessionTime", StringType(), True),  
    StructField("DriverAhead", StringType(), True),
    StructField("DistanceToDriverAhead", DoubleType(), True),
    StructField("Time", StringType(), True),        
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
    historical_df = spark.read \
    .format("json") \
    .option("multiLine", "true") \
    .schema(lap_schema) \
    .load(str(s3_dir))

    print("Inferred Schema:")
    historical_df.printSchema()


   
    print("\n ==================LAP PERFORMANCE ANALYSIS=============")
    lap_analysis = (historical_df.groupBy("Lap").agg(round(avg("Speed"),2).alias("Average_Speed"),spark_max("Speed").alias("Maximum_Speed"),spark_min("Speed").alias("Minimum_Speed"),round(avg("RPM"),2).alias("Average_RPM"),spark_max("RPM").alias("Average_RPM"),spark_max("RPM"),round(avg("Throttle"),2).alias("Average_TRhrottle"),spark_sum(when(col("Brake")==True,1).otherwise(0)).alias("Brake_Events"),spark_sum(when(col("DRS")>0,1).otherwise(0)).alias("DRS_Activations"),spark_max("Distance").alias("Lap_Distance"),count("*").alias("Telemetry_Points")).orderBy("Lap"))
    lap_analysis.show(100, truncate= False)


    print("\n =============Fastest Laps========")
    lap_analysis.orderBy(
         col("Average_Speed").desc()
    ).show(10)

 
    batch_results = historical_df.groupBy("Lap").agg(
    spark_max("Speed").alias("Historical_Max_Speed"),
    avg("Speed").alias("Historical_Avg_Speed"),
    # Count how many telemetry ticks had the brake applied (Boolean True)
    spark_sum(when(col("Brake"), 1).otherwise(0)).alias("Braking_Events")
).orderBy("Lap")

  
    print("Batch Aggregation Complete:")
    batch_results.show()

    spark.stop()



if __name__ == "__main__":
    main()