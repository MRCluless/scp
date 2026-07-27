from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, round, input_file_name, regexp_extract
from dotenv import load_dotenv

load_dotenv()

def main():
    spark = SparkSession.builder \
        .appName("Batch Layer") \
        .master("local[*]") \
        .getOrCreate()
    
    s3_path = "s3a://25140990-scp-f1-data"
    
    df = spark.read.parquet(f"../2025/*/*/*.parquet")

    df_with_driver = df.withColumn(
        "Driver", 
        regexp_extract(input_file_name(), r"([^/]+)(?=\.parquet$)", 1)
    )

    batch_view = df_with_driver.groupBy("Driver").agg(
        round(avg("Speed"), 2).alias("historical_avg_speed"),
        round(avg("RPM"), 2).alias("historical_avg_rpm")
    )

    batch_view.show()

    output_directory = "batch_output/2025_historical_baselines"
    batch_view.write.mode("overwrite").parquet(output_directory)
    print(f"Batch processing complete. Saved to {output_directory}")
    spark.stop()

if __name__ == "__main__":

    main()