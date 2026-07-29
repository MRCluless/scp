from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, round, input_file_name, regexp_extract

def main():
    spark = SparkSession.builder \
        .appName("Batch Layer") \
	.config(
            "spark.jars.packages", 
            "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262"
        ) \
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider", 
            "com.amazonaws.auth.InstanceProfileCredentialsProvider"
        ) \
	.config("spark.hadoop.fs.s3a.threads.keepalivetime", "60") \
        .config("spark.hadoop.fs.s3a.connection.timeout", "60000") \
        .config("spark.hadoop.fs.s3a.connection.establish.timeout", "60000") \
        .config("spark.hadoop.fs.s3a.multipart.purge.age", "86400") \
        .getOrCreate()
    s3_path = "s3a://25140990-scp-f1-data"
    df = spark.read.parquet(f"{s3_path}/*/*/*.parquet")
    df_with_driver = df.withColumn(
        "Driver", 
        regexp_extract(input_file_name(), r"([^/]+)(?=\.parquet$)", 1)
    ).withColumn(
        "Track",
        regexp_extract(input_file_name(), r"/([^/]+)/[^/]+\.parquet$", 1)
    )

    batch_view = df_with_driver.groupBy("Track", "Driver").agg(
        round(avg("Speed"), 2).alias("historical_avg_speed"),
        round(avg("RPM"), 2).alias("historical_avg_rpm")
    )

    batch_view.show()

    s3_output_path = "s3a://25140990-scp-f1-data/batch_output/2025_historical_baselines"
    print(f"Batch processing complete. Saving to: {s3_output_path}")
    batch_view.write.mode("overwrite").json(s3_output_path)
    spark.stop()

if __name__ == "__main__":
    main()