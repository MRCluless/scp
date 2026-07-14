from pyspark.sql import SparkSession

from pyspark.sql.functions import (
    col,
    avg,
    max as spark_max,
    min as spark_min,
    sum as spark_sum,
    count,
    countDistinct,
    when,
    current_timestamp,
    stddev,
    round as spark_round
)

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
    BooleanType,
    TimestampType
)

from pyspark import StorageLevel

from pathlib import Path

import os
import time
import platform
import builtins


os.environ["PYSPARK_PYTHON"] = "python"

os.environ["PYSPARK_DRIVER_PYTHON"] = "python"




spark_temp_dir = r"C:\SparkTemp"


os.makedirs(
    spark_temp_dir,
    exist_ok=True
)


os.environ["TMPDIR"] = spark_temp_dir
os.environ["TEMP"] = spark_temp_dir
os.environ["TMP"] = spark_temp_dir



os.environ["HADOOP_HOME"] = r"C:\hadoop"


os.environ["PATH"] = (
    r"C:\hadoop\bin"
    +
    os.pathsep
    +
    os.environ.get(
        "PATH",
        ""
    )
)


def main():


    batch_start_time = time.time()


    spark = (

        SparkSession.builder

        .appName(
            "F1_Lambda_Batch_Layer"
        )

        .config(
            "spark.sql.shuffle.partitions",
            "200"
        )

        .config(
            "spark.default.parallelism",
            "200"
        )

        .config(
            "spark.sql.adaptive.enabled",
            "true"
        )

        .config(
            "spark.sql.adaptive.coalescePartitions.enabled",
            "true"
        )

        .getOrCreate()

<<<<<<< HEAD
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
    s3_dir = Path(__file__).resolve().parent.parent / "s3"
    # historical_df = spark.read.load(str(s3_dir))
    historical_df = spark.read \
    .format("json") \
    .option("multiLine", "true") \
    .schema(lap_schema) \
    .load(str(s3_dir))

    print("Inferred Schema:")
    historical_df.printSchema()
=======
    )
>>>>>>> ad50bbf (Batch Layer analytics)


    print(
        "\n========== F1 BATCH STARTED =========="
    )


    print(
        "Spark Version:",
        spark.version
    )


    print(
        "Python Version:",
        platform.python_version()
    )



    schema = StructType([


        StructField(
            "Date",
            TimestampType(),
            True
        ),


        StructField(
            "SessionTime",
            StringType(),
            True
        ),


        StructField(
            "DriverAhead",
            StringType(),
            True
        ),


        StructField(
            "DistanceToDriverAhead",
            DoubleType(),
            True
        ),


        StructField(
            "Time",
            StringType(),
            True
        ),


        StructField(
            "RPM",
            DoubleType(),
            True
        ),


        StructField(
            "Speed",
            DoubleType(),
            True
        ),


        StructField(
            "nGear",
            IntegerType(),
            True
        ),


        StructField(
            "Throttle",
            DoubleType(),
            True
        ),


        StructField(
            "Brake",
            BooleanType(),
            True
        ),


        StructField(
            "DRS",
            IntegerType(),
            True
        ),


        StructField(
            "Source",
            StringType(),
            True
        ),


        StructField(
            "Distance",
            DoubleType(),
            True
        ),


        StructField(
            "RelativeDistance",
            DoubleType(),
            True
        ),


        StructField(
            "Status",
            StringType(),
            True
        ),


        StructField(
            "X",
            DoubleType(),
            True
        ),


        StructField(
            "Y",
            DoubleType(),
            True
        ),


        StructField(
            "Z",
            DoubleType(),
            True
        ),


        StructField(
            "Lap",
            IntegerType(),
            True
        )

    ])



    input_path = (

        Path(__file__)
        .resolve()
        .parent
        .parent
        /
        "s3"

    )



    print(
        "Reading raw telemetry from:",
        input_path
    )



    df = (

        spark.read

        .format(
            "json"
        )

        .option(
            "multiLine",
            "true"
        )

        .schema(
            schema
        )

        .load(
            str(input_path)
        )

    )



    print(
        "\nRaw telemetry loaded"
    )


    df.printSchema()



    clean_start = time.time()



    df = (

        df

        .dropDuplicates()


        .dropna(

            subset=[

                "Lap",
                "Speed",
                "RPM"

            ]

        )


        .filter(

            col("Status")
            ==
            "OnTrack"

        )


        .withColumn(

            "processed_time",

            current_timestamp()

        )

    )



    df = (

        df

        .repartition(
            "Lap"
        )

    )




    df = (

        df.persist(

            StorageLevel.MEMORY_AND_DISK

        )

    )



    total_records = df.count()



    clean_time = (

        time.time()
        -
        clean_start

    )



    print(
        "\nClean Records:",
        total_records
    )


    print(
        "Cleaning Time:",
        builtins.round(
            clean_time,
            3
        ),
        "seconds"
    )




    output = "batch_output"


    parquet_output = (

        output
        +
        "/parquet"

    )


    csv_output = (

        output
        +
        "/csv"

    )


    os.makedirs(
        output,
        exist_ok=True
    )

    lap_start = time.time()


    lap_analysis = (

        df

        .groupBy(
            "Lap"
        )

        .agg(

            spark_round(
                avg("Speed"),
                2
            )
            .alias(
                "Average_Speed"
            ),


            spark_max("Speed")
            .alias(
                "Maximum_Speed"
            ),


            spark_min("Speed")
            .alias(
                "Minimum_Speed"
            ),


            spark_round(
                avg("RPM"),
                2
            )
            .alias(
                "Average_RPM"
            ),


            spark_max("RPM")
            .alias(
                "Maximum_RPM"
            ),


            spark_round(
                avg("Throttle"),
                2
            )
            .alias(
                "Average_Throttle"
            ),


            spark_sum(

                when(
                    col("Brake") == True,
                    1
                )

                .otherwise(0)

            )
            .alias(
                "Brake_Events"
            ),



            spark_sum(

                when(
                    col("DRS") > 0,
                    1
                )

                .otherwise(0)

            )
            .alias(
                "DRS_Activations"
            ),



            spark_sum(
                "Distance"
            )
            .alias(
                "Lap_Distance"
            ),



            count("*")
            .alias(
                "Telemetry_Points"
            )

        )

    )


    print(
        "Lap analytics completed:",
        builtins.round(
            time.time()-lap_start,
            3
        ),
        "seconds"
    )



    race_summary = (

        df

        .agg(

            countDistinct("Lap")
            .alias(
                "Total_Laps"
            ),


            spark_round(
                avg("Speed"),
                2
            )
            .alias(
                "Average_Speed"
            ),


            spark_max("Speed")
            .alias(
                "Maximum_Speed"
            ),


            spark_round(
                avg("RPM"),
                2
            )
            .alias(
                "Average_RPM"
            ),


            spark_max("RPM")
            .alias(
                "Maximum_RPM"
            ),


            spark_round(
                avg("Throttle"),
                2
            )
            .alias(
                "Average_Throttle"
            ),


            spark_sum("Distance")
            .alias(
                "Total_Distance"
            ),


            count("*")
            .alias(
                "Total_Telemetry_Points"
            )

        )

    )


    gear_analysis = (

        df

        .groupBy(
            "nGear"
        )

        .agg(

            count("*")
            .alias(
                "Gear_Usage_Count"
            ),


            spark_round(
                avg("Speed"),
                2
            )
            .alias(
                "Gear_Average_Speed"
            ),


            spark_round(
                avg("RPM"),
                2
            )
            .alias(
                "Gear_Average_RPM"
            )

        )

    )




    braking_analysis = (

        df

        .filter(
            col("Brake") == True
        )

        .groupBy(
            "Lap"
        )

        .agg(

            count("*")
            .alias(
                "Brake_Events"
            ),


            spark_round(
                avg("Speed"),
                2
            )
            .alias(
                "Average_Braking_Speed"
            ),


            spark_round(
                avg("RPM"),
                2
            )
            .alias(
                "Average_Braking_RPM"
            )

        )

    )




    drs_analysis = (

        df

        .groupBy(
            "Lap"
        )

        .agg(

            spark_sum(

                when(
                    col("DRS") > 0,
                    1
                )

                .otherwise(0)

            )
            .alias(
                "DRS_Activations"
            ),


            count("*")
            .alias(
                "Total_Points"
            )

        )

    )



    drs_analysis = (

        drs_analysis

        .withColumn(

            "DRS_Percentage",

            spark_round(

                (

                    col("DRS_Activations")
                    /
                    col("Total_Points")

                )
                *
                100,

                2

            )

        )

    )






    speed_zone = (

        df

        .withColumn(

            "Speed_Category",

            when(
                col("Speed") < 100,
                "LOW"
            )

            .when(
                col("Speed") < 200,
                "MEDIUM"
            )

            .when(
                col("Speed") < 300,
                "HIGH"
            )

            .otherwise(
                "VERY_HIGH"
            )

        )

        .groupBy(
            "Speed_Category"
        )

        .agg(

            count("*")
            .alias(
                "Count"
            ),


            spark_round(
                avg("Speed"),
                2
            )
            .alias(
                "Average_Speed"
            )

        )

    )






    sector_analysis = (

        df

        .withColumn(

            "Sector",

            when(
                col("Distance") < 2000,
                "Sector_1"
            )

            .when(
                col("Distance") < 4000,
                "Sector_2"
            )

            .otherwise(
                "Sector_3"
            )

        )

        .groupBy(
            "Lap",
            "Sector"
        )

        .agg(

            spark_round(
                avg("Speed"),
                2
            )
            .alias(
                "Sector_Average_Speed"
            ),


            spark_round(
                avg("RPM"),
                2
            )
            .alias(
                "Sector_Average_RPM"
            ),


            count("*")
            .alias(
                "Telemetry_Points"
            )

        )

    )

    lap_consistency = (

        df

        .groupBy(
            "Lap"
        )

        .agg(

            spark_round(
                stddev("Speed"),
                2
            )
            .alias(
                "Speed_Variation"
            ),


            spark_round(
                stddev("RPM"),
                2
            )
            .alias(
                "RPM_Variation"
            ),


            spark_round(
                avg("Speed"),
                2
            )
            .alias(
                "Average_Speed"
            )

        )

    )



    lap_consistency = (

        lap_consistency

        .withColumn(

            "Consistency_Score",

            spark_round(

                100 -

                (

                    col("Speed_Variation")
                    /
                    col("Average_Speed")
                    *
                    100

                ),

                2

            )

        )

    )



    performance_trend = (

        lap_analysis

        .withColumn(

            "Performance_Status",

            when(
                col("Average_Speed") >= 220,
                "GOOD"
            )

            .when(
                col("Average_Speed") >= 180,
                "NORMAL"
            )

            .otherwise(
                "DEGRADED"
            )

        )

    )




    anomalies = (

        df

        .withColumn(

            "Anomaly_Type",

            when(
                col("Speed") > 350,
                "EXTREME_SPEED"
            )

            .when(
                col("RPM") > 12500,
                "RPM_LIMIT"
            )

            .when(
                col("Brake") == True,
                "BRAKING_EVENT"
            )

            .when(
                col("Throttle") == 0,
                "THROTTLE_DROP"
            )

            .otherwise(
                None
            )

        )

        .filter(

            col("Anomaly_Type")
            .isNotNull()

        )

    )



    print(
        "\n========== ALL ANALYTICS CREATED =========="
    )







    print(
        "\n========== WRITING BATCH OUTPUTS =========="
    )


    datasets = {


        "lap_analysis":
        lap_analysis,


        "race_summary":
        race_summary,


        "gear_analysis":
        gear_analysis,


        "braking_analysis":
        braking_analysis,


        "drs_analysis":
        drs_analysis,


        "speed_zone":
        speed_zone,


        "sector_analysis":
        sector_analysis,


        "lap_consistency":
        lap_consistency,


        "performance_trend":
        performance_trend,


        "anomalies":
        anomalies

    }



    for name, dataset in datasets.items():


        print(
            "Writing:",
            name
        )


        dataset.write \
            .mode(
                "overwrite"
            ) \
            .parquet(

                parquet_output
                +
                "/"
                +
                name

            )



    print(
        "\nBatch parquet views created"
    )








    print(
        "\n========== CREATING SERVING LAYER =========="
    )



    speed_layer_path = (

        Path(__file__)
        .resolve()
        .parent
        .parent
        /
        "speed_output"

    )




    if speed_layer_path.exists():


        speed_df = (

            spark.read

            .format(
                "json"
            )

            .load(
                str(speed_layer_path)
            )

        )


        print(
            "Speed layer loaded"
        )


    else:


        print(
            "No speed layer found"
        )


        speed_df = spark.createDataFrame(
            [],
            lap_analysis.schema
        )





    batch_columns = set(
        lap_analysis.columns
    )


    for c in speed_df.columns:


        if c in batch_columns and c != "Lap":

            speed_df = speed_df.drop(c)





    serving_view = (

        lap_analysis

        .join(

            speed_df,

            on="Lap",

            how="left"

        )

    )



    serving_view = (

        serving_view

        .dropDuplicates()

    )




    serving_view.write \
        .mode(
            "overwrite"
        ) \
        .parquet(

            parquet_output
            +
            "/serving_view"

        )



    print(
        "Serving layer created"
    )




    batch_end_time = time.time()



    execution_time = (

        batch_end_time
        -
        batch_start_time

    )



    throughput = (

        total_records
        /
        execution_time

    )





    performance_metrics = spark.createDataFrame(

        [

            (

                "F1_Batch",

                int(total_records),

                float(execution_time),

                float(throughput),

                int(
                    spark.sparkContext
                    .defaultParallelism
                )

            )

        ],


        [

            "Job_Name",

            "Records_Processed",

            "Execution_Time_Seconds",

            "Records_Per_Second",

            "Spark_Parallelism"

        ]

    )






    performance_metrics.write \
        .mode(
            "overwrite"
        ) \
        .parquet(

            parquet_output
            +
            "/performance_metrics"

        )



    print(
        "Performance metrics saved"
    )









    print(
        "\n========== SERVING PREVIEW =========="
    )



    print(
        "\n--- LAP ANALYSIS ---"
    )


    lap_analysis.show(
        10,
        truncate=False
    )



    print(
        "\n--- ANOMALIES ---"
    )


    anomalies.show(
        10,
        truncate=False
    )



    print(
        "\n--- PERFORMANCE ---"
    )


    performance_metrics.show(
        truncate=False
    )








    print(
        "\n========== CSV EXPORT =========="
    )



    for name, dataset in datasets.items():


        dataset.write \
            .mode(
                "overwrite"
            ) \
            .option(
                "header",
                "true"
            ) \
            .csv(

                csv_output
                +
                "/"
                +
                name

            )




    serving_view.write \
        .mode(
            "overwrite"
        ) \
        .option(
            "header",
            "true"
        ) \
        .csv(

            csv_output
            +
            "/serving_view"

        )





    performance_metrics.write \
        .mode(
            "overwrite"
        ) \
        .option(
            "header",
            "true"
        ) \
        .csv(

            csv_output
            +
            "/performance"

        )



    print(
        "\nCSV Export Completed"
    )



    print(
        csv_output
    )





    print(
        "\n========== F1 BATCH COMPLETED =========="
    )


    print(
        "Total Records:",
        total_records
    )


    print(
        "Execution Time:",
        builtins.round(
            execution_time,
            3
        ),
        "seconds"
    )


    print(
        "Throughput:",
        builtins.round(
            throughput,
            2
        ),
        "records/sec"
    )



    spark.stop()






if __name__ == "__main__":

    main()