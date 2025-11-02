from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract, col, rand
from pyspark.sql.functions import round as spark_round
import os
import argparse
import shutil
import glob
import pandas as pd

def main():
    parser = argparse.ArgumentParser(description="Problem 1: Log Level Distribution Analysis")
    parser.add_argument("spark_master", help="Spark master URL (e.g., spark://master-ip:7077)")
    parser.add_argument("--net-id", required=True, help="Your network ID (e.g., xl794)")
    args = parser.parse_args()

    output_dir = os.path.expanduser("~/spark-cluster/problem1_result")
    os.makedirs(output_dir, exist_ok=True)

    spark = SparkSession.builder \
        .appName(f"LogLevelAnalysis-{args.net_id}") \
        .master(args.spark_master) \
        .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3.S3FileSystem") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3n.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem") \
        .getOrCreate()

    s3_path = f"s3a://{args.net_id}-assignment-spark-cluster-logs/data/application_*/*.log"
    print(f"read from : {s3_path}")
    logs_df = spark.read.text(s3_path)

    parsed_df = logs_df.select(
        col("value").alias("log_entry"),
        regexp_extract(
            col("value"),
            r'^(\d{2}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}) (INFO|WARN|ERROR|DEBUG)',
            2  
        ).alias("log_level")
    )

    valid_levels = parsed_df.filter(
        col("log_level").isNotNull() & (col("log_level") != "")
    )

    level_counts = valid_levels.groupBy("log_level").count() \
        .orderBy("count", ascending=False)

    sample_entries = valid_levels.orderBy(rand(seed=42))  \
        .limit(10) \
        .select("log_entry", "log_level")


    total_lines = logs_df.count()
    total_with_level = valid_levels.count()
    unique_levels = level_counts.count()

    level_pct = level_counts.withColumn(
        "percentage",
        spark_round(col("count") / total_with_level * 100, 2)
    )


    level_counts.show(truncate=False)

    sample_entries.show(truncate=False)

    def save_single_csv(df, output_dir, output_name):
        pandas_df = df.toPandas()
        pandas_df.to_csv(f"{output_dir}/{output_name}.csv", index=False)

    save_single_csv(level_counts, output_dir, "problem1_counts")
    save_single_csv(sample_entries, output_dir, "problem1_sample")

    summary_path = f"{output_dir}/problem1_summary.txt"
    with open(summary_path, "w") as f:
        f.write(f"Total log lines processed: {total_lines:,}\n")
        f.write(f"Total lines with log levels: {total_with_level:,}\n")
        f.write(f"Unique log levels found: {unique_levels}\n\n")
        f.write("Log level distribution:\n")
        for row in level_pct.collect():
            f.write(f"  {row['log_level']:6}: {row['count']:,} ({row['percentage']}%)\n")

    spark.stop()
    print(f"Problem 1 done， saved in  {output_dir}")

if __name__ == "__main__":
    main()