from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract, col, rand, round
import os
import glob
import shutil


spark = SparkSession.builder \
    .appName("LogLevelAnalysisLocal") \
    .master("local[*]") \
    .getOrCreate()

SAMPLE_DATA_DIR = "data/sample"
OUTPUT_DIR = "data/output/local"
os.makedirs(OUTPUT_DIR, exist_ok=True)

log_files = glob.glob(f"{SAMPLE_DATA_DIR}/**/*.log", recursive=True)
if not log_files:
    raise FileNotFoundError(f"no log check dir: {SAMPLE_DATA_DIR}\n"
                          f"dir should be : data/sample/application_*/container_*.log")
print(f"find {len(log_files)} log，start analyze...")

logs_df = spark.read.text(log_files)

parsed_df = logs_df.select(
    col("value").alias("log_entry"),
    regexp_extract(
        col("value"), 
        r'^(\d{2}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}) (INFO|WARN|ERROR|DEBUG)', 
        2  
    ).alias("log_level")
)

level_counts = parsed_df.groupBy("log_level").count() \
    .filter(col("log_level").isNotNull() & (col("log_level") != "")) \
    .orderBy("count", ascending=False)

sample_entries = parsed_df.filter(col("log_level").isNotNull() & (col("log_level") != "")) \
    .orderBy(rand()) \
    .limit(10) \
    .select("log_entry", "log_level")


total_lines = logs_df.count()
total_with_level = parsed_df.filter(col("log_level").isNotNull() & (col("log_level") != "")).count()
unique_levels = level_counts.count() if level_counts.count() > 0 else 0


level_pct = level_counts.withColumn(
    "percentage", 
    round(col("count") / total_with_level * 100, 2)
)

def save_single_csv(df, output_dir, output_name):
    temp_dir = f"{output_dir}/{output_name}_temp"
    if os.path.exists(temp_dir):
        shutil.rmtree(temp_dir)
    df.coalesce(1).write \
        .mode("overwrite") \
        .option("header", "true") \
        .csv(temp_dir)
    csv_files = glob.glob(f"{temp_dir}/*.csv")
    if not csv_files:
        raise FileNotFoundError(f"CSV no found: {temp_dir}")
    os.rename(csv_files[0], f"{output_dir}/{output_name}.csv")
    shutil.rmtree(temp_dir)

save_single_csv(level_counts, OUTPUT_DIR, "problem1_counts")

save_single_csv(sample_entries, OUTPUT_DIR, "problem1_sample")

with open(f"{OUTPUT_DIR}/problem1_summary.txt", "w") as f:
    f.write(f"Total log lines processed: {total_lines:,}\n")
    f.write(f"Total lines with log levels: {total_with_level:,}\n")
    f.write(f"Unique log levels found: {unique_levels}\n\n")
    f.write("Log level distribution:\n")
    for row in level_pct.collect():
        f.write(f"  {row['log_level']:6}: {row['count']:,} ({row['percentage']}%)\n")

spark.stop()