from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract, col, to_timestamp, min, max, count
import os
import glob
import shutil
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns


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


spark = SparkSession.builder \
    .appName("ClusterUsageAnalysisLocal") \
    .master("local[*]") \
    .getOrCreate()

SAMPLE_DATA_DIR = "data/sample"
OUTPUT_DIR = "data/output/local"
os.makedirs(OUTPUT_DIR, exist_ok=True)

log_files = glob.glob(f"{SAMPLE_DATA_DIR}/**/*.log", recursive=True)
if not log_files:
    raise FileNotFoundError(
        f"err dir: {SAMPLE_DATA_DIR}\n"
    )
print(f"find {len(log_files)} logs，start analyze...")

logs_df = spark.read.text(log_files) \
    .withColumn("file_path", col("_metadata.file_path"))  


parsed_df = logs_df.select(
    col("value").alias("log_entry"),
    col("file_path"),
    regexp_extract("file_path", r"application_(\d+)_(\d+)", 1).alias("cluster_id"),
    regexp_extract("file_path", r"application_(\d+)_(\d+)", 2).alias("app_number"),
    regexp_extract("file_path", r"(application_\d+_\d+)", 1).alias("application_id")
)

start_time_df = parsed_df.filter(
    col("log_entry").contains("ApplicationMaster: Registered signal handlers") |
    col("log_entry").contains("ApplicationMaster: Starting the user application")
).select(
    col("cluster_id"),
    col("application_id"),
    col("app_number"),

    regexp_extract("log_entry", r"^(\d{2}/\d{2}/\d{2} \d{2}:\d{2}:\d{2})", 1).alias("start_time_str")
).withColumn(
    "start_time", 
    to_timestamp(col("start_time_str"), "yy/MM/dd HH:mm:ss")
).dropDuplicates(["application_id"])  

end_time_df = parsed_df.filter(
    col("log_entry").contains("ApplicationMaster: Final app status: SUCCEEDED") |
    col("log_entry").contains("ApplicationMaster: Application finished with state SUCCEEDED")
).select(
    col("application_id"),
    regexp_extract("log_entry", r"^(\d{2}/\d{2}/\d{2} \d{2}:\d{2}:\d{2})", 1).alias("end_time_str")
).withColumn(
    "end_time", 
    to_timestamp(col("end_time_str"), "yy/MM/dd HH:mm:ss")
).dropDuplicates(["application_id"])  

timeline_df = start_time_df.join(
    end_time_df, 
    on="application_id", 
    how="left"  
).select(
    "cluster_id",
    "application_id",
    "app_number",
    "start_time",
    "end_time"
).orderBy("start_time")

cluster_summary_df = timeline_df.groupBy("cluster_id").agg(
    count("application_id").alias("num_applications"),
    min("start_time").alias("cluster_first_app"),
    max("end_time").alias("cluster_last_app")
).orderBy("num_applications", ascending=False)


save_single_csv(timeline_df, OUTPUT_DIR, "problem2_timeline")

save_single_csv(cluster_summary_df, OUTPUT_DIR, "problem2_cluster_summary")

total_clusters = cluster_summary_df.count()
total_apps = timeline_df.count()
avg_apps_per_cluster = round(total_apps / total_clusters, 2) if total_clusters > 0 else 0

with open(f"{OUTPUT_DIR}/problem2_stats.txt", "w") as f:
    f.write(f"Total unique clusters: {total_clusters}\n")
    f.write(f"Total applications: {total_apps}\n")
    f.write(f"Average applications per cluster: {avg_apps_per_cluster}\n\n")
    f.write("Most heavily used clusters:\n")
    for row in cluster_summary_df.collect():
        f.write(f"  Cluster {row['cluster_id']}: {row['num_applications']} applications\n")

try:
    timeline_pd = timeline_df.toPandas()
    cluster_summary_pd = cluster_summary_df.toPandas()

    plt.figure(figsize=(10, 6))
    sns.barplot(
        data=cluster_summary_pd,
        x="cluster_id",
        y="num_applications",
        palette="viridis"
    )
    plt.title("Number of Applications per Cluster (Sample Data)")
    plt.xlabel("Cluster ID")
    plt.ylabel("Number of Applications")
    plt.xticks(rotation=45, ha="right") 
    for i, row in enumerate(cluster_summary_pd.itertuples()):
        plt.text(i, row.num_applications + 0.1, f"{row.num_applications}", ha="center")
    plt.tight_layout()
    plt.savefig(f"{OUTPUT_DIR}/problem2_bar_chart.png")
    plt.close()

    if not timeline_pd.empty and "start_time" in timeline_pd and "end_time" in timeline_pd:
        timeline_pd = timeline_pd.dropna(subset=["start_time", "end_time"])
        if not timeline_pd.empty:
            timeline_pd["duration_min"] = (
                (timeline_pd["end_time"] - timeline_pd["start_time"]).dt.total_seconds() / 60
            ).round(2)
            
            plt.figure(figsize=(10, 6))
            sns.histplot(
                data=timeline_pd,
                x="duration_min",
                kde=True,
                log_scale=True, 
                color="teal"
            )
            plt.title(f"Job Duration Distribution (n={len(timeline_pd)})")
            plt.xlabel("Duration (minutes, log scale)")
            plt.ylabel("Count")
            plt.tight_layout()
            plt.savefig(f"{OUTPUT_DIR}/problem2_density_plot.png")
            plt.close()
        else:
            print("empty data")
    else:
        print("empty timeline")

except Exception as e:
    print(f"err: {str(e)}")

spark.stop()