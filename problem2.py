from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract, col, min as spark_min, max as spark_max, count, to_timestamp, input_file_name
import os
import argparse
import shutil
import glob
import pandas as pd



def main():
    parser = argparse.ArgumentParser(description="Problem 2: Cluster Usage Analysis")
    parser.add_argument("spark_master", help="Spark master URL (e.g., spark://master-ip:7077)")
    parser.add_argument("--net-id", required=True, help="Your network ID (e.g., xl794)")
    args = parser.parse_args()

    output_dir = os.path.expanduser("~/spark-cluster/problem2_result")
    os.makedirs(output_dir, exist_ok=True)

    spark = SparkSession.builder \
        .appName(f"ClusterUsageAnalysis-{args.net_id}") \
        .master(args.spark_master) \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .getOrCreate()

    s3_path = f"s3a://{args.net_id}-assignment-spark-cluster-logs/data/application_*/*.log"
    logs_df = spark.read.text(s3_path)

    # 添加文件路径列
    logs_with_path = logs_df.withColumn("file_path", input_file_name())

    # 从路径提取cluster_id和application_id，从内容提取时间戳
    parsed_df = logs_with_path.select(
        regexp_extract(col("file_path"), r'application_(\d+)_(\d+)', 1).alias("cluster_id"),
        regexp_extract(col("file_path"), r'application_(\d+)_(\d+)', 0).alias("application_id"),
        regexp_extract(col("file_path"), r'application_\d+_(\d+)', 1).alias("app_number"),
        regexp_extract(col("value"), r'^(\d{2}/\d{2}/\d{2} \d{2}:\d{2}:\d{2})', 1).alias("timestamp_str")
    ).filter(
        (col("cluster_id") != "") & 
        (col("timestamp_str") != "")
    )

    # 转换时间格式
    parsed_df = parsed_df.withColumn(
        "timestamp",
        to_timestamp(col("timestamp_str"), "yy/MM/dd HH:mm:ss")
    )

    # 按应用分组，获取开始和结束时间
    timeline_df = parsed_df.groupBy("cluster_id", "application_id", "app_number").agg(
        spark_min("timestamp").alias("start_time"),
        spark_max("timestamp").alias("end_time")
    ).orderBy("cluster_id", "app_number")

    # 生成集群摘要
    cluster_summary = timeline_df.groupBy("cluster_id").agg(
        count("application_id").alias("num_applications"),
        spark_min("start_time").alias("cluster_first_app"),
        spark_max("end_time").alias("cluster_last_app")
    ).orderBy(col("num_applications").desc())

    # 计算统计信息
    total_clusters = timeline_df.select("cluster_id").distinct().count()
    total_apps = timeline_df.count()
    avg_apps = total_apps / total_clusters if total_clusters > 0 else 0
    cluster_stats = cluster_summary.collect()

    # 保存stats.txt
    stats_path = f"{output_dir}/problem2_stats.txt"
    with open(stats_path, "w") as f:
        f.write(f"Total unique clusters: {total_clusters}\n")
        f.write(f"Total applications: {total_apps}\n")
        f.write(f"Average applications per cluster: {avg_apps:.2f}\n\n")
        f.write("Most heavily used clusters:\n")
        for row in cluster_stats:
            f.write(f"  Cluster {row['cluster_id']}: {row['num_applications']} applications\n")

    print(f"Problem 2 completed! Results saved to: {output_dir}")

    timeline_df.show(timeline_df.count(), truncate=False)
    # cluster_summary.show(truncate=False)

    def save_single_csv(df, output_dir, output_name):
        pandas_df = df.toPandas()
        pandas_df.to_csv(f"{output_dir}/{output_name}.csv", index=False)

    save_single_csv(timeline_df, output_dir, "problem2_timeline")
    save_single_csv(cluster_summary, output_dir, "problem2_cluster_summary")

    spark.stop()


def generate_visualizations_from_csv(output_dir):

    import pandas as pd
    import matplotlib.pyplot as plt
    import seaborn as sns
    
    timeline_path = f"{output_dir}/problem2_timeline.csv"
    

    df = pd.read_csv(timeline_path)
    df['start_time'] = pd.to_datetime(df['start_time'])
    df['end_time'] = pd.to_datetime(df['end_time'])
    df['duration_seconds'] = (df['end_time'] - df['start_time']).dt.total_seconds()
    

    plt.figure(figsize=(12, 6))
    cluster_counts = df.groupby('cluster_id').size().sort_values(ascending=False)
    
    colors = sns.color_palette("husl", len(cluster_counts))
    bars = plt.bar(range(len(cluster_counts)), cluster_counts.values, color=colors)
    
    for bar, count in zip(bars, cluster_counts.values):
        plt.text(bar.get_x() + bar.get_width()/2, bar.get_height() + max(cluster_counts)*0.01,
                str(count), ha='center', va='bottom', fontsize=10)
    
    plt.xlabel('Cluster ID', fontsize=12)
    plt.ylabel('Number of Applications', fontsize=12)
    plt.title('Number of Applications per Cluster', fontsize=14, fontweight='bold')
    plt.xticks(range(len(cluster_counts)), cluster_counts.index, rotation=45, ha='right')
    plt.tight_layout()
    
    bar_chart_path = f"{output_dir}/problem2_bar_chart.png"
    plt.savefig(bar_chart_path, dpi=300, bbox_inches='tight')
    plt.close()

    

    largest_cluster = cluster_counts.idxmax()
    largest_cluster_data = df[df['cluster_id'] == largest_cluster]
    
    valid_durations = largest_cluster_data[largest_cluster_data['duration_seconds'] > 0]['duration_seconds']
    
    plt.figure(figsize=(12, 6))
    
    sns.histplot(valid_durations, bins=50, kde=True, color='skyblue', 
                 edgecolor='black', line_kws={'linewidth': 2, 'color': 'red'})
    
    plt.xlabel('Job Duration (seconds, log scale)', fontsize=12)
    plt.ylabel('Frequency', fontsize=12)
    plt.xscale('log')
    plt.title(f'Job Duration Distribution for Cluster {largest_cluster}\n(n={len(valid_durations)})',
              fontsize=14, fontweight='bold')
    plt.tight_layout()
    
    density_plot_path = f"{output_dir}/problem2_density_plot.png"
    plt.savefig(density_plot_path, dpi=300, bbox_inches='tight')
    plt.close()



if __name__ == "__main__":
    # main()
    generate_visualizations_from_csv("data/output")