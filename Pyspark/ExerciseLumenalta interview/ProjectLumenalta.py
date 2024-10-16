Tasks

Your task is to implement an ETL pipeline, which reads an input logs stream, merges additional information, transforms the data and finally, stores it in a new format.

Part 1: Data loaders

In app/etl.py implement the following functions:

1. load_logs - it should accept a path to a JSONL file, as defined in the Data section above, and returns a Spark DataFrame object containing logs data,

2. load_experiments - it should accept a path to a CSV file and returns a Spark DataFrame object containing expId and expName columns,

3. load_metrics - it should returns a Spark DataFrame object containing two rows and two columns, as defined in the Data section above.

Part 2: Merge data sources

Having the data loaded, your task is to merge the results into a single DataFrame. Implement join_tables function which accepts DataFrames from the previous task, and returns a new DataFrame with all three tables merged, using expId and metricId as foreign keys.

Part 3: Filter invalid logs

Some of the logs were ingested into the stream too late. We don't want that kind of behavior, and your task is to filter out logs, which were injected later than some specified number of hours after the creation date. Implement filter_late_logs method.

Part 4: Calculate experiment's statistics

Once you filtered out invalid logs, your task is to calculate final metrics for each metric in each experiment. The final metric value consists of the min and max values for each metric in each experiment. The expected DataFrame should consists of at least following columns:

• expName name of the experiment,

• metricName name of the metric,

• maxValue max value of the metricName in the expName,

minValue-min value of the metricName in A the expName.

Part 5: Save transformed data

Finally, the calculated statistics have to be saved in Parquet format. Ideally, the data should be partitioned by metricId column, so the analysis of the statistics in the future will be more efficient.

Experiments csv
expId, expName

0,FC32+relu

1,FC64+relu

2,FC32+tanh

3,FC64+tanh

Logs.jsonl

 ("logId":"5f2d8f14a746656e2427509b", "expId":0,"metricId":0,"valid":true,"createdAt":"2028-07-22T06:34:45", ingestedAt":"2020-07-23T16:09:58", "step":1,"value":0.25)
 
from functools import lru_cache
from pathlib import Path
import json
import csv

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    BooleanType,
    FloatType,
)
from pyspark.sql.functions import unix_timestamp, col, min, max


@lru_cache(maxsize=1)
def get_spark() -> SparkSession:
    return SparkSession.builder \
        .appName("ML Logs Transformer") \
        .master("local[1]") \
        .getOrCreate()


def load_logs(logs_path: Path) -> DataFrame:
    """
    Load logs from a JSONL file into a Spark DataFrame.
    """
    logs = []
    with logs_path.open() as f:
        for line in f:
            logs.append(json.loads(line))
    
    schema = StructType([
        StructField("logId", StringType(), True),
        StructField("expId", IntegerType(), True),
        StructField("metricId", IntegerType(), True),
        StructField("valid", BooleanType(), True),
        StructField("createdAt", StringType(), True),
        StructField("ingestedAt", StringType(), True),
        StructField("step", IntegerType(), True),
        StructField("value", FloatType(), True),
    ])
    
    return get_spark().createDataFrame(logs, schema)


def load_experiments(experiments_path: Path) -> DataFrame:
    """
    Load experiments from a CSV file into a Spark DataFrame.
    """
    experiments = []
    with experiments_path.open() as f:
        reader = csv.DictReader(f)
        for row in reader:
            experiments.append({
                "expId": int(row["expId"]),
                "expName": row["expName"]
            })
    
    schema = StructType([
        StructField("expId", IntegerType(), True),
        StructField("expName", StringType(), True)
    ])
    
    return get_spark().createDataFrame(experiments, schema)


def load_metrics() -> DataFrame:
    """
    Load metrics into a Spark DataFrame.
    """
    metrics = [
        {"metricId": 0, "metricName": "accuracy"},
        {"metricId": 1, "metricName": "loss"}
    ]
    
    schema = StructType([
        StructField("metricId", IntegerType(), True),
        StructField("metricName", StringType(), True)
    ])
    
    return get_spark().createDataFrame(metrics, schema)


def join_tables(logs: DataFrame, experiments: DataFrame, metrics: DataFrame) -> DataFrame:
    """
    Join logs, experiments, and metrics DataFrames.
    """
    logs_exp_df = logs.join(experiments, on="expId", how="inner")
    joined_df = logs_exp_df.join(metrics, on="metricId", how="inner")
    
    return joined_df


def filter_late_logs(data: DataFrame, hours: int) -> DataFrame:
    """
    Filter out logs that were ingested too late.
    """
    data_with_timestamps = data.withColumn("createdAtTs", unix_timestamp(col("createdAt"))) \
                               .withColumn("ingestedAtTs", unix_timestamp(col("ingestedAt")))

    data_with_difference = data_with_timestamps.withColumn("hoursDiff",
                                                           (col("ingestedAtTs") - col("createdAtTs")) / 3600)
    
    filtered_logs_df = data_with_difference.filter(col("hoursDiff") <= hours)
    final_filtered_df = filtered_logs_df.drop("createdAtTs", "ingestedAtTs", "hoursDiff")

    return final_filtered_df


def calculate_experiment_final_scores(data: DataFrame) -> DataFrame:
    """
    Calculate final metrics (min and max) for each metric in each experiment.
    """
    scores_df = data.groupBy("expName", "metricName") \
        .agg(
            min("value").alias("minValue"),
            max("value").alias("maxValue")
        )

    exp_metric_ids_df = data.select("expId", "metricId", "expName", "metricName").distinct()
    scores_with_ids_df = scores_df.join(exp_metric_ids_df, on=["expName", "metricName"], how="inner")

    final_scores_df = scores_with_ids_df.select(
        "expId",
        "metricId",
        "expName",
        "metricName",
        "maxValue",
        "minValue"
    )

    return final_scores_df


def save(data: DataFrame, output_path: Path):
    """
    Save the transformed data to a Parquet file, partitioned by metricId.
    """
    data.write.partitionBy("metricId").parquet(str(output_path))
