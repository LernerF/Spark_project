"""
Spark session and S3 configuration utilities.
"""
from pyspark.sql import SparkSession
from config import (
    SPARK_APP_NAME,
    SPARK_MASTER,
    S3_ACCESS_KEY,
    S3_SECRET_KEY,
    S3_ENDPOINT,
    S3_REGION
)


def create_spark_session():
    """Create and configure Spark session with S3 support."""
    spark = (
        SparkSession.builder
        .appName(SPARK_APP_NAME)
        .master(SPARK_MASTER)
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", S3_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", S3_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.region", S3_REGION)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )
    return spark


def configure_s3_hadoop(spark):
    """Configure Hadoop for S3 access."""
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    hadoop_conf.set("fs.s3a.endpoint", S3_ENDPOINT)
    hadoop_conf.set("fs.s3a.access.key", S3_ACCESS_KEY)
    hadoop_conf.set("fs.s3a.secret.key", S3_SECRET_KEY)
    hadoop_conf.set("fs.s3a.region", S3_REGION)
    hadoop_conf.set("fs.s3a.path.style.access", "true")
    hadoop_conf.set("fs.s3a.connection.ssl.enabled", "false")
