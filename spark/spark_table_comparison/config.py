"""
Configuration file for table comparison project.
"""
import os

# Spark configuration
SPARK_APP_NAME = "TableComparison"
SPARK_MASTER = "local[*]"  # Use all available cores locally

# S3 configuration
S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://localhost:9000")  # MinIO by default
S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY", "minioadmin")
S3_SECRET_KEY = os.getenv("S3_SECRET_KEY", "minioadmin")
S3_BUCKET = os.getenv("S3_BUCKET", "comparison-reports")
S3_REGION = os.getenv("S3_REGION", "us-east-1")

# Input data paths (relative to data/ directory)
ORIGINAL_TABLE_PATH = "data/original_table.csv"
SHADOW_TABLE_PATH = "data/shadow_table.csv"

# Output configuration
OUTPUT_PATH = "s3a://{bucket}/comparison_reports/".format(bucket=S3_BUCKET)

# Comparison settings
PRIMARY_KEY_COLUMNS = None  # Set to list of column names, e.g., ["id"], or None for full row comparison
IGNORE_COLUMNS = []  # List of columns to ignore in comparison
IGNORE_NULL_DIFFERENCES = False  # If True, NULL and empty string are considered equal
