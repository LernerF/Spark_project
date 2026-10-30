"""
Report generation and S3 upload utilities.
"""
import json
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType, BooleanType, ArrayType


class ReportGenerator:
    """Generate and save comparison reports."""
    
    def __init__(self, spark: SparkSession, output_path: str):
        """
        Initialize report generator.
        
        Args:
            spark: SparkSession
            output_path: S3 path for reports (e.g., s3a://bucket/reports/)
        """
        self.spark = spark
        self.output_path = output_path.rstrip("/")
        self.timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    def save_json_report(self, report: dict, report_name: str = "comparison_report") -> str:
        """
        Save report as JSON file to S3.
        
        Args:
            report: Report dictionary
            report_name: Base name for the report file
            
        Returns:
            Full path to saved report
        """
        file_path = f"{self.output_path}/{report_name}_{self.timestamp}.json"
        
        # Convert report to JSON string
        json_content = json.dumps(report, indent=2, ensure_ascii=False, default=str)
        
        # Create DataFrame with JSON content
        schema = StructType([StructField("content", StringType(), True)])
        df = self.spark.createDataFrame([(json_content,)], schema)
        
        # Write as single file
        df.coalesce(1).write.mode("overwrite").text(file_path)
        
        return file_path
    
    def save_summary_csv(self, report: dict, report_name: str = "comparison_summary") -> str:
        """
        Save summary report as CSV file to S3.
        
        Args:
            report: Report dictionary
            report_name: Base name for the report file
            
        Returns:
            Full path to saved report
        """
        file_path = f"{self.output_path}/{report_name}_{self.timestamp}.csv"
        
        # Flatten report for CSV
        csv_rows = []
        
        # Header
        csv_rows.append(("comparison_type", "metric", "original_value", "shadow_value", "status"))
        
        # Schema differences
        schema_diff = report.get("schema_differences", {})
        csv_rows.append(("schema", "columns_count", 
                        str(schema_diff.get("original_table_columns", 0)),
                        str(schema_diff.get("shadow_table_columns", 0)),
                        "MATCH" if not schema_diff.get("has_schema_differences", False) else "DIFFER"))
        
        csv_rows.append(("schema", "columns_only_in_original", 
                        str(len(schema_diff.get("columns_only_in_original", []))),
                        "-",
                        "INFO"))
        
        csv_rows.append(("schema", "columns_only_in_shadow",
                        "-",
                        str(len(schema_diff.get("columns_only_in_shadow", []))),
                        "INFO"))
        
        csv_rows.append(("schema", "type_differences",
                        str(len(schema_diff.get("type_differences", []))),
                        "-",
                        "WARN" if len(schema_diff.get("type_differences", [])) > 0 else "OK"))
        
        # Row differences
        row_diff = report.get("row_differences", {})
        csv_rows.append(("rows", "total_rows",
                        str(row_diff.get("original_table_rows", 0)),
                        str(row_diff.get("shadow_table_rows", 0)),
                        "MATCH" if row_diff.get("match", False) else "DIFFER"))
        
        csv_rows.append(("rows", "row_count_difference",
                        str(row_diff.get("difference", 0)),
                        "-",
                        "OK" if row_diff.get("difference", 0) == 0 else "WARN"))
        
        # Content differences
        content_diff = report.get("content_differences", {})
        csv_rows.append(("content", "matching_rows",
                        str(content_diff.get("matching_rows", 0)),
                        "-",
                        "INFO"))
        
        csv_rows.append(("content", "different_rows",
                        str(content_diff.get("different_rows", 0)),
                        "-",
                        "OK" if content_diff.get("different_rows", 0) == 0 else "WARN"))
        
        csv_rows.append(("content", "only_in_original",
                        str(content_diff.get("only_in_original", 0)),
                        "-",
                        "OK" if content_diff.get("only_in_original", 0) == 0 else "WARN"))
        
        csv_rows.append(("content", "only_in_shadow",
                        str(content_diff.get("only_in_shadow", 0)),
                        "-",
                        "OK" if content_diff.get("only_in_shadow", 0) == 0 else "WARN"))
        
        # Summary
        summary = report.get("summary", {})
        csv_rows.append(("summary", "comparison_passed",
                        str(summary.get("comparison_passed", False)),
                        "-",
                        "PASS" if summary.get("comparison_passed", False) else "FAIL"))
        
        issues = summary.get("issues_found", [])
        csv_rows.append(("summary", "issues_count",
                        str(len(issues)),
                        "-",
                        "OK" if len(issues) == 0 else "FAIL"))
        
        # Create DataFrame
        schema = StructType([
            StructField("comparison_type", StringType(), True),
            StructField("metric", StringType(), True),
            StructField("original_value", StringType(), True),
            StructField("shadow_value", StringType(), True),
            StructField("status", StringType(), True)
        ])
        
        df = self.spark.createDataFrame(csv_rows, schema)
        df.coalesce(1).write.mode("overwrite").option("header", "true").csv(file_path)
        
        return file_path
    
    def save_detailed_csv(self, report: dict, report_name: str = "comparison_detailed") -> str:
        """
        Save detailed report as CSV file to S3.
        
        Args:
            report: Report dictionary
            report_name: Base name for the report file
            
        Returns:
            Full path to saved report
        """
        file_path = f"{self.output_path}/{report_name}_{self.timestamp}.csv"
        
        csv_rows = []
        
        # Schema details
        schema_diff = report.get("schema_differences", {})
        
        # Columns only in original
        for item in schema_diff.get("columns_only_in_original", []):
            csv_rows.append(("schema", "column_missing_in_shadow", 
                           item["column"], item["type"], "original"))
        
        # Columns only in shadow
        for item in schema_diff.get("columns_only_in_shadow", []):
            csv_rows.append(("schema", "column_missing_in_original",
                           item["column"], item["type"], "shadow"))
        
        # Type differences
        for item in schema_diff.get("type_differences", []):
            csv_rows.append(("schema", "type_mismatch",
                           item["column"], 
                           f"{item['original_type']} -> {item['shadow_type']}",
                           "both"))
        
        # Summary issues
        summary = report.get("summary", {})
        for issue in summary.get("issues_found", []):
            csv_rows.append(("summary", "issue", issue, "-", "both"))
        
        # If no detailed data, add header only
        if not csv_rows:
            csv_rows.append(("info", "no_detailed_differences", "-", "-", "both"))
        
        # Create DataFrame
        schema = StructType([
            StructField("category", StringType(), True),
            StructField("issue_type", StringType(), True),
            StructField("column_name", StringType(), True),
            StructField("details", StringType(), True),
            StructField("table", StringType(), True)
        ])
        
        df = self.spark.createDataFrame(csv_rows, schema)
        df.coalesce(1).write.mode("overwrite").option("header", "true").csv(file_path)
        
        return file_path
    
    def generate_and_save_all_reports(self, report: dict) -> dict:
        """
        Generate and save all report formats.
        
        Args:
            report: Full comparison report dictionary
            
        Returns:
            Dictionary with paths to all generated reports
        """
        paths = {}
        
        # Save JSON report
        paths["json_report"] = self.save_json_report(report, "comparison_report")
        
        # Save summary CSV
        paths["summary_csv"] = self.save_summary_csv(report, "comparison_summary")
        
        # Save detailed CSV
        paths["detailed_csv"] = self.save_detailed_csv(report, "comparison_detailed")
        
        return paths
