"""
Core comparison logic for table comparison.
"""
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, concat_ws, sha2, when, count, isnan, isnull
from pyspark.sql.types import StructType, StructField
import json
from datetime import datetime


class TableComparator:
    """Universal table comparator for migration validation."""
    
    def __init__(self, spark, primary_key_columns=None, ignore_columns=None, ignore_null_differences=False):
        """
        Initialize comparator.
        
        Args:
            spark: SparkSession
            primary_key_columns: List of column names to use as primary key. If None, uses full row hash.
            ignore_columns: List of columns to exclude from comparison
            ignore_null_differences: If True, NULL and empty string are considered equal
        """
        self.spark = spark
        self.primary_key_columns = primary_key_columns or []
        self.ignore_columns = ignore_columns or []
        self.ignore_null_differences = ignore_null_differences
        self.report = {
            "comparison_timestamp": datetime.now().isoformat(),
            "schema_differences": None,
            "row_differences": None,
            "content_differences": None,
            "summary": {}
        }
    
    def compare_schemas(self, original_df: DataFrame, shadow_df: DataFrame) -> dict:
        """
        Compare schemas of two DataFrames.
        
        Returns:
            Dictionary with schema differences
        """
        original_schema = {field.name: field.dataType.simpleString() 
                          for field in original_df.schema.fields}
        shadow_schema = {field.name: field.dataType.simpleString() 
                        for field in shadow_df.schema.fields}
        
        all_columns = set(original_schema.keys()) | set(shadow_schema.keys())
        
        differences = {
            "original_table_columns": len(original_schema),
            "shadow_table_columns": len(shadow_schema),
            "columns_only_in_original": [],
            "columns_only_in_shadow": [],
            "type_differences": [],
            "identical_columns": []
        }
        
        for col_name in all_columns:
            in_original = col_name in original_schema
            in_shadow = col_name in shadow_schema
            
            if in_original and not in_shadow:
                differences["columns_only_in_original"].append({
                    "column": col_name,
                    "type": original_schema[col_name]
                })
            elif in_shadow and not in_original:
                differences["columns_only_in_shadow"].append({
                    "column": col_name,
                    "type": shadow_schema[col_name]
                })
            elif original_schema[col_name] != shadow_schema[col_name]:
                differences["type_differences"].append({
                    "column": col_name,
                    "original_type": original_schema[col_name],
                    "shadow_type": shadow_schema[col_name]
                })
            else:
                differences["identical_columns"].append(col_name)
        
        self.report["schema_differences"] = differences
        
        # Update summary
        has_differences = (
            len(differences["columns_only_in_original"]) > 0 or
            len(differences["columns_only_in_shadow"]) > 0 or
            len(differences["type_differences"]) > 0
        )
        differences["has_schema_differences"] = has_differences
        
        return differences
    
    def compare_row_counts(self, original_df: DataFrame, shadow_df: DataFrame) -> dict:
        """
        Compare row counts between two DataFrames.
        
        Returns:
            Dictionary with row count differences
        """
        original_count = original_df.count()
        shadow_count = shadow_df.count()
        
        differences = {
            "original_table_rows": original_count,
            "shadow_table_rows": shadow_count,
            "difference": abs(original_count - shadow_count),
            "match": original_count == shadow_count
        }
        
        self.report["row_differences"] = differences
        return differences
    
    def _get_comparison_columns(self, df: DataFrame) -> list:
        """Get columns to use for comparison, excluding ignored columns."""
        return [c for c in df.columns if c not in self.ignore_columns]
    
    def _add_row_hash(self, df: DataFrame, columns: list, hash_col_name: str = "row_hash") -> DataFrame:
        """Add a hash column based on specified columns."""
        # Handle null values in hash calculation
        df_hashed = df.withColumn(
            hash_col_name,
            sha2(concat_ws("|", *[col(c) for c in columns]), 256)
        )
        return df_hashed
    
    def compare_content(self, original_df: DataFrame, shadow_df: DataFrame) -> dict:
        """
        Compare content of two DataFrames row by row.
        
        Returns:
            Dictionary with content differences
        """
        # Get common columns for comparison
        original_cols = set(original_df.columns)
        shadow_cols = set(shadow_df.columns)
        common_columns = list(original_cols & shadow_cols)
        comparison_columns = [c for c in common_columns if c not in self.ignore_columns]
        
        if not comparison_columns:
            return {
                "error": "No common columns found for comparison",
                "matching_rows": 0,
                "different_rows": 0,
                "only_in_original": 0,
                "only_in_shadow": 0
            }
        
        # Select only comparison columns
        original_subset = original_df.select(*comparison_columns)
        shadow_subset = shadow_df.select(*comparison_columns)
        
        # Use primary key if specified
        if self.primary_key_columns:
            pk_cols = [c for c in self.primary_key_columns if c in comparison_columns]
            if pk_cols:
                return self._compare_with_primary_key(original_subset, shadow_subset, pk_cols, comparison_columns)
        
        # Otherwise, use row hash comparison
        return self._compare_by_row_hash(original_subset, shadow_subset, comparison_columns)
    
    def _compare_by_row_hash(self, original_df: DataFrame, shadow_df: DataFrame, columns: list) -> dict:
        """Compare tables using row hash."""
        # Add row hashes
        original_hashed = self._add_row_hash(original_df, columns, "row_hash")
        shadow_hashed = self._add_row_hash(shadow_df, columns, "row_hash")
        
        # Get distinct hashes
        original_hashes = original_hashed.select("row_hash").distinct()
        shadow_hashes = shadow_hashed.select("row_hash").distinct()
        
        # Find differences
        only_in_original = original_hashes.subtract(shadow_hashes).count()
        only_in_shadow = shadow_hashes.subtract(original_hashes).count()
        
        # Find matching rows
        matching_hashes = original_hashes.intersect(shadow_hashes)
        matching_count = matching_hashes.count()
        
        differences = {
            "comparison_method": "row_hash",
            "matching_rows": matching_count,
            "different_rows": only_in_original + only_in_shadow,
            "only_in_original": only_in_original,
            "only_in_shadow": only_in_shadow,
            "total_original_rows": original_df.count(),
            "total_shadow_rows": shadow_df.count()
        }
        
        self.report["content_differences"] = differences
        return differences
    
    def _compare_with_primary_key(self, original_df: DataFrame, shadow_df: DataFrame, 
                                   pk_columns: list, comparison_columns: list) -> dict:
        """Compare tables using primary key."""
        # Add row hashes for non-PK columns
        non_pk_columns = [c for c in comparison_columns if c not in pk_columns]
        
        if non_pk_columns:
            original_hashed = self._add_row_hash(original_df, non_pk_columns, "content_hash")
            shadow_hashed = self._add_row_hash(shadow_df, non_pk_columns, "content_hash")
            
            # Join on primary key
            joined = original_hashed.alias("orig").join(
                shadow_hashed.alias("shadow"),
                on=pk_columns,
                how="full_outer"
            )
            
            # Categorize rows
            conditions = [
                (col("orig.content_hash").isNull() & col("shadow.content_hash").isNotNull(), "only_in_shadow"),
                (col("orig.content_hash").isNotNull() & col("shadow.content_hash").isNull(), "only_in_original"),
                (col("orig.content_hash") == col("shadow.content_hash"), "matching"),
                (col("orig.content_hash") != col("shadow.content_hash"), "different")
            ]
            
            # Initialize counters
            counters = {"matching": 0, "different": 0, "only_in_original": 0, "only_in_shadow": 0}
            
            for condition, label in conditions:
                count = joined.filter(condition).count()
                counters[label] = count
        else:
            # Only PK columns - check existence
            counters = {
                "matching": original_df.intersect(shadow_df).count(),
                "different": 0,
                "only_in_original": original_df.subtract(shadow_df).count(),
                "only_in_shadow": shadow_df.subtract(original_df).count()
            }
        
        differences = {
            "comparison_method": "primary_key",
            "primary_key_columns": pk_columns,
            "matching_rows": counters["matching"],
            "different_rows": counters["different"],
            "only_in_original": counters["only_in_original"],
            "only_in_shadow": counters["only_in_shadow"],
            "total_original_rows": original_df.count(),
            "total_shadow_rows": shadow_df.count()
        }
        
        self.report["content_differences"] = differences
        return differences
    
    def generate_summary(self) -> dict:
        """Generate overall comparison summary."""
        schema_diff = self.report.get("schema_differences", {})
        row_diff = self.report.get("row_differences", {})
        content_diff = self.report.get("content_differences", {})
        
        summary = {
            "comparison_passed": True,
            "issues_found": []
        }
        
        # Check schema
        if schema_diff.get("has_schema_differences"):
            summary["comparison_passed"] = False
            summary["issues_found"].append("Schema differences detected")
        
        # Check row counts
        if not row_diff.get("match", True):
            summary["comparison_passed"] = False
            summary["issues_found"].append(f"Row count mismatch: {row_diff.get('difference')} rows difference")
        
        # Check content
        if content_diff.get("different_rows", 0) > 0:
            summary["comparison_passed"] = False
            summary["issues_found"].append(f"Content differences: {content_diff.get('different_rows')} rows differ")
        
        if content_diff.get("only_in_original", 0) > 0:
            summary["comparison_passed"] = False
            summary["issues_found"].append(f"{content_diff.get('only_in_original')} rows only in original table")
        
        if content_diff.get("only_in_shadow", 0) > 0:
            summary["comparison_passed"] = False
            summary["issues_found"].append(f"{content_diff.get('only_in_shadow')} rows only in shadow table")
        
        self.report["summary"] = summary
        return summary
    
    def get_full_report(self) -> dict:
        """Get full comparison report."""
        return self.report
