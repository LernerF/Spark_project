"""
Spark Table Comparison Tool

A universal tool for comparing two tables (original and shadow)
during migration validation.
"""

__version__ = "1.0.0"
__author__ = "Migration Team"

from .comparison_engine import TableComparator
from .dag import ComparisonDAG, create_comparison_dag
from .report_generator import ReportGenerator
from .spark_utils import create_spark_session, configure_s3_hadoop

__all__ = [
    'TableComparator',
    'ComparisonDAG',
    'create_comparison_dag',
    'ReportGenerator',
    'create_spark_session',
    'configure_s3_hadoop'
]
