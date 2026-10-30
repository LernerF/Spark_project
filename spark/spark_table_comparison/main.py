"""
Main entry point for table comparison application.
"""
import sys
import os

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from spark_table_comparison.spark_utils import create_spark_session, configure_s3_hadoop
from spark_table_comparison.dag import create_comparison_dag
from spark_table_comparison.config import (
    ORIGINAL_TABLE_PATH,
    SHADOW_TABLE_PATH,
    OUTPUT_PATH,
    PRIMARY_KEY_COLUMNS,
    IGNORE_COLUMNS,
    S3_BUCKET
)


def main():
    """Main entry point for the comparison application."""
    print("="*60)
    print("Table Comparison Tool for Migration Validation")
    print("="*60)
    print()
    
    # Check if data files exist
    if not os.path.exists(ORIGINAL_TABLE_PATH):
        print(f"ERROR: Original table not found at {ORIGINAL_TABLE_PATH}")
        print("Please place your CSV file at this location and try again.")
        sys.exit(1)
    
    if not os.path.exists(SHADOW_TABLE_PATH):
        print(f"ERROR: Shadow table not found at {SHADOW_TABLE_PATH}")
        print("Please place your CSV file at this location and try again.")
        sys.exit(1)
    
    print(f"Original table: {ORIGINAL_TABLE_PATH}")
    print(f"Shadow table: {SHADOW_TABLE_PATH}")
    print(f"Output path: {OUTPUT_PATH}")
    print()
    
    # Create Spark session
    print("Creating Spark session...")
    spark = create_spark_session()
    configure_s3_hadoop(spark)
    print("Spark session created successfully")
    print()
    
    try:
        # Create and execute DAG
        print("Creating comparison DAG...")
        dag = create_comparison_dag(
            spark=spark,
            original_path=ORIGINAL_TABLE_PATH,
            shadow_path=SHADOW_TABLE_PATH,
            output_path=OUTPUT_PATH,
            primary_key_columns=PRIMARY_KEY_COLUMNS,
            ignore_columns=IGNORE_COLUMNS
        )
        print(f"DAG created: {dag.name}")
        print()
        
        print("Executing comparison DAG...")
        print("-"*60)
        context = dag.execute()
        print("-"*60)
        print()
        
        # Print execution summary
        summary = dag.get_execution_summary()
        print("="*60)
        print("EXECUTION SUMMARY")
        print("="*60)
        print(f"DAG Status: {summary['status'].upper()}")
        print()
        
        print("Node Execution Details:")
        for node_name, node_info in summary['nodes'].items():
            status_emoji = "✓" if node_info['status'] == 'completed' else "✗"
            duration = f"{node_info['duration']:.2f}s" if node_info['duration'] else "N/A"
            print(f"  {status_emoji} {node_name}: {node_info['status']} ({duration})")
            if node_info['error']:
                print(f"      Error: {node_info['error']}")
        
        print()
        
        # Print comparison results
        if 'generate_summary' in context:
            report = context['generate_summary'].get('full_report', {})
            comparison_summary = report.get('summary', {})
            
            print("="*60)
            print("COMPARISON RESULTS")
            print("="*60)
            
            if comparison_summary.get('comparison_passed'):
                print("✓ COMPARISON PASSED - Tables are identical")
            else:
                print("✗ COMPARISON FAILED - Differences detected")
                print()
                print("Issues found:")
                for issue in comparison_summary.get('issues_found', []):
                    print(f"  - {issue}")
            
            print()
            
            # Print report locations
            if 'generate_reports' in context:
                print("="*60)
                print("REPORTS GENERATED")
                print("="*60)
                for report_name, path in context['generate_reports'].get('report_paths', {}).items():
                    print(f"  {report_name}: {path}")
        
        print()
        print("="*60)
        print("Comparison completed")
        print("="*60)
        
        # Return exit code
        if summary['status'] == 'failed':
            sys.exit(1)
        else:
            sys.exit(0)
    
    except Exception as e:
        print(f"ERROR: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
