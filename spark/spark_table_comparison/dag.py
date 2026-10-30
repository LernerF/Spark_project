"""
DAG (Directed Acyclic Graph) for table comparison workflow.
Represents the comparison process as a series of steps.
"""
from datetime import datetime
from pyspark.sql import SparkSession
from typing import Callable, Dict, Any, List
import time


class DAGNode:
    """Represents a single node in the DAG."""
    
    def __init__(self, name: str, task: Callable, dependencies: List[str] = None):
        """
        Initialize DAG node.
        
        Args:
            name: Unique name for the node
            task: Callable function to execute
            dependencies: List of node names that must complete before this node
        """
        self.name = name
        self.task = task
        self.dependencies = dependencies or []
        self.status = "pending"
        self.start_time = None
        self.end_time = None
        self.result = None
        self.error = None


class ComparisonDAG:
    """DAG for orchestrating table comparison workflow."""
    
    def __init__(self, name: str = "TableComparisonDAG"):
        """
        Initialize comparison DAG.
        
        Args:
            name: Name of the DAG
        """
        self.name = name
        self.nodes: Dict[str, DAGNode] = {}
        self.execution_log = []
    
    def add_node(self, name: str, task: Callable, dependencies: List[str] = None):
        """
        Add a node to the DAG.
        
        Args:
            name: Unique name for the node
            task: Callable function to execute
            dependencies: List of node names that must complete before this node
        """
        node = DAGNode(name, task, dependencies)
        self.nodes[name] = node
    
    def _get_execution_order(self) -> List[str]:
        """
        Get topological order for node execution.
        
        Returns:
            List of node names in execution order
        """
        visited = set()
        order = []
        
        def visit(node_name: str):
            if node_name in visited:
                return
            visited.add(node_name)
            
            node = self.nodes.get(node_name)
            if node:
                for dep in node.dependencies:
                    visit(dep)
                order.append(node_name)
        
        for node_name in self.nodes:
            visit(node_name)
        
        return order
    
    def _validate_dependencies(self):
        """Validate that all dependencies exist."""
        for node in self.nodes.values():
            for dep in node.dependencies:
                if dep not in self.nodes:
                    raise ValueError(f"Dependency '{dep}' not found for node '{node.name}'")
    
    def execute(self, context: Dict[str, Any] = None) -> Dict[str, Any]:
        """
        Execute the DAG.
        
        Args:
            context: Initial context dictionary passed to tasks
            
        Returns:
            Final context with results from all tasks
        """
        self._validate_dependencies()
        context = context or {}
        execution_order = self._get_execution_order()
        
        self.execution_log.append({
            "timestamp": datetime.now().isoformat(),
            "event": "DAG_STARTED",
            "dag_name": self.name
        })
        
        for node_name in execution_order:
            node = self.nodes[node_name]
            
            # Check if dependencies are satisfied
            deps_satisfied = all(
                self.nodes[dep].status == "completed"
                for dep in node.dependencies
            )
            
            if not deps_satisfied:
                node.status = "skipped"
                self.execution_log.append({
                    "timestamp": datetime.now().isoformat(),
                    "event": "NODE_SKIPPED",
                    "node": node_name,
                    "reason": "Dependencies not satisfied"
                })
                continue
            
            # Execute node
            try:
                node.status = "running"
                node.start_time = datetime.now()
                
                self.execution_log.append({
                    "timestamp": datetime.now().isoformat(),
                    "event": "NODE_STARTED",
                    "node": node_name
                })
                
                # Execute task with context
                result = node.task(context)
                
                node.result = result
                node.end_time = datetime.now()
                node.status = "completed"
                
                # Update context with result
                context[node_name] = result
                
                self.execution_log.append({
                    "timestamp": datetime.now().isoformat(),
                    "event": "NODE_COMPLETED",
                    "node": node_name,
                    "duration_seconds": (node.end_time - node.start_time).total_seconds()
                })
                
            except Exception as e:
                node.status = "failed"
                node.error = str(e)
                node.end_time = datetime.now()
                
                self.execution_log.append({
                    "timestamp": datetime.now().isoformat(),
                    "event": "NODE_FAILED",
                    "node": node_name,
                    "error": str(e)
                })
                
                # Store error in context but continue with other nodes
                context[node_name] = {"error": str(e)}
        
        self.execution_log.append({
            "timestamp": datetime.now().isoformat(),
            "event": "DAG_COMPLETED",
            "dag_name": self.name,
            "status": self._get_dag_status()
        })
        
        context["execution_log"] = self.execution_log
        return context
    
    def _get_dag_status(self) -> str:
        """Get overall DAG status."""
        statuses = [node.status for node in self.nodes.values()]
        
        if "failed" in statuses:
            return "failed"
        elif "running" in statuses:
            return "running"
        elif all(s == "completed" for s in statuses):
            return "completed"
        else:
            return "partial"
    
    def get_execution_summary(self) -> Dict[str, Any]:
        """Get summary of DAG execution."""
        return {
            "dag_name": self.name,
            "status": self._get_dag_status(),
            "nodes": {
                name: {
                    "status": node.status,
                    "duration": (node.end_time - node.start_time).total_seconds() 
                               if node.end_time and node.start_time else None,
                    "error": node.error
                }
                for name, node in self.nodes.items()
            }
        }


def create_comparison_dag(spark: SparkSession, original_path: str, shadow_path: str,
                          output_path: str, primary_key_columns: list = None,
                          ignore_columns: list = None) -> ComparisonDAG:
    """
    Create a DAG for table comparison.
    
    Args:
        spark: SparkSession
        original_path: Path to original table
        shadow_path: Path to shadow table
        output_path: S3 path for output reports
        primary_key_columns: List of primary key columns
        ignore_columns: List of columns to ignore
        
    Returns:
        Configured ComparisonDAG instance
    """
    from comparison_engine import TableComparator
    from report_generator import ReportGenerator
    
    dag = ComparisonDAG("TableComparisonMigrationDAG")
    
    # Task 1: Load original table
    def load_original_table(context):
        print("Loading original table...")
        df = spark.read.option("header", "true").option("inferSchema", "true").csv(original_path)
        print(f"Original table loaded: {df.count()} rows, {len(df.columns)} columns")
        return {"dataframe": df, "path": original_path}
    
    # Task 2: Load shadow table
    def load_shadow_table(context):
        print("Loading shadow table...")
        df = spark.read.option("header", "true").option("inferSchema", "true").csv(shadow_path)
        print(f"Shadow table loaded: {df.count()} rows, {len(df.columns)} columns")
        return {"dataframe": df, "path": shadow_path}
    
    # Task 3: Compare schemas
    def compare_schemas(context):
        print("Comparing schemas...")
        comparator = TableComparator(
            spark,
            primary_key_columns=primary_key_columns,
            ignore_columns=ignore_columns
        )
        
        original_df = context["load_original_table"]["dataframe"]
        shadow_df = context["load_shadow_table"]["dataframe"]
        
        schema_diff = comparator.compare_schemas(original_df, shadow_df)
        print(f"Schema comparison complete: {schema_diff['original_table_columns']} vs {schema_diff['shadow_table_columns']} columns")
        
        return {
            "schema_differences": schema_diff,
            "comparator": comparator
        }
    
    # Task 4: Compare row counts
    def compare_row_counts(context):
        print("Comparing row counts...")
        comparator = context["compare_schemas"]["comparator"]
        
        original_df = context["load_original_table"]["dataframe"]
        shadow_df = context["load_shadow_table"]["dataframe"]
        
        row_diff = comparator.compare_row_counts(original_df, shadow_df)
        print(f"Row count comparison complete: {row_diff['original_table_rows']} vs {row_diff['shadow_table_rows']} rows")
        
        return {"row_differences": row_diff}
    
    # Task 5: Compare content
    def compare_content(context):
        print("Comparing content...")
        comparator = context["compare_schemas"]["comparator"]
        
        original_df = context["load_original_table"]["dataframe"]
        shadow_df = context["load_shadow_table"]["dataframe"]
        
        content_diff = comparator.compare_content(original_df, shadow_df)
        print(f"Content comparison complete: {content_diff['matching_rows']} matching rows")
        
        return {"content_differences": content_diff}
    
    # Task 6: Generate summary
    def generate_summary(context):
        print("Generating summary...")
        comparator = context["compare_schemas"]["comparator"]
        summary = comparator.generate_summary()
        
        print(f"Comparison {'PASSED' if summary['comparison_passed'] else 'FAILED'}")
        if summary['issues_found']:
            print("Issues found:")
            for issue in summary['issues_found']:
                print(f"  - {issue}")
        
        return {"summary": summary, "full_report": comparator.get_full_report()}
    
    # Task 7: Generate reports
    def generate_reports(context):
        print("Generating reports...")
        report = context["generate_summary"]["full_report"]
        
        report_gen = ReportGenerator(spark, output_path)
        paths = report_gen.generate_and_save_all_reports(report)
        
        print("Reports saved:")
        for name, path in paths.items():
            print(f"  {name}: {path}")
        
        return {"report_paths": paths}
    
    # Add nodes to DAG
    dag.add_node("load_original_table", load_original_table)
    dag.add_node("load_shadow_table", load_shadow_table)
    dag.add_node("compare_schemas", compare_schemas, 
                 ["load_original_table", "load_shadow_table"])
    dag.add_node("compare_row_counts", compare_row_counts, ["compare_schemas"])
    dag.add_node("compare_content", compare_content, ["compare_schemas"])
    dag.add_node("generate_summary", generate_summary, 
                 ["compare_row_counts", "compare_content"])
    dag.add_node("generate_reports", generate_reports, ["generate_summary"])
    
    return dag
