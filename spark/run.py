#!/usr/bin/env python3
"""
Simple script to run the table comparison tool.
"""
import sys
import os

# Add the project root to Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Import and run main
from spark_table_comparison.main import main

if __name__ == "__main__":
    main()
