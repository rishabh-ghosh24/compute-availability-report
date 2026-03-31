import sys
import os

# Ensure the project root is on sys.path so tests can import
# compute_availability_report regardless of working directory
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
