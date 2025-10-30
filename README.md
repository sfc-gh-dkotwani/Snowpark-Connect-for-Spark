# Snowpark-Connect-for-Spark

Overview
These scripts validate the input files (JSON and CSV) against a predefined schema using PySpark with Snowpark Connect, which allows PySpark code to execute on Snowflake infrastructure instead of local Spark.

🎯 Purpose
Validates input files by:
- Reading JSON and CSV with schema enforcement
- Separating valid and invalid records
- Analyzing data quality
- Loading results to Snowflake tables
