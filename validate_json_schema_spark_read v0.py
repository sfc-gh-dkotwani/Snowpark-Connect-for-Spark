"""
JSON Schema Validation using Spark Read with PERMISSIVE mode
Leverages Spark's built-in schema enforcement to separate valid and invalid records
Uses Snowpark Connect for Snowflake integration
"""

import os
import json
from typing import Dict, Tuple, TYPE_CHECKING
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime
import sys

# Snowpark Connect imports
try:
    from snowflake import snowpark_connect
    SNOWPARK_CONNECT_AVAILABLE = True
except ImportError:
    print("⚠️  Warning: Snowpark Connect not available")
    print("   Install with: pip install snowflake-snowpark-python")
    SNOWPARK_CONNECT_AVAILABLE = False
    snowpark_connect = None

# For type hints
if TYPE_CHECKING:
    from pyspark.sql import DataFrame
else:
    # At runtime, we'll get DataFrame from the spark session
    DataFrame = None

def get_predefined_billing_schema():
    """
    Returns the predefined schema for GCP billing data
    This schema matches the structure shown in the terminal output
    """
    return StructType([
        StructField("billing_account_id", StringType(), True),
        StructField("cost", StringType(), True),
        StructField("currency", StringType(), True),
        StructField("currency_conversion_rate", DecimalType(38, 10), True),
        StructField("export_time", StringType(), True),
        StructField("invoice", StructType([
            StructField("month", LongType(), True),
            StructField("publisher_type", StringType(), True)
        ]), True),
        StructField("labels", ArrayType(StructType([
            StructField("key", StringType(), True),
            StructField("value", StringType(), True)
        ])), True),
        StructField("location", StringType(), True),
        StructField("project", StringType(), True),
        StructField("service", StructType([
            StructField("description", StringType(), True),
            StructField("id", StringType(), True)
        ]), True),
        StructField("sku", StructType([
            StructField("description", StringType(), True),
            StructField("id", StringType(), True)
        ]), True),
        StructField("usage", StructType([
            StructField("amount", StringType(), True),
            StructField("amount_in_pricing_units", DoubleType(), True),
            StructField("pricing_unit", StringType(), True),
            StructField("unit", StringType(), True)
        ]), True),
        StructField("usage_end_time", StringType(), True),
        StructField("usage_start_time", StringType(), True),
        # Add corrupt record column to capture validation failures
        StructField("_corrupt_record", StringType(), True)
    ])


class SparkSchemaValidator:
    """
    Validates JSON data using Spark's schema enforcement during read operation
    Uses PERMISSIVE mode to separate valid and invalid records
    Uses Snowpark Connect for Snowflake integration
    """
    
    def __init__(self, spark_session=None):
        """
        Initialize validator with Snowpark Connect session
        
        Args:
            spark_session: Optional pre-existing spark session. If None, gets current session.
        """
        if spark_session is None:
            # Get the current Snowpark Connect session
            if not SNOWPARK_CONNECT_AVAILABLE:
                raise ImportError(
                    "Snowpark Connect is not available. "
                    "Please install with: pip install snowflake-snowpark-python"
                )
            self.spark = snowpark_connect.get_session()
        else:
            self.spark = snowpark_connect.get_session()
            
        self.predefined_schema = get_predefined_billing_schema()
        self.validation_results = {
            "total_records": 0,
            "valid_records": 0,
            "invalid_records": 0,
            "validation_timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            "file_path": None,
            "null_value_stats": {},
            "business_rule_violations": []
        }
    
    def read_and_validate(self, json_file_path: str, csv_file_path: str) -> Tuple:
        """
        Read JSON file with schema validation and return valid/invalid dataframes
        
        Uses Spark's PERMISSIVE mode which:
        - Sets malformed records to null for all fields
        - Captures the original malformed record in _corrupt_record column
        
        Args:
            json_file_path: Path to JSON file
            
        Returns:
            Tuple of (valid_df, invalid_df, validation_results)
        """
        print("\n" + "="*60)
        print("📋 SPARK SCHEMA VALIDATION (PERMISSIVE MODE)")
        print("="*60)
        print(f"File: {json_file_path}")
        print(f"Started: {self.validation_results['validation_timestamp']}")
        
        if not os.path.exists(json_file_path):
            print(f"❌ File not found: {json_file_path}")
            raise FileNotFoundError(f"JSON file not found: {json_file_path}")
        
        self.validation_results["file_path"] = json_file_path
        
        try:
            # Read JSON with predefined schema using PERMISSIVE mode
            print("\n🔍 Step 1: Reading JSON with predefined schema (PERMISSIVE mode)...")
            print(f"\n🔍 Step 1: Reading CSV with predefined schema...")
            print("   - Valid records: All fields populated as per schema")
            print("   - Invalid records: Captured in _corrupt_record column")
            
            df_all = self.spark.read \
                .schema(self.predefined_schema) \
                .option("mode", "PERMISSIVE") \
                .option("columnNameOfCorruptRecord", "_corrupt_record") \
                .option("multiline", "true") \
                .json(json_file_path)

            total_records = df_all.count()
            self.validation_results["total_records"] = total_records
            print(f"✅ Total records read: {total_records}")
            
            # Step 2: Separate valid and invalid records
            print("\n🔍 Step 2: Separating valid and invalid records...")
            
            # Invalid records have non-null _corrupt_record
            df_invalid = df_all.filter(col("_corrupt_record").isNotNull())
            invalid_count = df_invalid.count()
            
            # Valid records have null _corrupt_record
            df_valid = df_all.filter(col("_corrupt_record").isNull())
            valid_count = df_valid.count()
            
            self.validation_results["valid_records"] = valid_count
            self.validation_results["invalid_records"] = invalid_count
            
            print(f"✅ Valid records: {valid_count} ({valid_count/total_records*100:.2f}%)")
            print(f"⚠️  Invalid records: {invalid_count} ({invalid_count/total_records*100:.2f}%)")
            
            # Step 3: Drop _corrupt_record from valid dataframe
            print("\n🔍 Step 3: Cleaning valid dataframe...")
            df_valid_clean = df_valid.drop("_corrupt_record")
            print(f"✅ Valid dataframe ready with {len(df_valid_clean.columns)} columns")
            
            # Step 4: Analyze invalid records
            if invalid_count > 0:
                print("\n🔍 Step 4: Analyzing invalid records...")
                self._analyze_invalid_records(df_invalid)
            
            # Step 5: Analyze valid records for data quality
            if valid_count > 0:
                print("\n🔍 Step 5: Analyzing data quality on valid records...")
               # self._analyze_data_quality(df_valid_clean)
            
            # Step 6: Generate report
            print("\n🔍 Step 6: Generating validation report...")
            self._generate_validation_report()
            
            return df_valid_clean, df_invalid, self.validation_results
            
        except Exception as e:
            print(f"❌ Error during validation: {e}")
            import traceback
            traceback.print_exc()
            raise
    
    def _analyze_invalid_records(self, df_invalid):
        """Analyze invalid records to understand what went wrong"""
        print("\n📊 Invalid Records Analysis:")
        
        invalid_count = df_invalid.count()
        print(f"   Total invalid records: {invalid_count}")
        
        # Show sample of corrupt records
        print("\n   Sample corrupt records (first 5):")
        corrupt_samples = df_invalid.select("_corrupt_record").limit(5).collect()
        for idx, row in enumerate(corrupt_samples, 1):
            corrupt_record = row["_corrupt_record"]
            # Truncate if too long
            if len(corrupt_record) > 200:
                corrupt_record = corrupt_record[:200] + "..."
            print(f"   {idx}. {corrupt_record}")
        
        # Try to identify common issues
        print("\n   Analyzing corruption patterns...")
        
        # Check if it's a parsing issue (missing braces, quotes, etc)
        parsing_issues = df_invalid.filter(
            col("_corrupt_record").rlike(".*[{}\\[\\]].*")
        ).count()
        if parsing_issues > 0:
            print(f"   - {parsing_issues} records with potential JSON structure issues")
        
        # Check for encoding issues
        encoding_issues = df_invalid.filter(
            col("_corrupt_record").rlike(".*[\\\\u].*")
        ).count()
        if encoding_issues > 0:
            print(f"   - {encoding_issues} records with potential encoding issues")
    
    def _analyze_data_quality(self, df_valid):
        """Analyze data quality on valid records"""
        print("\n📊 Data Quality Analysis (Valid Records):")
        
        total_valid = df_valid.count()
        
        # Null value analysis for key fields
        print("\n   Null value statistics:")
        key_fields = ["billing_account_id", "cost", "currency", "project", "service", "sku"]
        
        null_stats = {}
        for field in key_fields:
            null_count = df_valid.filter(col(field).isNull()).count()
            null_percentage = (null_count / total_valid * 100) if total_valid > 0 else 0
            null_stats[field] = {
                "null_count": null_count,
                "null_percentage": round(null_percentage, 2)
            }
            
            if null_count > 0:
                print(f"   - {field:<25} {null_count:>8} nulls ({null_percentage:>6.2f}%)")
            else:
                print(f"   - {field:<25} {'✓':>8} (no nulls)")
        
        self.validation_results["null_value_stats"] = null_stats
        
        # Business rule validation
        print("\n   Business rule validation:")
        
        # Rule 1: billing_account_id should not be null (critical)
        null_billing = df_valid.filter(col("billing_account_id").isNull()).count()
        if null_billing > 0:
            msg = f"{null_billing} valid records with null billing_account_id"
            print(f"   ⚠️  {msg}")
            self.validation_results["business_rule_violations"].append(msg)
        else:
            print(f"   ✅ All records have billing_account_id")
        
        # Rule 2: cost should be numeric
        try:
            non_numeric_cost = df_valid.filter(
                col("cost").isNotNull() & 
                ~col("cost").rlike("^-?[0-9]+(\\.[0-9]+)?$")
            ).count()
            if non_numeric_cost > 0:
                msg = f"{non_numeric_cost} records with non-numeric cost"
                print(f"   ⚠️  {msg}")
                self.validation_results["business_rule_violations"].append(msg)
            else:
                print(f"   ✅ All cost values are numeric")
        except Exception as e:
            print(f"   ⚠️  Could not validate cost: {e}")
        
        # Rule 3: Check timestamp ordering
        try:
            invalid_timestamps = df_valid.filter(
                (col("usage_start_time").isNotNull() & col("usage_end_time").isNotNull()) &
                (col("usage_start_time") > col("usage_end_time"))
            ).count()
            if invalid_timestamps > 0:
                msg = f"{invalid_timestamps} records with end_time < start_time"
                print(f"   ⚠️  {msg}")
                self.validation_results["business_rule_violations"].append(msg)
            else:
                print(f"   ✅ All timestamps in valid order")
        except Exception as e:
            print(f"   ⚠️  Could not validate timestamps: {e}")
        
        # Rule 4: Check invoice month format
        try:
            invalid_invoice = df_valid.filter(
                col("invoice.month").isNotNull() &
                ~col("invoice.month").between(202201, 209912)
            ).count()
            if invalid_invoice > 0:
                msg = f"{invalid_invoice} records with invalid invoice month"
                print(f"   ⚠️  {msg}")
                self.validation_results["business_rule_violations"].append(msg)
            else:
                print(f"   ✅ All invoice months valid")
        except Exception as e:
            print(f"   ⚠️  Could not validate invoice month: {e}")
    
    def _generate_validation_report(self):
        """Generate and display validation report"""
        print("\n" + "="*60)
        print("📋 VALIDATION SUMMARY REPORT")
        print("="*60)
        
        results = self.validation_results
        
        # Overall status
        valid_pct = (results["valid_records"] / results["total_records"] * 100) if results["total_records"] > 0 else 0
        invalid_pct = (results["invalid_records"] / results["total_records"] * 100) if results["total_records"] > 0 else 0
        
        print(f"\n📊 Record Statistics:")
        print(f"   Total records:    {results['total_records']:>10}")
        print(f"   Valid records:    {results['valid_records']:>10} ({valid_pct:.2f}%)")
        print(f"   Invalid records:  {results['invalid_records']:>10} ({invalid_pct:.2f}%)")
        
        # Validation status
        print(f"\n📊 Validation Status:")
        if results["invalid_records"] == 0 and len(results["business_rule_violations"]) == 0:
            print("   ✅ VALIDATION PASSED - All records valid, no business rule violations")
        elif results["invalid_records"] == 0 and len(results["business_rule_violations"]) > 0:
            print("   ⚠️  VALIDATION PASSED WITH WARNINGS - All records schema-valid but with data quality issues")
        elif results["invalid_records"] > 0 and len(results["business_rule_violations"]) == 0:
            print("   ⚠️  VALIDATION PARTIAL - Some records failed schema validation")
        else:
            print("   ⚠️  VALIDATION ISSUES - Schema failures and data quality issues found")
        
        # Business rule violations
        if results["business_rule_violations"]:
            print(f"\n⚠️  Business Rule Violations ({len(results['business_rule_violations'])}):")
            for violation in results["business_rule_violations"]:
                print(f"   - {violation}")
        
        # Save report
        report_file = f"validation_report_spark_read_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(report_file, 'w') as f:
            json.dump(results, f, indent=2, default=str)
        print(f"\n📝 Detailed report saved to: {report_file}")
        
        print("\n" + "="*60)
    
    def load_valid_to_snowflake(self, df_valid, table_name: str):
        """
        Load valid dataframe to Snowflake table
        
        Args:
            df_valid: Valid records dataframe
            table_name: Snowflake table name
        """
        print("\n" + "="*60)
        print("💾 LOADING VALID RECORDS TO SNOWFLAKE")
        print("="*60)
        
        try:
            record_count = df_valid.count()
            
            if record_count == 0:
                print("⚠️  No valid records to load")
                return
            
            print(f"📊 Records to load: {record_count}")
            
            # Drop table if exists
            print(f"🔄 Dropping table if exists: {table_name}")
            self.spark.sql(f"DROP TABLE IF EXISTS {table_name}").collect()
            
            # Create and load table
            print(f"💾 Creating table: {table_name}")
            df_valid.write.mode("overwrite").saveAsTable(table_name)
            
            # Verify
            verify_count = self.spark.sql(f"SELECT COUNT(*) as cnt FROM {table_name}").collect()[0]['cnt']
            print(f"✅ Table created successfully with {verify_count} records")
            
            # Show sample
            print(f"\n📋 Sample data from {table_name}:")
            self.spark.sql(f"SELECT * FROM {table_name} LIMIT 3").show(truncate=False)
            
        except Exception as e:
            print(f"❌ Error loading to Snowflake: {e}")
            import traceback
            traceback.print_exc()
            raise
    
    def save_invalid_to_table(self, df_invalid, table_name: str):
        """
        Save invalid records to a separate table for investigation
        
        Args:
            df_invalid: Invalid records dataframe (with _corrupt_record)
            table_name: Table name for invalid records
        """
        print("\n" + "="*60)
        print("💾 SAVING INVALID RECORDS FOR INVESTIGATION")
        print("="*60)
        
        try:
            invalid_count = df_invalid.count()
            
            if invalid_count == 0:
                print("✅ No invalid records - nothing to save")
                return
            
            print(f"⚠️  Invalid records to save: {invalid_count}")
            
            # Create table for invalid records
            print(f"💾 Creating invalid records table: {table_name}")
            self.spark.sql(f"DROP TABLE IF EXISTS {table_name}").collect()
            
            df_invalid.write.mode("overwrite").saveAsTable(table_name)
            
            # Verify
            verify_count = self.spark.sql(f"SELECT COUNT(*) as cnt FROM {table_name}").collect()[0]['cnt']
            print(f"✅ Invalid records table created with {verify_count} records")
            
            print(f"\n📋 Sample invalid records from {table_name}:")
            self.spark.sql(f"SELECT _corrupt_record FROM {table_name} LIMIT 3").show(truncate=100)
            
        except Exception as e:
            print(f"❌ Error saving invalid records: {e}")
            import traceback
            traceback.print_exc()


def create_spark_session():
    """Create Spark session using Snowpark Connect"""
    
    if not SNOWPARK_CONNECT_AVAILABLE:
        raise ImportError(
            "Snowpark Connect is not available. "
            "Please install with: pip install snowflake-snowpark-python"
        )
    
    try:
        # Fix Python version mismatch
        python_path = sys.executable
        os.environ["PYSPARK_PYTHON"] = python_path
        os.environ["PYSPARK_DRIVER_PYTHON"] = python_path
        
        print("🔗 Starting Snowpark Connect session...")
        
        # Set the environment variable to enable Snowpark Connect
        os.environ["SPARK_CONNECT_MODE_ENABLED"] = "1"
        
        # Start the local session with connection parameters
        snowpark_connect.start_session(connection_parameters={"connection_name": "poaconnection"})
        
        # Get the remote SparkSession
        spark = snowpark_connect.get_session()
        
        print("✅ Snowpark Connect session created!")
        print(f"   Session type: {type(spark)}")
        
        return spark
        
    except Exception as e:
        print(f"❌ Error creating Snowpark Connect session: {e}")
        import traceback
        traceback.print_exc()
        raise


def main():
    """Main function demonstrating the Spark-based validation approach"""
    print("\n" + "="*60)
    print("🔍 JSON SCHEMA VALIDATION USING SPARK READ")
    print("="*60)
    
    # Configuration
    json_file = "01E091-D83A6B-1F55DD-gcp-billing-v1_cleaned.json"
    csv_file = "testme.csv"
    valid_table = "gcp_billing_valid"
    invalid_table = "gcp_billing_invalid"
    
    spark = None
    try:
        # Create Snowpark Connect session
        spark = create_spark_session()
        
        # Create validator with the session
        validator = SparkSchemaValidator(spark)
        
        # Read and validate - returns two dataframes
        df_valid, df_invalid, results = validator.read_and_validate(json_file, csv_file)
        
        print("\n" + "="*60)
        print("📊 VALIDATION COMPLETE")
        print("="*60)
        print(f"✅ Valid DataFrame: {df_valid.count()} records")
        print(f"⚠️  Invalid DataFrame: {df_invalid.count()} records")
        
        # Show schemas
        print("\n📋 Valid DataFrame Schema:")
        df_valid.printSchema()
        
        if df_invalid.count() > 0:
            print("\n📋 Invalid DataFrame Schema:")
            df_invalid.printSchema()
        
        # Option 1: Load valid records to Snowflake
        print("\n" + "="*60)
        print("📦 LOADING DATA TO TABLES")
        print("="*60)
        
        response = input(f"\n💾 Load {df_valid.count()} valid records to '{valid_table}'? (yes/no): ")
        if response.lower() == 'yes':
            validator.load_valid_to_snowflake(df_valid, valid_table)
        
        # Option 2: Save invalid records for investigation
        if df_invalid.count() > 0:
            response = input(f"\n💾 Save {df_invalid.count()} invalid records to '{invalid_table}' for investigation? (yes/no): ")
            if response.lower() == 'yes':
                validator.save_invalid_to_table(df_invalid, invalid_table)
        
        print("\n✅ Validation and processing complete!")
        
        return df_valid, df_invalid, results
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return None, None, None
    finally:
        if spark:
            # Don't stop - leave it running for further analysis
            print("\n💡 Spark session still active for further analysis")
            print("   Use df_valid and df_invalid for additional processing")


if __name__ == "__main__":
    df_valid, df_invalid, results = main()

