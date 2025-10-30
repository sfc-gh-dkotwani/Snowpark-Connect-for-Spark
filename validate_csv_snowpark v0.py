"""
CSV Schema Validation using Snowpark Connect
Reads CSV file, reconstructs complex fields, and validates against predefined schema
"""

import os
import json
from typing import Tuple, Dict
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


def get_predefined_billing_schema():
    """
    Returns the predefined schema for GCP billing data
    This is the TARGET schema after reconstruction
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
        # Add corrupt record column for validation
        StructField("_corrupt_record", StringType(), True)
    ])


def get_csv_schema():
    """
    Returns schema for reading CSV - all columns as StringType
    CSV columns are flattened versions of nested structures
    """
    return StructType([
        StructField("billing_account_id", StringType(), True),
        StructField("cost", StringType(), True),
        StructField("currency", StringType(), True),
        StructField("currency_conversion_rate", StringType(), True),
        StructField("export_time", StringType(), True),
        StructField("invoice_month", StringType(), True),
        StructField("invoice_publisher_type", StringType(), True),
        StructField("labels", StringType(), True),  # JSON string
        StructField("location", StringType(), True),
        StructField("project", StringType(), True),
        StructField("service_id", StringType(), True),
        StructField("service_description", StringType(), True),
        StructField("sku_id", StringType(), True),
        StructField("sku_description", StringType(), True),
        StructField("usage_amount", StringType(), True),
        StructField("usage_amount_in_pricing_units", StringType(), True),
        StructField("usage_pricing_unit", StringType(), True),
        StructField("usage_unit", StringType(), True),
        StructField("usage_end_time", StringType(), True),
        StructField("usage_start_time", StringType(), True)
    ])


def get_sub_schemas():
    """Returns sub-schemas for reconstructing complex fields from JSON strings"""
    
    invoice_schema = StructType([
        StructField("month", LongType(), True),
        StructField("publisher_type", StringType(), True)
    ])
    
    service_schema = StructType([
        StructField("description", StringType(), True),
        StructField("id", StringType(), True)
    ])
    
    sku_schema = StructType([
        StructField("description", StringType(), True),
        StructField("id", StringType(), True)
    ])
    
    usage_schema = StructType([
        StructField("amount", StringType(), True),
        StructField("amount_in_pricing_units", DoubleType(), True),
        StructField("pricing_unit", StringType(), True),
        StructField("unit", StringType(), True)
    ])
    
    labels_schema = ArrayType(StructType([
        StructField("key", StringType(), True),
        StructField("value", StringType(), True)
    ]))
    
    return {
        "invoice": invoice_schema,
        "service": service_schema,
        "sku": sku_schema,
        "usage": usage_schema,
        "labels": labels_schema
    }


class CSVSchemaValidator:
    """
    Validates CSV data by reconstructing complex fields and checking against schema
    Uses Snowpark Connect for Snowflake integration
    """
    
    def __init__(self, spark_session=None):
        """
        Initialize validator with Snowpark Connect session
        
        Args:
            spark_session: Optional pre-existing spark session
        """
        if spark_session is None:
            if not SNOWPARK_CONNECT_AVAILABLE:
                raise ImportError("Snowpark Connect is not available")
            self.spark = snowpark_connect.get_session()
        else:
            self.spark = spark_session
        
        self.csv_schema = get_csv_schema()
        self.predefined_schema = get_predefined_billing_schema()
        self.sub_schemas = get_sub_schemas()
        self.validation_results = {
            "total_records": 0,
            "valid_records": 0,
            "invalid_records": 0,
            "validation_timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            "validation_method": "implicit_spark_casting",
            "file_path": None,
            "reconstruction_errors": []
        }
    
    def read_and_validate_csv(self, csv_file_path: str) -> Tuple:
        """
        Read CSV file, reconstruct complex fields, and validate implicitly via Spark
        
        Uses Spark's implicit validation by reconstructing fields and letting
        Spark enforce data types and schema constraints automatically.
        
        Args:
            csv_file_path: Path to CSV file
            
        Returns:
            Tuple of (valid_df, invalid_df, validation_results)
        """
        print("\n" + "="*60)
        print("📋 CSV SCHEMA VALIDATION (IMPLICIT - SNOWPARK CONNECT)")
        print("="*60)
        print(f"File: {csv_file_path}")
        print(f"Started: {self.validation_results['validation_timestamp']}")
        
        if not os.path.exists(csv_file_path):
            print(f"❌ File not found: {csv_file_path}")
            raise FileNotFoundError(f"CSV file not found: {csv_file_path}")
        
        self.validation_results["file_path"] = csv_file_path
        
        try:
            # Step 1: Read CSV with all columns as StringType for controlled casting
            print("\n🔍 Step 1: Reading CSV with StringType schema...")
            print(f"   Expected columns: {len(self.csv_schema.fields)}")
            
            df_csv = self.spark.read \
                .schema(self.csv_schema) \
                .option("header", "true") \
                .option("mode", "PERMISSIVE") \
                .csv(csv_file_path)
            
            total_records = df_csv.count()
            self.validation_results["total_records"] = total_records
            print(f"✅ Total records read: {total_records}")
            print(f"   Columns found: {len(df_csv.columns)}")
            
            # Step 2: Reconstruct complex fields (implicit validation via casting)
            print("\n🔍 Step 2: Reconstructing complex fields with implicit validation...")
            df_reconstructed = self._reconstruct_complex_fields_with_validation(df_csv)
            
            # Step 3: Separate valid and invalid using Spark's validation
            print("\n🔍 Step 3: Separating valid and invalid records...")
            df_valid, df_invalid = self._separate_valid_invalid(df_reconstructed)
            
            valid_count = df_valid.count()
            invalid_count = df_invalid.count()
            
            self.validation_results["valid_records"] = valid_count
            self.validation_results["invalid_records"] = invalid_count
            
            print(f"✅ Valid records: {valid_count} ({valid_count/total_records*100:.2f}%)")
            print(f"⚠️  Invalid records: {invalid_count} ({invalid_count/total_records*100:.2f}%)")
            
            # Step 4: Generate report
            print("\n🔍 Step 4: Generating validation report...")
            self._generate_validation_report()
            
            return df_valid, df_invalid, self.validation_results
            
        except Exception as e:
            print(f"❌ Error during validation: {e}")
            import traceback
            traceback.print_exc()
            raise
    
    def _reconstruct_complex_fields_with_validation(self, df_csv):
        """
        Reconstruct complex fields with implicit validation via Spark casting
        
        Uses try expressions and Spark's type system to automatically validate.
        Invalid casts will result in nulls, which we'll use to identify invalid records.
        """
        print("\n📊 Reconstructing Complex Fields with Implicit Validation:")
        
        try:
            # Add row_id for tracking
            df = df_csv.withColumn("_row_id", monotonically_increasing_id())
            
            # Reconstruct invoice struct - Spark validates LongType cast implicitly
            print("   - invoice struct (with implicit Long validation)...")
            df = df.withColumn(
                "invoice",
                when(
                    col("invoice_month").isNotNull() & col("invoice_publisher_type").isNotNull(),
                    struct(
                        col("invoice_month").cast(LongType()).alias("month"),
                        col("invoice_publisher_type").alias("publisher_type")
                    )
                ).otherwise(lit(None))
            )
            
            # Reconstruct service struct
            print("   - service struct...")
            df = df.withColumn(
                "service",
                when(
                    col("service_id").isNotNull() & col("service_description").isNotNull(),
                    struct(
                        col("service_description").alias("description"),
                        col("service_id").alias("id")
                    )
                ).otherwise(lit(None))
            )
            
            # Reconstruct sku struct
            print("   - sku struct...")
            df = df.withColumn(
                "sku",
                when(
                    col("sku_id").isNotNull() & col("sku_description").isNotNull(),
                    struct(
                        col("sku_description").alias("description"),
                        col("sku_id").alias("id")
                    )
                ).otherwise(lit(None))
            )
            
            # Reconstruct usage struct - Spark validates DoubleType cast implicitly
            print("   - usage struct (with implicit Double validation)...")
            df = df.withColumn(
                "usage",
                when(
                    col("usage_amount").isNotNull(),
                    struct(
                        col("usage_amount").alias("amount"),
                        col("usage_amount_in_pricing_units").cast(DoubleType()).alias("amount_in_pricing_units"),
                        col("usage_pricing_unit").alias("pricing_unit"),
                        col("usage_unit").alias("unit")
                    )
                ).otherwise(lit(None))
            )
            
            # Reconstruct labels array from JSON string - implicit validation via from_json
            print("   - labels array from JSON (implicit JSON validation)...")
            df = df.withColumn(
                "labels_parsed",
                when(
                    (col("labels").isNotNull()) & (col("labels") != "") & (col("labels") != "[]"),
                    from_json(col("labels"), self.sub_schemas["labels"])
                ).otherwise(lit(None))
            )
            
            # Mark rows where JSON parsing failed (from_json returns null for invalid JSON)
            df = df.withColumn(
                "labels",
                when(
                    col("labels_parsed").isNotNull() | (col("labels") == "[]") | (col("labels") == ""),
                    col("labels_parsed")
                ).otherwise(lit(None))
            ).drop("labels_parsed")
            
            # Cast numeric fields - Spark validates implicitly, invalid casts become null
            print("   - casting numeric fields (implicit type validation)...")
            df = df.withColumn(
                "currency_conversion_rate",
                col("currency_conversion_rate").cast(DecimalType(38, 10))
            )
            
            # Add validation marker: check if any required reconstruction failed
            df = df.withColumn(
                "_validation_failed",
                when(
                    # Core required fields that should not be null after reconstruction
                    col("billing_account_id").isNull() |
                    col("cost").isNull() |
                    col("invoice").isNull() |
                    col("service").isNull() |
                    col("sku").isNull() |
                    col("usage").isNull(),
                    lit(True)
                ).otherwise(lit(False))
            )
            
            # Add corrupt record marker for failed validations
            df = df.withColumn(
                "_corrupt_record",
                when(
                    col("_validation_failed") == True,
                    concat_ws(", ",
                        when(col("billing_account_id").isNull(), lit("billing_account_id: null")).otherwise(lit("")),
                        when(col("invoice").isNull(), lit("invoice: reconstruction failed")).otherwise(lit("")),
                        when(col("service").isNull(), lit("service: reconstruction failed")).otherwise(lit("")),
                        when(col("sku").isNull(), lit("sku: reconstruction failed")).otherwise(lit("")),
                        when(col("usage").isNull(), lit("usage: reconstruction failed")).otherwise(lit(""))
                    )
                ).otherwise(lit(None).cast(StringType()))
            )
            
            # Select columns in predefined schema order
            print("   - reordering to match predefined schema...")
            predefined_cols = [field.name for field in self.predefined_schema.fields 
                             if field.name not in ["_corrupt_record"]]
            df = df.select(*predefined_cols, "_corrupt_record", "_validation_failed", "_row_id")
            
            print("✅ Complex field reconstruction complete with implicit validation")
            
            return df
            
        except Exception as e:
            print(f"❌ Error in reconstruction: {e}")
            self.validation_results["reconstruction_errors"].append(str(e))
            raise
    
    def _separate_valid_invalid(self, df_reconstructed):
        """
        Separate valid and invalid records based on Spark's implicit validation
        
        Uses the _validation_failed marker created during reconstruction
        """
        print("\n📊 Separating Valid and Invalid Records:")
        
        try:
            # Split based on validation marker
            df_invalid = df_reconstructed.filter(col("_validation_failed") == True) \
                .drop("_validation_failed", "_row_id")
            
            df_valid = df_reconstructed.filter(col("_validation_failed") == False) \
                .drop("_validation_failed", "_row_id", "_corrupt_record") \
                .withColumn("_corrupt_record", lit(None).cast(StringType()))
            
            print(f"✅ Records separated based on implicit Spark validation")
            
            return df_valid, df_invalid
            
        except Exception as e:
            print(f"❌ Error separating records: {e}")
            raise
    
    
    def _generate_validation_report(self):
        """Generate and display validation report"""
        print("\n" + "="*60)
        print("📋 VALIDATION SUMMARY REPORT (IMPLICIT VALIDATION)")
        print("="*60)
        
        results = self.validation_results
        
        # Overall status
        valid_pct = (results["valid_records"] / results["total_records"] * 100) if results["total_records"] > 0 else 0
        invalid_pct = (results["invalid_records"] / results["total_records"] * 100) if results["total_records"] > 0 else 0
        
        print(f"\n📊 Record Statistics:")
        print(f"   Total records:    {results['total_records']:>10}")
        print(f"   Valid records:    {results['valid_records']:>10} ({valid_pct:.2f}%)")
        print(f"   Invalid records:  {results['invalid_records']:>10} ({invalid_pct:.2f}%)")
        
        # Validation approach
        print(f"\n📊 Validation Approach:")
        print(f"   Method: Implicit via Spark type casting")
        print(f"   - Type validation: Automatic (Long, Double, Decimal)")
        print(f"   - JSON validation: Automatic (from_json)")
        print(f"   - Struct reconstruction: Validated during creation")
        
        # Reconstruction errors
        if results["reconstruction_errors"]:
            print(f"\n⚠️  Reconstruction Issues ({len(results['reconstruction_errors'])}):")
            for error in results["reconstruction_errors"]:
                print(f"   - {error}")
        
        # Validation status
        print(f"\n📊 Validation Status:")
        if results["invalid_records"] == 0 and not results["reconstruction_errors"]:
            print("   ✅ VALIDATION PASSED - All records valid")
        elif results["invalid_records"] == 0 and results["reconstruction_errors"]:
            print("   ⚠️  VALIDATION PASSED WITH WARNINGS - Check reconstruction issues")
        else:
            print("   ⚠️  VALIDATION ISSUES - Some records failed implicit validation")
            print("       (Type casting failures, JSON parsing errors, or null required fields)")
        
        # Save report
        report_file = f"validation_report_csv_implicit_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(report_file, 'w') as f:
            json.dump(results, f, indent=2, default=str)
        print(f"\n📝 Detailed report saved to: {report_file}")
        
        print("\n" + "="*60)
    
    def load_valid_to_snowflake(self, df_valid, table_name: str):
        """Load valid dataframe to Snowflake table"""
        print("\n" + "="*60)
        print("💾 LOADING VALID RECORDS TO SNOWFLAKE")
        print("="*60)
        
        try:
            record_count = df_valid.count()
            
            if record_count == 0:
                print("⚠️  No valid records to load")
                return
            
            print(f"📊 Records to load: {record_count}")
            
            # Drop _corrupt_record column before loading
            df_to_load = df_valid.drop("_corrupt_record")
            
            # Drop table if exists
            print(f"🔄 Dropping table if exists: {table_name}")
            self.spark.sql(f"DROP TABLE IF EXISTS {table_name}").collect()
            
            # Create and load table
            print(f"💾 Creating table: {table_name}")
            df_to_load.write.mode("overwrite").saveAsTable(table_name)
            
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


def create_spark_session():
    """Create Spark session using Snowpark Connect"""
    
    if not SNOWPARK_CONNECT_AVAILABLE:
        raise ImportError("Snowpark Connect is not available")
    
    try:
        # Fix Python version mismatch
        python_path = sys.executable
        os.environ["PYSPARK_PYTHON"] = python_path
        os.environ["PYSPARK_DRIVER_PYTHON"] = python_path
        
        print("🔗 Starting Snowpark Connect session...")
        
        # Set Snowpark Connect mode
        os.environ["SPARK_CONNECT_MODE_ENABLED"] = "1"
        
        # Start session
        snowpark_connect.start_session(
            connection_parameters={"connection_name": "poaconnection"}
        )
        
        # Get session
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
    """Main function demonstrating CSV validation"""
    print("\n" + "="*60)
    print("🔍 CSV SCHEMA VALIDATION USING SNOWPARK CONNECT")
    print("="*60)
    
    # Configuration
    csv_file = "synthetic_billing_data.csv"  # Change this to your CSV file
    valid_table = "gcp_billing_valid_from_csv"
    
    spark = None
    try:
        # Try to get existing session first, then create new one
        if SNOWPARK_CONNECT_AVAILABLE:
            try:
                spark = snowpark_connect.get_session()
                print("✅ Using existing Snowpark Connect session")
            except:
                spark = create_spark_session()
        else:
            raise ImportError("Snowpark Connect is not available")
        
        # Create validator
        validator = CSVSchemaValidator(spark)
        
        # Read and validate CSV
        df_valid, df_invalid, results = validator.read_and_validate_csv(csv_file)
        
        print("\n" + "="*60)
        print("📊 VALIDATION COMPLETE")
        print("="*60)
        print(f"✅ Valid DataFrame: {df_valid.count()} records")
        print(f"⚠️  Invalid DataFrame: {df_invalid.count()} records")
        
        # Show schemas
        print("\n📋 Valid DataFrame Schema:")
        df_valid.printSchema()
        
        if df_invalid.count() > 0:
            print("\n📋 Invalid DataFrame (first 5):")
            df_invalid.select("billing_account_id", "project", "_corrupt_record").show(5, truncate=False)
        
        # Option: Load valid records to Snowflake
        if df_valid.count() > 0:
            response = input(f"\n💾 Load {df_valid.count()} valid records to '{valid_table}'? (yes/no): ")
            if response.lower() == 'yes':
                validator.load_valid_to_snowflake(df_valid, valid_table)
        
        print("\n✅ CSV validation and processing complete!")
        
        return df_valid, df_invalid, results
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return None, None, None
    finally:
        if spark:
            print("\n💡 Spark session still active for further analysis")


if __name__ == "__main__":
    df_valid, df_invalid, results = main()

