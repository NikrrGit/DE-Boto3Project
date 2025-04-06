import os
import sys
from pathlib import Path
import pandas as pd
import logging
from pyspark.sql import functions as F
from pyspark.sql.types import *

# Add the project root to Python path
project_root = Path(__file__).parent.parent.parent.parent.parent
sys.path.insert(0, str(project_root))

from resources.dev import config
from src.main.utility.s3_client_object import S3ClientProvider
from src.main.utility.encrypt_decrypt import *
from src.main.utility.logging_config import *
from src.main.utility.my_sql_session import get_mysql_connection
from src.main.utility.s3_operations import S3Operations
from src.main.utility.spark_session import SparkSessionManager
from src.main.utility.database_writer import DatabaseWriter
from src.main.utility.file_utils import clean_up_local_files

logger = logging.getLogger(__name__)

def process_file(file_path):
    """Process a single CSV file and return True if successful"""
    try:
        logger.info(f"Starting to process file: {file_path}")
        
        # Read the CSV file
        df = pd.read_csv(file_path)
        logger.info(f"File loaded successfully. Shape: {df.shape}")
        
        # Verify mandatory columns
        missing_cols = [col for col in config.mandatory_columns if col not in df.columns]
        if missing_cols:
            raise ValueError(f"Missing mandatory columns: {missing_cols}")
        
        # Enhanced data validation
        validation_errors = []
        
        # Check for null values in mandatory columns
        for col in config.mandatory_columns:
            null_count = df[col].isnull().sum()
            if null_count > 0:
                validation_errors.append(f"Column '{col}' has {null_count} null values")
        
        # Check data types
        if not pd.to_numeric(df['price'], errors='coerce').notnull().all():
            validation_errors.append("Invalid price values detected")
            
        if not pd.to_numeric(df['quantity'], errors='coerce').notnull().all():
            validation_errors.append("Invalid quantity values detected")
        
        if validation_errors:
            raise ValueError(f"Validation errors found:\n" + "\n".join(validation_errors))
            
        logger.info("All validations passed successfully")
        return True
        
    except Exception as e:
        logger.error(f"Error processing file: {str(e)}")
        return False

def retry_failed_files():
    """Retry processing files that previously failed"""
    connection = None
    cursor = None
    try:
        connection = get_mysql_connection()
        cursor = connection.cursor()
        
        # Get list of failed files
        failed_query = """
        SELECT file_name, file_location 
        FROM product_staging_table 
        WHERE status = 'I'
        """
        cursor.execute(failed_query)
        failed_files = cursor.fetchall()
        
        if not failed_files:
            logger.info("No failed files to retry")
            return
            
        logger.info(f"Found {len(failed_files)} failed files to retry")
        
        for file_name, file_location in failed_files:
            file_path = os.path.join(file_location, file_name)
            
            if not os.path.exists(file_path):
                logger.error(f"Failed file not found: {file_path}")
                continue
                
            logger.info(f"Retrying file: {file_name}")
            
            try:
                # Update status to 'P' for retry
                update_query = """
                UPDATE product_staging_table 
                SET status = 'P', updated_date = NOW()
                WHERE file_name = %s AND status = 'I'
                """
                cursor.execute(update_query, (file_name,))
                connection.commit()
                
                # Process the file
                if process_file(file_path):
                    # Update status to 'A'
                    update_query = """
                    UPDATE product_staging_table 
                    SET status = 'A', updated_date = NOW()
                    WHERE file_name = %s AND status = 'P'
                    """
                    cursor.execute(update_query, (file_name,))
                    connection.commit()
                    logger.info(f"Successfully processed {file_name} on retry")
                else:
                    # Update status back to 'I'
                    update_query = """
                    UPDATE product_staging_table 
                    SET status = 'I', updated_date = NOW()
                    WHERE file_name = %s AND status = 'P'
                    """
                    cursor.execute(update_query, (file_name,))
                    connection.commit()
                    logger.error(f"Failed to process {file_name} on retry")
                    
            except Exception as e:
                logger.error(f"Error processing file {file_name} on retry: {str(e)}")
                # Update status back to 'I'
                update_query = """
                UPDATE product_staging_table 
                SET status = 'I', updated_date = NOW()
                WHERE file_name = %s
                """
                cursor.execute(update_query, (file_name,))
                connection.commit()
                
    except Exception as e:
        logger.error(f"Error in retry process: {str(e)}")
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()

def generate_error_summary():
    """Generate a summary of processing errors"""
    connection = None
    cursor = None
    try:
        connection = get_mysql_connection()
        cursor = connection.cursor()
        
        # Get summary of file statuses
        summary_query = """
        SELECT status, COUNT(*) as count
        FROM product_staging_table
        GROUP BY status
        """
        cursor.execute(summary_query)
        status_summary = cursor.fetchall()
        
        logger.info("\n=== Processing Summary ===")
        for status, count in status_summary:
            logger.info(f"Status '{status}': {count} files")
            
        # Get details of failed files
        failed_query = """
        SELECT file_name, created_date, updated_date
        FROM product_staging_table
        WHERE status = 'I'
        """
        cursor.execute(failed_query)
        failed_files = cursor.fetchall()
        
        if failed_files:
            logger.info("\n=== Failed Files ===")
            for file_name, created_date, updated_date in failed_files:
                logger.info(f"File: {file_name}")
                logger.info(f"First Processed: {created_date}")
                logger.info(f"Last Updated: {updated_date}")
                logger.info("-" * 30)
                
    except Exception as e:
        logger.error(f"Error generating summary: {str(e)}")
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()

def process_s3_files():
    """Main pipeline to process S3 files"""
    s3_ops = None
    spark_manager = None
    connection = None

    try:
        logger.info("=== Starting Data Processing Pipeline ===")
        
        # Initialize S3 client with IAM role if configured
        s3_provider = S3ClientProvider(
            use_iam_role=config.use_iam_role,
            role_arn=config.role_arn,
            session_name=config.role_session_name
        )
        s3_client = s3_provider.get_client()
        
        # Initialize S3 operations using client
        s3_ops = S3Operations(s3_client)
        logger.info("S3 Connection Established")

        # List files from S3
        files = s3_ops.list_files('sales_mart')
        if not files:
            logger.warning("No files found in S3 directory")
            return
            
        # Filter only CSV files
        csv_files, non_csv_files = s3_ops.filter_csv_files(files)
        if not csv_files:
            logger.warning("No CSV files to process")
            return
            
        # Download files locally
        valid_files = s3_ops.download_files(csv_files, config.local_directory)
        if not valid_files:
            logger.warning("No files downloaded successfully")
            return
            
        logger.info(f"Downloaded {len(valid_files)} files")

        # Initialize Spark session
        spark_manager = SparkSessionManager()
        spark = spark_manager.create_session()
        logger.info("Spark Session Created")
        
        # Read and process the local files
        result_df = spark_manager.process_csv_files(valid_files)
        
        if result_df is None or result_df.count() == 0:
            logger.warning("No valid data found in the CSV files")
            return
            
        try:
            # Read dimension tables
            dim_tables = spark_manager.read_dimension_tables()
            logger.info(f"Loaded {len(dim_tables)} dimension tables")
            
            # Create data marts
            customer_mart, sales_mart = spark_manager.create_data_marts(
                result_df, 
                dim_tables
            )
            logger.info(f"Customer Mart Records: {customer_mart.count()}")
            logger.info(f"Sales Mart Records: {sales_mart.count()}")
            
            # Calculate analytics
            monthly_purchases = spark_manager.calculate_customer_monthly_purchases(customer_mart)
            sales_incentives = spark_manager.calculate_sales_incentives(sales_mart)
            
            logger.info(f"Monthly Purchase Records: {monthly_purchases.count()}")
            logger.info(f"Sales Incentives Records: {sales_incentives.count()}")
            
            # Create directories if they don't exist
            os.makedirs(config.customer_monthly_purchases_path, exist_ok=True)
            os.makedirs(config.sales_incentives_path, exist_ok=True)
            
            # Write to local Parquet
            monthly_purchases.write.mode('overwrite') \
                .partitionBy('purchase_month') \
                .parquet(config.customer_monthly_purchases_path)
                
            sales_incentives.write.mode('overwrite') \
                .partitionBy('sales_month') \
                .parquet(config.sales_incentives_path)
            
            logger.info("Written data marts to Parquet files")
            
            # Write to MySQL
            connection = get_mysql_connection()
            db_writer = DatabaseWriter(connection)
            
            db_writer.write_to_mysql(monthly_purchases, 'customer_monthly_purchases')
            db_writer.write_to_mysql(sales_incentives, 'sales_team_incentives')
            logger.info("Written data marts to MySQL")
            
            # Clean up local files
            clean_up_local_files([
                config.customer_monthly_purchases_path,
                config.sales_incentives_path
            ])
            logger.info("Cleaned up local files")
            
            logger.info("=== Pipeline Completed Successfully ===")
            
        except Exception as e:
            logger.error(f"Error in data mart creation: {str(e)}")
            raise
            
    except Exception as e:
        logger.error(f"Pipeline error: {str(e)}")
        raise
        
    finally:
        if spark_manager and spark_manager.spark:
            # Stop Spark UI
            spark_manager.spark.stop()
            # Kill any remaining Spark processes
            os.system("pkill -f 'spark'")
            logger.info("Spark session terminated")
        
        if connection:
            connection.close()
            logger.info("Database connection closed")

if __name__ == "__main__":
    try:
        process_s3_files()
    except KeyboardInterrupt:
        logger.info("Interrupted by user. Cleaning up...")
        os.system("pkill -f 'spark'")
        logger.info("Spark processes terminated")
    finally:
        # Ensure no Spark processes are left running
        os.system("pkill -f 'spark'") 