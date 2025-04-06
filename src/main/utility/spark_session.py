from pyspark.sql.window import Window
import findspark
findspark.init()
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import *
from pyspark.sql import functions as F
from typing import List, Optional, Dict
import logging
import os
import sys
from resources.dev import config
from pathlib import Path
import shutil

logger = logging.getLogger(__name__)

class SparkSessionManager:
    def __init__(self):
        self.spark = None
        self.sales_schema = StructType([
            StructField("customer_id", IntegerType(), True),
            StructField("store_id", IntegerType(), True),
            StructField("product_name", StringType(), True),
            StructField("sales_date", StringType(), True),
            StructField("sales_person_id", IntegerType(), True),
            StructField("price", DoubleType(), True),
            StructField("quantity", IntegerType(), True),
            StructField("total_cost", DoubleType(), True)
        ])

    def create_session(self):
        try:
            self.spark = SparkSession.builder \
                .appName("Boto3 Data Engineering Pipeline") \
                .config("spark.some.config.option", "some-value") \
                .getOrCreate()
            logger.info("Spark session created successfully")
            return self.spark
        except Exception as e:
            logger.error(f"Error creating Spark session: {str(e)}")
            raise

    def process_csv_files(self, files):
        try:
            df = self.spark.read.csv(files, header=True, inferSchema=True)
            logger.info(f"Processed CSV files into DataFrame with {df.count()} records")
            return df
        except Exception as e:
            logger.error(f"Error processing CSV files: {str(e)}")
            return None

    def read_dimension_tables(self):
        return []

    def create_data_marts(self, result_df, dim_tables):
        return result_df, result_df

    def calculate_customer_monthly_purchases(self, customer_mart):
        return customer_mart

    def calculate_sales_incentives(self, sales_mart):
        return sales_mart

    def create_data_marts(self, sales_df: DataFrame, dim_tables: Dict[str, DataFrame]):
        try:
            store_df = dim_tables['store'].withColumnRenamed("id", "store_id")
            customer_df = dim_tables['customer'].withColumnRenamed("id", "customer_id")
            product_df = dim_tables['product'].withColumnRenamed("name", "product_name")

            enriched_df = sales_df \
                .join(customer_df, "customer_id") \
                .join(store_df, "store_id") \
                .join(product_df, sales_df.product_name == product_df.product_name)

            customer_mart = enriched_df \
                .withColumn('purchase_month', F.date_format('sales_date', 'yyyy-MM')) \
                .groupBy('customer_id', 'purchase_month') \
                .agg(
                    F.first('name').alias('customer_name'),
                    F.sum('total_cost').alias('total_purchases'),
                    F.count('*').alias('purchase_count'),
                    F.avg('total_cost').alias('avg_purchase_amount')
                )

            sales_mart = enriched_df \
                .withColumn('sales_month', F.date_format('sales_date', 'yyyy-MM')) \
                .select(
                    'store_id',
                    'sales_month',
                    'sales_person_id',
                    'total_cost',
                    'store_manager_name'
                )

            return customer_mart, sales_mart

        except Exception as e:
            logger.error(f"Error creating data marts: {str(e)}")
            raise

    def create_customer_mart(self, enriched_df):
        return enriched_df.groupBy('customer_id') \
            .agg(
                F.first('name').alias('customer_name'),
                F.sum('total_cost').alias('total_purchases'),
                F.count('*').alias('purchase_count'),
                F.avg('total_cost').alias('avg_purchase_amount')
            )

    def create_sales_mart(self, enriched_df):
        return enriched_df \
            .withColumn('sales_month', F.date_format('sales_date', 'yyyy-MM')) \
            .groupBy('store_id', 'sales_month') \
            .agg(
                F.sum('total_cost').alias('total_sales'),
                F.count('*').alias('transaction_count'),
                F.avg('total_cost').alias('avg_transaction_value')
            )

    def write_to_parquet(self, df, path, partition_cols=None):
        try:
            writer = df.write.mode('overwrite').format('parquet')
            if partition_cols:
                writer = writer.partitionBy(*partition_cols)
            writer.save(path)
            logger.info(f"Successfully wrote DataFrame to {path}")
        except Exception as e:
            logger.error(f"Error writing to Parquet: {str(e)}")
            raise

    def stop_session(self):
        if self.spark:
            self.spark.stop()
            logger.info("Spark session stopped")

    def clean_up_local_files(self, directory: str) -> None:
        try:
            if not os.path.exists(directory):
                logger.warning(f"Directory {directory} does not exist")
                return

            files = os.listdir(directory)
            if not files:
                logger.info(f"No files found in directory {directory}")
                return

            total_files = len(files)
            deleted_files = 0
            failed_deletions = 0

            for file in files:
                file_path = os.path.join(directory, file)
                try:
                    if os.path.isfile(file_path):
                        os.remove(file_path)
                        deleted_files += 1
                        logger.debug(f"Successfully deleted file: {file_path}")
                    elif os.path.isdir(file_path):
                        shutil.rmtree(file_path)
                        deleted_files += 1
                        logger.debug(f"Successfully deleted directory: {file_path}")
                except Exception as e:
                    failed_deletions += 1
                    logger.error(f"Failed to delete {file_path}: {str(e)}")

            logger.info(f"Cleanup completed: {deleted_files}/{total_files} files deleted successfully")
            if failed_deletions > 0:
                logger.warning(f"{failed_deletions} files failed to delete")

        except Exception as e:
            logger.error(f"Error during file cleanup: {str(e)}")
            raise 