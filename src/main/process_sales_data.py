from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Initialize Spark session
spark = SparkSession.builder \
    .appName("SalesDataProcessing") \
    .config("spark.jars", "mysql-connector-java-8.0.28.jar") \
    .getOrCreate()

# Read sales data from local file
def read_sales_data():
    local_path = "sales_data.csv"  # The file is in the root directory
    return spark.read.csv(local_path, header=True, inferSchema=True)

# Process customer monthly purchases
def process_customer_purchases(df):
    return df.groupBy("customer_id", date_format("sales_date", "yyyy-MM").alias("purchase_month")) \
        .agg(
            sum("total_cost").alias("total_purchases"),
            count("*").alias("purchase_count"),
            avg("total_cost").alias("avg_purchase_amount")
        )

# Process sales team incentives
def process_sales_incentives(df):
    return df.groupBy("sales_person_id", date_format("sales_date", "yyyy-MM").alias("sales_month")) \
        .agg(
            sum("total_cost").alias("total_sales"),
            count("*").alias("transaction_count"),
            avg("total_cost").alias("avg_transaction_value")
        )

# Write to MySQL
def write_to_mysql(df, table_name):
    df.write \
        .format("jdbc") \
        .option("url", f"jdbc:mysql://{os.getenv('DB_HOST')}/{os.getenv('DB_NAME')}") \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .option("dbtable", table_name) \
        .option("user", os.getenv('DB_USER')) \
        .option("password", os.getenv('DB_PASSWORD')) \
        .mode("overwrite") \
        .save()

def main():
    try:
        # Read data
        print("Reading sales data...")
        sales_df = read_sales_data()
        
        # Process data
        print("Processing customer purchases...")
        customer_purchases = process_customer_purchases(sales_df)
        print("Processing sales incentives...")
        sales_incentives = process_sales_incentives(sales_df)
        
        # Write to MySQL
        print("Writing customer purchases to MySQL...")
        write_to_mysql(customer_purchases, "customer_monthly_purchases")
        print("Writing sales incentives to MySQL...")
        write_to_mysql(sales_incentives, "sales_team_incentives")
        
        print("Data processing completed successfully!")
        
    except Exception as e:
        print(f"Error processing data: {str(e)}")
    finally:
        spark.stop()

if __name__ == "__main__":
    main() 