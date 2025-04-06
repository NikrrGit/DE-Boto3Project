import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from dotenv import load_dotenv

def create_spark_session():
    return SparkSession.builder \
        .appName("SalesDataProcessing") \
        .config("spark.jars", "mysql-connector-java-8.0.28.jar") \
        .getOrCreate()

def read_sales_data(spark):
    return spark.read.csv("sales_data.csv", header=True, inferSchema=True)

def process_data(df):
    # Calculate monthly purchases per customer
    monthly_purchases = df.groupBy("customer_id", F.date_format("sales_date", "yyyy-MM").alias("purchase_month")) \
        .agg(
            F.sum("total_cost").alias("total_purchases"),
            F.count("*").alias("purchase_count"),
            F.avg("total_cost").alias("avg_purchase_amount")
        )

    # Calculate sales team incentives
    sales_incentives = df.groupBy("sales_person_id", F.date_format("sales_date", "yyyy-MM").alias("sales_month")) \
        .agg(
            F.sum("total_cost").alias("total_sales"),
            F.count("*").alias("transaction_count"),
            F.avg("total_cost").alias("avg_transaction_value")
        )

    return monthly_purchases, sales_incentives

def write_to_mysql(df, table_name):
    load_dotenv()
    
    # Get MySQL connection properties from environment variables
    mysql_properties = {
        "driver": "com.mysql.cj.jdbc.Driver",
        "url": f"jdbc:mysql://{os.getenv('DB_HOST')}/{os.getenv('DB_NAME')}",
        "user": os.getenv('DB_USER'),
        "password": os.getenv('DB_PASSWORD')
    }
    
    # Write DataFrame to MySQL
    df.write \
        .format("jdbc") \
        .mode("append") \
        .options(**mysql_properties) \
        .option("dbtable", table_name) \
        .save()

def main():
    try:
        # Create Spark session
        spark = create_spark_session()
        
        # Read sales data
        sales_df = read_sales_data(spark)
        print(f"Loaded {sales_df.count()} records from sales data")
        
        # Process data
        monthly_purchases, sales_incentives = process_data(sales_df)
        print(f"Generated {monthly_purchases.count()} monthly purchase records")
        print(f"Generated {sales_incentives.count()} sales incentive records")
        
        # Write to MySQL
        write_to_mysql(monthly_purchases, "customer_monthly_purchases")
        write_to_mysql(sales_incentives, "sales_team_incentives")
        print("Successfully wrote data to MySQL")
        
        # Stop Spark session
        spark.stop()
        
    except Exception as e:
        print(f"Error processing data: {str(e)}")
        raise e

if __name__ == "__main__":
    main() 