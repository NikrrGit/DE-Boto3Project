from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, avg, count, col
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Initialize Spark session with MySQL connector
spark = SparkSession.builder \
    .appName("SalesDataProcessing") \
    .config("spark.jars", "jars/mysql-connector-java-8.0.28.jar,jars/hadoop-aws-3.3.4.jar,jars/aws-java-sdk-bundle-1.12.262.jar") \
    .config("spark.hadoop.fs.s3a.access.key", os.getenv('AWS_ACCESS_KEY_ID')) \
    .config("spark.hadoop.fs.s3a.secret.key", os.getenv('AWS_SECRET_ACCESS_KEY')) \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

def process_sales_data():
    try:
        print("Reading sales data from S3...")
        # Read sales data from S3
        sales_df = spark.read.csv("s3a://project-de-001/sales_mart/sales_data.csv", 
                                header=True, 
                                inferSchema=True)
        
        print("Processing customer purchases...")
        # Process customer purchases
        customer_purchases = sales_df.groupBy("customer_id") \
            .agg(
                sum("total_cost").alias("total_purchases"),
                avg("total_cost").alias("avg_purchase"),
                count("*").alias("purchase_count")
            )
        
        print("Processing sales incentives...")
        # Process sales team incentives (5% commission)
        sales_incentives = sales_df.groupBy("sales_person_id") \
            .agg(
                sum("total_cost").alias("total_sales")
            ) \
            .withColumn("commission", col("total_sales") * 0.05)
        
        # MySQL connection properties
        mysql_properties = {
            "user": os.getenv('DB_USER'),
            "password": os.getenv('DB_PASSWORD'),
            "driver": "com.mysql.cj.jdbc.Driver"
        }
        
        mysql_url = f"jdbc:mysql://{os.getenv('DB_HOST')}/{os.getenv('DB_NAME')}"
        
        print("Writing customer purchases to MySQL...")
        # Write customer purchases to MySQL
        customer_purchases.write \
            .mode("overwrite") \
            .jdbc(url=mysql_url, 
                  table="customer_purchases", 
                  properties=mysql_properties)
        
        print("Writing sales incentives to MySQL...")
        # Write sales incentives to MySQL
        sales_incentives.write \
            .mode("overwrite") \
            .jdbc(url=mysql_url, 
                  table="sales_team_incentives", 
                  properties=mysql_properties)
        
        print("Data processing completed successfully!")
        
    except Exception as e:
        print(f"Error processing data: {str(e)}")
        raise e
    finally:
        spark.stop()

if __name__ == "__main__":
    process_sales_data() 