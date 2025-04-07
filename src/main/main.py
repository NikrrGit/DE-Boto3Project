import os
import sys
from dotenv import load_dotenv
from process_sales_data import process_sales_data
from generate_sales_data import generate_sales_data, upload_to_s3

# Load environment variables
load_dotenv()

def run_pipeline():
    try:
        print("\n⚙️ Step 1: Generating sales data and uploading to S3...")
        # Generate sales data and upload to S3
        sales_data = generate_sales_data(100)
        upload_to_s3(sales_data, "project-de-001", "sales_mart/sales_data.csv")
        print("✅ Data generation and upload completed successfully")
        
        print("\n⚙️ Step 2: Processing data from S3...")
        # Process the sales data
        process_sales_data()
        print("✅ Data processing completed successfully")
        
        print("\n🎉 Pipeline completed successfully!")
        
    except Exception as e:
        print(f"\n❌ Pipeline failed: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    run_pipeline() 