import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
import boto3
import os
from dotenv import load_dotenv
import io

# Load environment variables
load_dotenv()

# Generate random sales data
def generate_sales_data(num_records=100):
    # Product list
    products = ['quaker oats', 'refined oil', 'sugar', 'flour']
    
    # Generate random dates within the last 3 months
    end_date = datetime.now()
    start_date = end_date - timedelta(days=90)
    
    data = {
        'customer_id': np.random.randint(1, 11, num_records),
        'store_id': np.random.choice([121, 122, 123], num_records),
        'product_name': np.random.choice(products, num_records),
        'sales_date': [(start_date + timedelta(days=random.randint(0, 90))).strftime('%Y-%m-%d') for _ in range(num_records)],
        'sales_person_id': np.random.randint(1, 10, num_records),
        'price': np.random.uniform(10, 200, num_records).round(2),
        'quantity': np.random.randint(1, 10, num_records)
    }
    
    df = pd.DataFrame(data)
    df['total_cost'] = (df['price'] * df['quantity']).round(2)
    
    return df

def upload_to_s3(df, bucket_name, s3_key):
    try:
        # Initialize S3 client
        s3_client = boto3.client(
            's3',
            aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
            region_name=os.getenv('AWS_DEFAULT_REGION')
        )
        
        # Convert DataFrame to CSV in memory
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False)
        
        # Upload to S3
        s3_client.put_object(
            Bucket=bucket_name,
            Key=s3_key,
            Body=csv_buffer.getvalue()
        )
        
        print(f"Successfully uploaded data to s3://{bucket_name}/{s3_key}")
        return True
        
    except Exception as e:
        print(f"Error uploading to S3: {str(e)}")
        return False

# Generate and save the data
if __name__ == "__main__":
    # Generate data
    sales_data = generate_sales_data(100)
    
    # Upload directly to S3
    bucket_name = "project-de-001"
    s3_key = "sales_mart/sales_data.csv"
    
    upload_to_s3(sales_data, bucket_name, s3_key) 