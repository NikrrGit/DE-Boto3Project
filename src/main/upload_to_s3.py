import boto3
import os
from dotenv import load_dotenv

def upload_to_s3(file_path, bucket_name, s3_key):
    try:
        # Load environment variables
        load_dotenv()
        
        # Initialize S3 client
        s3_client = boto3.client(
            's3',
            aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
            region_name=os.getenv('AWS_DEFAULT_REGION')
        )
        
        # Upload file
        s3_client.upload_file(file_path, bucket_name, s3_key)
        print(f"Successfully uploaded {file_path} to s3://{bucket_name}/{s3_key}")
        return True
        
    except Exception as e:
        print(f"Error uploading to S3: {str(e)}")
        return False

if __name__ == "__main__":
    bucket_name = "project-de-001"
    file_path = "sales_data.csv"
    s3_key = "sales_mart/sales_data.csv"  # Organizing data in a sales_mart folder
    
    upload_to_s3(file_path, bucket_name, s3_key) 