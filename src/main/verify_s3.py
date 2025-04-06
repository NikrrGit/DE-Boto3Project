import boto3
import os
from dotenv import load_dotenv

def verify_s3_connection():
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
        
        # List all buckets
        response = s3_client.list_buckets()
        print("Successfully connected to S3!")
        print("\nAvailable buckets:")
        for bucket in response['Buckets']:
            print(f"- {bucket['Name']}")
            
        return True
        
    except Exception as e:
        print(f"Error connecting to S3: {str(e)}")
        return False

if __name__ == "__main__":
    verify_s3_connection() 