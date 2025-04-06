from boto3.session import Session
from botocore.config import Config
from botocore.exceptions import ClientError
import boto3
import logging
import os
from typing import List, Tuple, Dict
import pandas as pd

logger = logging.getLogger(__name__)

class S3Operations:
    def __init__(self, s3_client):
        self.s3_client = s3_client
        self.bucket_name = "project-de-001"
        self.directories = {
            'sales_mart': 'sales_data_mart/',
            'sales_error': 'sales_data_error/',
            'sales_processed': 'sales_data_processed/'
        }
        self.required_columns = [
            "customer_id", "store_id", "product_name", 
            "sales_date", "sales_person_id", "price", 
            "quantity", "total_cost"
        ]

    def list_files(self, bucket_name):
        try:
            response = self.s3_client.list_objects_v2(Bucket=bucket_name)
            files = [content['Key'] for content in response.get('Contents', [])]
            logger.info(f"Found {len(files)} files in bucket {bucket_name}")
            return files
        except Exception as e:
            logger.error(f"Error listing files in bucket {bucket_name}: {str(e)}")
            return []

    def filter_csv_files(self, files):
        csv_files = [file for file in files if file.endswith('.csv')]
        non_csv_files = [file for file in files if not file.endswith('.csv')]
        logger.info(f"Filtered {len(csv_files)} CSV files and {len(non_csv_files)} non-CSV files")
        return csv_files, non_csv_files

    def move_to_error(self, file_key: str, reason: str = None):
        try:
            filename = os.path.basename(file_key)
            if reason:
                name, ext = os.path.splitext(filename)
                error_key = f"{self.directories['sales_error']}{name}_ERROR_{reason}{ext}"
            else:
                error_key = f"{self.directories['sales_error']}{filename}"

            self.s3_client.copy_object(
                Bucket=self.bucket_name,
                CopySource={'Bucket': self.bucket_name, 'Key': file_key},
                Key=error_key
            )
            self.s3_client.delete_object(
                Bucket=self.bucket_name,
                Key=file_key
            )
            logger.info(f"Moved {file_key} to error folder")
        except ClientError as e:
            logger.error(f"Error moving file to error folder: {str(e)}")

    def download_files(self, files, local_directory):
        downloaded_files = []
        for file in files:
            try:
                local_path = os.path.join(local_directory, file)
                os.makedirs(os.path.dirname(local_path), exist_ok=True)
                self.s3_client.download_file('your_bucket_name', file, local_path)
                downloaded_files.append(local_path)
                logger.info(f"Downloaded {file} to {local_path}")
            except Exception as e:
                logger.error(f"Error downloading {file}: {str(e)}")
        return downloaded_files

    def validate_csv_file(self, file_path: str) -> bool:
        try:
            df = pd.read_csv(file_path, nrows=1)
            return True
        except Exception as e:
            logger.error(f"Invalid CSV file {file_path}: {str(e)}")
            return False

    def validate_csv_schema(self, file_path: str) -> Tuple[bool, List[str], List[str]]:
        try:
            df = pd.read_csv(file_path, nrows=0)
            columns = df.columns.tolist()
            missing_cols = [col for col in self.required_columns if col not in columns]
            extra_cols = [col for col in columns if col not in self.required_columns]
            is_valid = len(missing_cols) == 0
            return is_valid, missing_cols, extra_cols
        except Exception as e:
            logger.error(f"Error validating schema: {str(e)}")
            return False, [], [] 