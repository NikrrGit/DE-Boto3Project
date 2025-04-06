import boto3
import logging

logger = logging.getLogger(__name__)

class S3ClientProvider:
    def __init__(self, use_iam_role=False, role_arn=None, session_name=None):
        self.use_iam_role = use_iam_role
        self.role_arn = role_arn
        self.session_name = session_name

    def get_client(self):
        try:
            if self.use_iam_role and self.role_arn and self.session_name:
                logger.info("Assuming IAM role for S3 client")
                sts_client = boto3.client('sts')
                assumed_role = sts_client.assume_role(
                    RoleArn=self.role_arn,
                    RoleSessionName=self.session_name
                )
                credentials = assumed_role['Credentials']
                s3_client = boto3.client(
                    's3',
                    aws_access_key_id=credentials['AccessKeyId'],
                    aws_secret_access_key=credentials['SecretAccessKey'],
                    aws_session_token=credentials['SessionToken']
                )
            else:
                logger.info("Using default S3 client")
                s3_client = boto3.client('s3')
            return s3_client
        except Exception as e:
            logger.error(f"Error creating S3 client: {str(e)}")
            raise 