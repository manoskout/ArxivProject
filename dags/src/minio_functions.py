from airflow.providers.amazon.aws.hooks.s3 import S3Hook

def validate_bucket(bucket_name: str, s3_hook: S3Hook):
    """Check if the bucket exists in MinIO; create it if it doesn't."""
    if not s3_hook.check_for_bucket(bucket_name):
        s3_hook.create_bucket(bucket_name)
        print(f"Created bucket: {bucket_name}")
    else:
        print(f"Bucket already exists: {bucket_name}")