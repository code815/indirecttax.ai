import os, boto3, botocore

ENDPOINT=os.getenv("S3_ENDPOINT","http://localhost:9000")
KEY=os.getenv("S3_ACCESS_KEY","minio")
SECRET=os.getenv("S3_SECRET_KEY","minio123")
BUCKET=os.getenv("S3_BUCKET","bulletin-raw")

s3 = boto3.client("s3", endpoint_url=ENDPOINT,
                  aws_access_key_id=KEY, aws_secret_access_key=SECRET)

def ensure_bucket():
    try:
        s3.head_bucket(Bucket=BUCKET)
    except botocore.exceptions.ClientError:
        s3.create_bucket(Bucket=BUCKET)

def put_bytes(key: str, data: bytes) -> str:
    s3.put_object(Bucket=BUCKET, Key=key, Body=data)
    return f"s3://{BUCKET}/{key}"
def presign(key: str, minutes: int = 1440) -> str:
    """Return a temporary URL for an object key."""
    try:
        return s3.generate_presigned_url(
            "get_object",
            Params={"Bucket": BUCKET, "Key": key},
            ExpiresIn=minutes * 60,
        )
    except Exception:
        # If presign fails (e.g., in dev), return a console-ish path
        return f"{ENDPOINT}/browser/{BUCKET}/{key}"
