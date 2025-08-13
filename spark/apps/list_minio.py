import boto3


BUCKET_NAME =  "data"
ENDPOINT_URL = "http://minio:9000"
ACCESS_KEY = "admin"
SECRET_KEY ="password"
s3 = boto3.client(
    's3',
    endpoint_url=ENDPOINT_URL,
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY
)

# Get the list of all files
response = s3.list_objects_v2(Bucket=BUCKET_NAME,Prefix="controls/")

print(response)

files = [obj['Key'] for obj in response.get('Contents', [])]

print(files)