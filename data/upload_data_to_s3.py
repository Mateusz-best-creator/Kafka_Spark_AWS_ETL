import boto3
import os
from dotenv import load_dotenv

load_dotenv(dotenv_path="../.env")

# Creating Session With Boto3.
session = boto3.Session(aws_access_key_id=os.getenv("ACCESS_KEY"),
                        aws_secret_access_key=os.getenv("SECRET_ACCESS_KEY"))

# Creating S3 Resource From the Session.
s3 = session.resource('s3')
bucket_name = os.getenv("BUCKET_NAME")
file_path = "./parquet_format"

for file in os.listdir(file_path):
    if file.endswith(".parquet"):
        local_file = f"{file_path}/{file}"
        s3_file_key = file

        try:
            s3.Bucket(bucket_name).upload_file(local_file, s3_file_key)
            print(f"File: {file} uploaded succesfully!")
        except Exception as e:
            print(f"Expcetion occured!: {e}")