from pyspark.sql import SparkSession
import os
from dotenv import load_dotenv

class ETL:

    def __init__(self, spark, bucket_name, questionaire_filename):
        self.spark = spark
        self.bucket_name = bucket_name
        self.questionaire_filename = questionaire_filename
        self.questionaire_data = None

    def extract(self):
        # Read Parquet from S3
        self.questionaire_data = self.spark.read.parquet(
            f"s3a://{self.bucket_name}/{self.questionaire_filename}"
        )
        self.questionaire_data.show()

    def process(self):
        self.extract()


if __name__ == "__main__":

    load_dotenv(dotenv_path="../.env")
    AWS_ACCESS_KEY = os.getenv("ACCESS_KEY")
    AWS_SECRET_KEY = os.getenv("SECRET_ACCESS_KEY")
    BUCKET_NAME = os.getenv("BUCKET_NAME")

    spark = SparkSession.builder \
        .appName("S3SparkIntegration") \
        .master("local[*]") \
        .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.jars.packages", 
                "org.apache.hadoop:hadoop-aws:3.3.2,"
                "com.amazonaws:aws-java-sdk-bundle:1.12.262") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    etl_job = ETL(spark=spark,
                  bucket_name=BUCKET_NAME,
                  questionaire_filename="dane_ankiet.parquet")
    etl_job.process()

    spark.stop()
