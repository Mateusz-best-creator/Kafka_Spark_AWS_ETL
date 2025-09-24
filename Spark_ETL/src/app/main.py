from pyspark.sql import SparkSession
import os
from dotenv import load_dotenv
from data import DatasetLoader
from transformations import add_country, longitude_latitude_transformation, \
    geohash_transformation_from_lat_lon, joining_datasets, remove_unused_columns, \
    fill_missing_values, group_weather
from text_encryption import PIIEncryption

class ETL:

    def __init__(self, 
                 spark, 
                 bucket_name,
                 weather_filename,
                 public_traffic_filename):
        """
        
        """
                
        self.spark = spark
        self.bucket_name = bucket_name

        self.weather_filename = weather_filename
        self.public_traffic_filename = public_traffic_filename

        self.weather_data = None
        self.public_traffic_data = None
        self.joined_data = None
        
    def extract(self):

        self.weather_data = DatasetLoader.read(spark=self.spark,
                                               path=f"s3a://{self.bucket_name}/{self.weather_filename}")
        self.public_traffic_data = DatasetLoader.read(spark=self.spark,
                                                      path=f"s3a://{self.bucket_name}/{self.public_traffic_filename}")
        print(f"Datasets lengths: \nWeather = {self.weather_data.count()}\nPublic traffic = {self.public_traffic_data.count()}")

    def transform(self):

        self.weather_data = add_country(self.weather_data)
        self.weather_data.show()
        self.weather_data = longitude_latitude_transformation(self.weather_data)
        self.weather_data.show()
        self.public_traffic_data = geohash_transformation_from_lat_lon(self.public_traffic_data)
        self.public_traffic_data = remove_unused_columns(self.public_traffic_data, columns_to_drop=["sensor_id", 
                                                                                                    "latitude", 
                                                                                                    "longitude", 
                                                                                                    "accident_hotspot"])
        self.public_traffic_data = fill_missing_values(self.public_traffic_data)
        self.public_traffic_data.show()
        self.weather_data = group_weather(self.weather_data)
        self.weather_data.show()
        self.joined_data = joining_datasets(self.weather_data, self.public_traffic_data)
        print("\n\n\nHALLOOOOOO\n\n")
        self.weather_data.show()
        self.public_traffic_data.show()
        PII_encryption = PIIEncryption()
        self.weather_data = PII_encryption.encrypt_columns(self.weather_data,
                                                          columns=["City",
                                                                   "Latitude",
                                                                   "Longitude"])
        print(f"\n\nDatasets after transformations:\n\n")
        self.weather_data.show(10)
        self.public_traffic_data.show(10)
        print(f"\n\nJoined dataset after transformations and encryption:\n\n")
        self.joined_data.show(20)

    def load(self):
        pass

    def __call__(self):
        self.extract()
        self.transform()
        self.load()


if __name__ == "__main__":

    load_dotenv(dotenv_path="../../../.env")
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
                  weather_filename="weather_data.parquet",
                  public_traffic_filename="iot_edge_computing_public_management.parquet")
    etl_job()

    spark.stop()
