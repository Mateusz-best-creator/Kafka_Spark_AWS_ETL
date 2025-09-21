from pyspark.sql import functions as F
from opencage.geocoder import OpenCageGeocode
from dotenv import load_dotenv
import os
from pyspark.sql.types import StructField, StructType, DoubleType, StringType

def add_country(df,
                country_column_name="Country",
                column_value="USA"):
    """
    
    """
    return df.withColumn(country_column_name, F.lit(column_value))

def geocode_udf_func(geocoder,
                country_column,
                city_column,
                geohash_precision=4):
    """
    
    """
    if country_column is None and city_column is None:
        return (None, None, None)
    query = f'{city_column}, {country_column}'
    query_result = geocoder.geocode(query)[0]
    return query_result["geometry"]["lat"], query_result["geometry"]["lng"], query_result["annotations"]["geohash"][:geohash_precision]

def get_geocode_udf(geocoder):
    """
    
    """
    return F.udf(lambda country, location: geocode_udf_func(geocoder, country, location), StructType([StructField("Latitude", DoubleType(), True),
                                                                                                      StructField("Longitude", DoubleType(), True),
                                                                                                      StructField("GeoHash", StringType(), True)]))

def longitude_latitude_transformation(df):
    """
    
    """
    load_dotenv(dotenv_path="../.env")
    key = os.getenv("GEOCODE_KEY")
    geocoder = OpenCageGeocode(key)
    geocode_udf = get_geocode_udf(geocoder)
    combined_column_name = "LatLonGeo"

    df = df.withColumn(combined_column_name, geocode_udf(F.col("Country"), F.col("Location")))
    df = df.withColumn("Latitude", F.col(f"{combined_column_name}.Latitude")) \
           .withColumn("Longitude", F.col(f"{combined_column_name}.Longitude")) \
           .withColumn("GeoHash", F.col(f"{combined_column_name}.GeoHash")) \
           .drop(F.col(combined_column_name))

    print("\n\nAfter lat, lon transformations:\n\n")
    df.show()

def get_geohash(geocoder,
                latitude,
                longitude,
                geohash_precision=4):
    """
    
    """
    results = geocoder.reverse_geocode(latitude, longitude)[0]
    return results["annotations"]["geohash"][:geohash_precision]

def get_geohash_func(geocoder):
    return F.udf(lambda lat, lon: get_geohash(geocoder, lat, lon), StringType())

def geohash_transformation_from_lat_lon(df):
    """
    
    """

    load_dotenv(dotenv_path="../.env")
    key = os.getenv("GEOCODE_KEY")
    geocoder = OpenCageGeocode(key)
    geohash_udf = get_geohash_func(geocoder)

    df = df.withColumn("GeoHash", geohash_udf(F.col("latitude"), F.col("longitude")))
    print("\n\nAfter adding Geohash column:\n\n")
    df.show()

    return df