from pyspark.sql import functions as F
from api import GeocoderAPI

def remove_unused_columns(df,
                          columns_to_drop):
    return df.drop(*columns_to_drop)

def add_country(df,
                country_column_name="Country",
                column_value="USA"):
    """
    
    """
    return df.withColumn(country_column_name, F.lit(column_value))

def longitude_latitude_transformation(df):
    """
    
    """
    geocoder_api = GeocoderAPI()
    geocode_udf = geocoder_api.udf_get_lat_lon_hash()
    combined_column_name = "LatLonGeo"

    df = df.withColumn(combined_column_name, geocode_udf(F.col("Country"), F.col("Location")))
    df = df.withColumn("Latitude", F.col(f"{combined_column_name}.Latitude")) \
           .withColumn("Longitude", F.col(f"{combined_column_name}.Longitude")) \
           .withColumn("GeoHash", F.col(f"{combined_column_name}.GeoHash")) \
           .drop(F.col(combined_column_name))
    return df

def geohash_transformation_from_lat_lon(df):
    """
    
    """

    geocoder_api = GeocoderAPI()
    geohash_udf = geocoder_api.udf_get_hash_from_lat_lon()
    df = df.withColumn("GeoHash", geohash_udf(F.col("latitude"), F.col("longitude")))

    return df

def fill_missing_values_with_mode(df):
    pass

def joining_datasets(weather_df, 
                     traffic_df,
                     key_for_join="GeoHash",
                     join_type="left"):
    return traffic_df.join(other=weather_df,
                           on=key_for_join, 
                           how=join_type)
