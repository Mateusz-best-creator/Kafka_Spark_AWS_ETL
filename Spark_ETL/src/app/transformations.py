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
    df = df.withColumn("Latitude", F.col(f"{combined_column_name}.Latitude").cast("string")) \
           .withColumn("Longitude", F.col(f"{combined_column_name}.Longitude").cast("string")) \
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

def fill_missing_values(df,
                        col_fill_mapping={"vehicle_speed (km/h)": "avg",
                                          "traffic_pattern": "mode",
                                          "incident_report": "mode",
                                          "event_type": "mode"}):
    for column_name, method in col_fill_mapping.items():
        if method == "avg":
            mean_value = df.select(F.mean(column_name)).collect()[0][0]
            df = df.na.fill(mean_value, subset=[column_name])
        elif method == "mode":
            mode_value = df.select(F.mode(column_name)).collect()[0][0]
            df = df.na.fill(mode_value, subset=[column_name])
    return df

def group_weather(weather_df):
    df = weather_df.groupBy(F.col("GeoHash")).agg(
        F.avg(F.col("Temperature_C")).alias("Avg_Temperature_C"),
        F.avg(F.col("Precipitation_mm")).alias("Avg_Precipitation_mm"),
        F.avg(F.col("Wind_Speed_kmh")).alias("Avg_Wind_Speed_kmh"),
        F.max(F.col("Latitude")).alias("Latitude"),
        F.max(F.col("Longitude")).alias("Longitude"),
        F.max(F.col("Country")).alias("Country"),
        F.max(F.col("Location")).alias("City"),
        F.max(F.col("Date_Time")).alias("Timestamp")
    )
    return df

def joining_datasets(weather_df, 
                     traffic_df,
                     key_for_join="GeoHash",
                     join_type="left"):
    return traffic_df.join(other=weather_df,
                           on=key_for_join, 
                           how=join_type)
