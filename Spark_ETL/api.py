from opencage.geocoder import OpenCageGeocode
from dotenv import load_dotenv
import os
from typing import Tuple
from pyspark.sql.functions import udf
from pyspark.sql.types import StructField, StructType, DoubleType, StringType

class GeocoderAPI:
    """
    A wrapper around the OpenCage Geocoding API for retrieving geospatial data such as
    latitude, longitude, and geohash values from location information.

    This class loads an API key from a `.env` file (variable name: `GEOCODE_KEY`) and
    provides helper methods for forward and reverse geocoding.
    """
    def __init__(self):
        """Initializes the GeocoderAPI by loading the OpenCage API key from .env and setting up the geocoder client.

        Loads the environment variable `GEOCODE_KEY` and sets a default geohash precision.
        """
        load_dotenv(dotenv_path="../.env")
        key = os.getenv("GEOCODE_KEY")
        self.geocoder = OpenCageGeocode(key)
        self.geohash_precision = 4

    def get_latitude_longitude_geohash_from_country_city(self,
                                                         country: str,
                                                         city: str) -> Tuple[float, float, str] | Tuple[None, None, None]:
        """Retrieves latitude, longitude, and geohash from a given country and city name.

        Args:
            country (str): The country name.
            city (str): The city name.

        Returns:
            tuple: (latitude (float), longitude (float), geohash (str)) if found, otherwise (None, None, None).
        """
        if country is None or city is None:
            return (None, None, None)
        query = f'{city}, {country}'
        query_result = self.geocoder.geocode(query)[0]
        lat = query_result["geometry"]["lat"]
        lon = query_result["geometry"]["lng"]
        geohash = query_result["annotations"]["geohash"][:self.geohash_precision]

        return lat, lon, geohash
    
    def udf_get_lat_lon_hash(self):
        return udf(lambda city, country: 
                   self.get_latitude_longitude_geohash_from_country_city(country, city), StructType([StructField("Latitude", DoubleType(), True),
                                                                                                      StructField("Longitude", DoubleType(), True),
                                                                                                      StructField("GeoHash", StringType(), True)]))
    
    def get_geohash_from_latitude_longitude(self,
                                            latitude: float,
                                            longitude: float) -> str | None:
        """Retrieves geohash for a given latitude and longitude.

        Args:
            latitude (float): The latitude coordinate.
            longitude (float): The longitude coordinate.

        Returns:
            str: Geohash string truncated to the configured precision, or None if inputs are invalid.
        """
        if latitude is None or longitude is None:
            return None
        query_result = self.geocoder.reverse_geocode(latitude, longitude)[0]
        return query_result["annotations"]["geohash"][:self.geohash_precision]
    

    def udf_get_hash_from_lat_lon(self):
        return udf(lambda lat, lng: self.get_geohash_from_latitude_longitude(lat, lng), StringType())