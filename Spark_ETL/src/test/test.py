import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'app')))

import unittest
from pyspark.sql import SparkSession
from pyspark.testing.utils import assertDataFrameEqual, assertSchemaEqual # type: ignore
from transformations import remove_unused_columns, add_country # type: ignore
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, IntegralType
import random
from api import GeocoderAPI # type: ignore
from pyspark.sql.functions import col

class PySparkTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.appName("Testing PySpark ETL appication").getOrCreate()
        cls.spark.sparkContext.setLogLevel("ERROR") # Make less logs in the console

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

class TestTranformation(PySparkTestCase):

    def test_remove_unused_columns(self):

        original_data = [{
                "id": i,
                "age": random.randint(18, 60),
                "unused_col1": str(random.randint(0, 10)) + "_flight",
                "unused_col2": random.randint(100, 200),
            }
            for i in range(5)
        ]
        expected_data = [{"id": row["id"], "age": row["age"]} for row in original_data]
        o_df = remove_unused_columns(self.spark.createDataFrame(original_data), ["unused_col1", "unused_col2"])
        e_df = self.spark.createDataFrame(expected_data)

        assertDataFrameEqual(o_df, e_df)
        assertSchemaEqual(o_df.schema, e_df.schema)

    def test_add_country(self):
        
        original_data = [{"id": i, "age": random.randint(16, 60)} for i in range(5)]
        expected_data = [{"id": row["id"], "age": row["age"], "Country_Code": "FIN"} for row in original_data]

        schema = StructType([StructField("id", IntegerType(), True),
                               StructField("age", IntegerType(), True),
                               StructField("Country_Code", StringType(), True)])
        expected_schema = StructType([StructField("id", IntegerType(), True),
                               StructField("age", IntegerType(), True),
                               StructField("Country_Code", StringType(), False)])

        o_df = add_country(self.spark.createDataFrame(original_data, schema=schema), country_column_name="Country_Code", column_value="FIN")
        e_df = self.spark.createDataFrame(expected_data, schema=expected_schema)

        self.assertEqual(o_df.schema, e_df.schema)
        self.assertEqual(sorted(o_df.collect()), sorted(e_df.collect()))

    def test_GeocoderAPI_lat_lng_hash(self):
        geocoder = GeocoderAPI()

        lat_1, lng_1, hash1 = geocoder.get_latitude_longitude_geohash_from_country_city("Germany", "Berlin")
        lat_2, lng_2, hash2 = geocoder.get_latitude_longitude_geohash_from_country_city("USA", "Chicago")

        expected_data = [(52.5173885, 13.3951309, "u33"),
                         (41.8755616, -87.6244212, "dp3")]
        self.assertEqual(lat_1, expected_data[0][0])
        self.assertEqual(lng_1, expected_data[0][1])
        self.assertEqual(hash1, expected_data[0][2])

        self.assertEqual(lat_2, expected_data[1][0])
        self.assertEqual(lng_2, expected_data[1][1])
        self.assertEqual(hash2, expected_data[1][2])

    def test_GeocoderAPI_hash_from_lat_lng(self):
        geocoder = GeocoderAPI()

        lat1, lng2 = 41.8303087, -0.1761911
        hash1 = geocoder.get_geohash_from_latitude_longitude(lat1, lng2)
        lat2, lng2 = 70.6203087, 10.9761911
        hash2 = geocoder.get_geohash_from_latitude_longitude(lat2, lng2)
        lat3, lng3 = 30.6203087, 20.9761911
        hash3 = geocoder.get_geohash_from_latitude_longitude(lat3, lng3)
        
        expected_values = ["ezr", "uhx", "smp"]
        self.assertEqual(hash1, expected_values[0])
        self.assertEqual(hash2, expected_values[1])
        self.assertEqual(hash3, expected_values[2])

        for h in (hash1, hash2, hash3):
            self.assertEqual(3, len(h))

    def test_udfs(self):

        geocoder = GeocoderAPI()
        data = [("Germany", "Berlin"),
                ("USA", "Chicago")]
        schema = StructType([StructField("Country", StringType(), True),
                             StructField("City", StringType(), True)])
        df = self.spark.createDataFrame(data, schema)
        original_data = df.withColumn("LatLonHash", geocoder.udf_get_lat_lon_hash()(col("City"), col("Country")))
        result = original_data.collect()
        original_result = []
        for row in result:
            original_result.append((row["LatLonHash"].Latitude, row["LatLonHash"].Longitude, row["LatLonHash"].GeoHash))

        for index, record in enumerate(original_result):
            if index == 0:
                self.assertEqual(record[0], 52.5173885)
                self.assertEqual(record[1], 13.3951309)
                self.assertEqual(record[2], "u33")
            else:
                self.assertEqual(record[0], 41.8755616)
                self.assertEqual(record[1], -87.6244212)
                self.assertEqual(record[2], "dp3")

        data = [(41.8303087, -0.1761911),
                (70.6203087, 10.9761911)]
        schema = StructType([StructField("Latitude", DoubleType(), True),
                             StructField("Longitude", DoubleType(), True)])
        df = self.spark.createDataFrame(data, schema)
        original_data = df.withColumn("GeoHash", geocoder.udf_get_hash_from_lat_lon()(col("Latitude"), col("Longitude")))
        for index, record in enumerate(original_data.collect()):
            if index == 0:
                self.assertEqual("ezr", record.GeoHash)
            if index == 1:
                self.assertEqual("uhx", record.GeoHash)



    def test_longitude_latitude_transformation(self):
        pass



if __name__ == "__main__":
    unittest.main()
