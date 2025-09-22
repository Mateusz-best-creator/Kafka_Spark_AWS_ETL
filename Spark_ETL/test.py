import unittest
from pyspark.sql import SparkSession
from pyspark.testing.utils import assertDataFrameEqual, assertSchemaEqual # type: ignore
from transformations import remove_unused_columns, add_country
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
import random

class PySparkTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.appName("Testing PySpark Example").getOrCreate()
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

    def test_geocode_udf_func(self):
        pass


if __name__ == "__main__":
    unittest.main()
