
class DatasetLoader:

    @staticmethod
    def read(spark,
             path,
             format='parquet',
             header=True,
             infer_schema=True,
             compression_name=None):
        if compression_name:
            df = spark.read.format(format) \
                        .option("header", header) \
                        .option("inferSchema", infer_schema) \
                        .option("compression", compression_name) \
                        .load(path)
        else:
            df = spark.read.format(format) \
                        .option("header", header) \
                        .option("inferSchema", infer_schema) \
                        .load(path)
        return df
