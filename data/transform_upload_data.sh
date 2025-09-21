#!/bin/bash

python3 transform_to_parquet.py
python3 upload_data_to_s3.py