import pandas as pd
import os

input_folder = "./initial_format/"
output_folder = "./parquet_format/"
input_format = ".csv"
input_separator = ","

os.makedirs(output_folder, exist_ok=True)

for file in os.listdir(input_folder):
    if file.endswith(input_format):
        df = pd.read_csv(os.path.join(input_folder, file), sep=input_separator, header=0)
        new_name = file.split(".")[0] + ".parquet"
        print(f"New parquet file = {new_name}")
        df.to_parquet(os.path.join(output_folder, new_name), engine='pyarrow', index=False)

