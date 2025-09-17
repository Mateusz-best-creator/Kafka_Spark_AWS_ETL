import pandas as pd
import os

input_folder = "./txt_format/"
output_folder = "./parquet_format/"

os.makedirs(output_folder, exist_ok=True)

for file in os.listdir(input_folder):
    if file.endswith(".txt"):
        df = pd.read_csv(os.path.join(input_folder, file), sep="\t", header=0)
        new_name = file.split(".")[0] + ".parquet"
        print(f"New parquet file = {new_name}")
        df.to_parquet(os.path.join(output_folder, new_name), engine='pyarrow', index=False)

# Check if we can read parquet file
df = pd.read_parquet(os.path.join(output_folder, "dane_ankiet.parquet"))
print(df)
