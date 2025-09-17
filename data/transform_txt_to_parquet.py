import pandas as pd
import os

for file in os.listdir("./txt_format/"):
    df = pd.read_csv(f"./txt_format/{file}")
    new_name = file.split(".")[0] + ".parquet"
    print(f"New parquet file = {new_name}")
    df.to_parquet(f"./parquet_format/{new_name}")