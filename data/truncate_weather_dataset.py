import pandas as pd

# Input and output file paths
input_csv = "./initial_format/weather_data.csv"
output_csv = "./initial_format/weather_data.csv"

df = pd.read_csv(input_csv, nrows=15000)
df.to_csv(output_csv, index=False)

print(f"Saved first 15000 rows to {output_csv}")
