import pandas as pd

# Specify the input and output file paths
input_file = 'instance_events-000000000033.json.gz'  # Replace with your .parquet.gz file path
output_file = 'output_file.csv'       # Replace with your desired .csv file path

# Read the Parquet file
df = pd.read_parquet(input_file, engine='pyarrow')  # Or engine='fastparquet'

# Write to a CSV file
df.to_csv(output_file, index=False)

print(f"File converted and saved to {output_file}")
