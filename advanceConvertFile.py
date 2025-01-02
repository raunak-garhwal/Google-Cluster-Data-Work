import os
import pandas as pd

def convert_files_to_csv(input_folder, output_folder):
    # Create the output folder if it doesn't exist
    os.makedirs(output_folder, exist_ok=True)
    
    # Iterate over all files in the input folder
    for file_name in os.listdir(input_folder):
        input_file = os.path.join(input_folder, file_name)
        
        # Check file extension and process accordingly
        if file_name.endswith(".parquet") or file_name.endswith(".parquet.gz"):
            # Construct output file path
            output_file = os.path.join(output_folder, file_name.replace(".parquet", ".csv").replace(".gz", ""))
            
            # Read the Parquet file
            df = pd.read_parquet(input_file, engine='pyarrow')  # Or use engine='fastparquet'
            df.to_csv(output_file, index=False)
            print(f"Converted Parquet: {file_name} -> {output_file}")
        
        elif file_name.endswith(".json.gz"):
            # Construct output file path
            output_file = os.path.join(output_folder, file_name.replace(".json.gz", ".csv"))
            
            # Read the JSON file
            df = pd.read_json(input_file, compression='gzip', lines=True)  # Use 'lines=True' if the JSON is line-delimited
            df.to_csv(output_file, index=False)
            print(f"Converted JSON: {file_name} -> {output_file}")

# Specify the input and output folder paths
input_folder = 'Cluster-Data-Input'  # Replace with the folder containing .parquet and .json.gz files
output_folder = 'Cluster-Data-Output'  # Replace with the desired output folder path

# Call the conversion function
convert_files_to_csv(input_folder, output_folder)

print(f"All files from {input_folder} have been converted to CSV in {output_folder}")

# this code requires more ram and processing power to run