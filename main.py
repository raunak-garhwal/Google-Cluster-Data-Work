import os
import pandas as pd
import gzip

def convert_files_to_csv(input_folder, output_folder):
    # Create the output folder if it doesn't exist
    os.makedirs(output_folder, exist_ok=True)
    
    # Iterate over all files in the input folder
    for file_name in os.listdir(input_folder):
        input_file = os.path.join(input_folder, file_name)
        
        try:
            # Check file extension and process accordingly
            if file_name.endswith(".parquet") or file_name.endswith(".parquet.gz"):
                # Construct output file path
                output_file = os.path.join(output_folder, file_name.replace(".parquet", ".csv").replace(".gz", ""))
                
                if file_name.endswith(".parquet.gz"):
                    # Decompress the .parquet.gz file
                    decompressed_file = input_file.replace(".gz", "")
                    with gzip.open(input_file, 'rb') as gz_file:
                        with open(decompressed_file, 'wb') as temp_file:
                            temp_file.write(gz_file.read())
                    input_file = decompressed_file  # Use the decompressed file path
                
                # Read the Parquet file
                df = pd.read_parquet(input_file, engine='pyarrow')  # Use engine='pyarrow' or 'fastparquet'
                df.to_csv(output_file, index=False)
                print(f"Converted Parquet: {file_name} -> {output_file}")
                
                # Clean up decompressed file if needed
                if file_name.endswith(".parquet.gz"):
                    os.remove(decompressed_file)
            
            elif file_name.endswith(".json.gz"):
                # Construct output file path
                output_file = os.path.join(output_folder, file_name.replace(".json.gz", ".csv"))
                
                # Read the JSON file
                df = pd.read_json(input_file, compression='gzip', lines=True)  # Use 'lines=True' for line-delimited JSON
                df.to_csv(output_file, index=False)
                print(f"Converted JSON: {file_name} -> {output_file}")
            
            else:
                print(f"Unsupported file format: {file_name}")

        except Exception as e:
            print(f"Error processing {file_name}: {e}")

# Specify the input and output folder paths
input_folder = 'Cluster-Data-Input'  # Replace with the folder containing .parquet and .json.gz files
output_folder = 'Cluster-Data-Output'  # Replace with the desired output folder path

# Call the conversion function
convert_files_to_csv(input_folder, output_folder)

print(f"All files from {input_folder} have been converted to CSV in {output_folder}")

# not working properly