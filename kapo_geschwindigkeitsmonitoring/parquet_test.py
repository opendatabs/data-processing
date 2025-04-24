import pandas as pd
import pyarrow.parquet as pq

# Specify the file path
file_path = 'data/100097.parquet'

# Read the Parquet file
df = pd.read_parquet(file_path)

# Display the contents of the Parquet file
print("Data Contents:")
print(df)
# Print every first entry of the columns
print("First entry of each column:")
for column in df.columns:
    print(f"{column}: {df[column][0]}")
quit()

# Load the file using pyarrow to access metadata
parquet_file = pq.ParquetFile(file_path)

# Print the metadata
print("\nMetadata:")
print(parquet_file.metadata)

# Loop through each row group and print more details
for i in range(parquet_file.metadata.num_row_groups):
    row_group = parquet_file.metadata.row_group(i)
    print(f"\nRow Group {i + 1}:")
    print(f"  Number of Rows: {row_group.num_rows}")
    print(f"  Total Byte Size: {row_group.total_byte_size}")

# Optionally, print more detailed information about the schema
print("\nSchema:")
print(parquet_file.schema)
