import pandas as pd
import os


def split_csv(file_path, chunk_size, output_folder):
    # Read the CSV file in chunks
    chunk_iter = pd.read_csv(file_path, chunksize=chunk_size)
    
    # Iterate over the chunks and save each as a separate file
    for i, chunk in enumerate(chunk_iter):
        chunk.to_csv(f"{output_folder}/chunk_{i}.csv", index=False)



def merge_csv(output_folder, output_file):
    # List all chunk files in the output folder
    chunk_files = [f for f in os.listdir(output_folder) if f.startswith('chunk_') and f.endswith('.csv')]
    
    # Sort files by the order they were created
    chunk_files.sort(key=lambda x: int(x.split('_')[1].split('.')[0]))
    
    # Read the first chunk to get the header
    combined_df = pd.read_csv(os.path.join(output_folder, chunk_files[0]))
    
    # Append the rest of the chunks
    for chunk_file in chunk_files[1:]:
        chunk_df = pd.read_csv(os.path.join(output_folder, chunk_file))
        combined_df = pd.concat([combined_df, chunk_df], ignore_index=True)
    
    # Save the combined DataFrame to the output file
    combined_df.to_csv(output_file, index=False)
