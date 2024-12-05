import pandas as pd
import numpy as np
from pathlib import Path

def merge_point_clouds(label_file_path, height_file_path, output_file_path, chunk_size=100000000000000000000000):
    """
    Merge two point cloud CSV files based on X,Y coordinates.
    
    Parameters:
    -----------
    label_file_path : str
        Path to the CSV file containing labels
    height_file_path : str
        Path to the CSV file containing height data
    output_file_path : str
        Path where the merged CSV will be saved
    chunk_size : int
        Number of rows to process at once (to manage memory usage)
        
    Returns:
    --------
    bool
        True if successful, False otherwise
    """
    try:
        # Verify input files exist
        if not Path(label_file_path).exists() or not Path(height_file_path).exists():
            raise FileNotFoundError("One or both input files not found")
            
        # Create output directory if it doesn't exist
        Path(output_file_path).parent.mkdir(parents=True, exist_ok=True)
        
        # Read the first row to verify columns
        label_sample = pd.read_csv(label_file_path, nrows=1)
        height_sample = pd.read_csv(height_file_path, nrows=1)
        
        # Verify X,Y columns exist in both files
        required_cols = ['x', 'y']
        if not all(col in label_sample.columns for col in required_cols) or \
           not all(col in height_sample.columns for col in required_cols):
            raise ValueError("Both files must contain 'X' and 'Y' columns")
        
        # Initialize chunk processing
        header_written = False
        
        # Process the label file in chunks
        for label_chunk in pd.read_csv(label_file_path, chunksize=chunk_size):
            # Round coordinates for label chunk
            decimals = 6  # Adjust based on your data precision needs
            label_chunk[['x', 'y']] = label_chunk[['x', 'y']].round(decimals)
            
            # Read and process the height file for each label chunk
            height_data = pd.read_csv(height_file_path)
            height_data[['x', 'y']] = height_data[['x', 'y']].round(decimals)
            
            # Merge chunks based on X,Y coordinates
            merged_chunk = pd.merge(
                label_chunk,
                height_data,
                on=['x', 'y'],
                how='inner',
                suffixes=('_label', '_height')
            )
            
            # Write to output file
            if not header_written:
                merged_chunk.to_csv(output_file_path, index=False, mode='w')
                header_written = True
            else:
                merged_chunk.to_csv(output_file_path, index=False, mode='a', header=False)
            
            # Clear memory
            del height_data
            del merged_chunk
            
        return True
        
    except Exception as e:
        print(f"Error processing files: {str(e)}")
        return False

# Example usage
if __name__ == "__main__":
    # Example file paths
    label_file = "./12000-8000-20.csv"
    height_file = "./merged_output.csv"
    output_file = "./merged_output_RGB.csv"
    
    success = merge_point_clouds(label_file, height_file, output_file)
    if success:
        print("Files merged successfully!")
    else:
        print("Failed to merge files. Check the error message above.")