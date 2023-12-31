import pandas as pd 
import os 

def merge_job_tables(data_path:str) -> pd.DataFrame:
    """
    Merge the job tables of each month into a single one

    Args:
        data_path (str): Path to the parquet files containing the job table files.

    Returns:
        pd.DataFrame: The merged job table.
    """
    
    table_path_list = [os.path.join(data_path, f) for f in os.listdir(data_path) if os.path.isfile(os.path.join(data_path, f)) if ".parquet" in f]
    
    job_table = pd.read_parquet(table_path_list[0])
    
    for t_p in table_path_list[1:]:
        
        job_table = job_table.append(pd.read_parquet(t_p))
    
    return job_table

if __name__ == "__main__":
    
    # Path to the parquet files containing the job table files
    job_table_data_path = ""
    
    # Path to the merged job table
    job_table_merged_path = ""

    merge_job_tables(job_table_data_path).to_parquet(job_table_merged_path)
    
    