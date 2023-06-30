import os
from multiprocessing import Pool
from typing import Iterable, Literal
from parallel_pandas import ParallelPandas
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
import pickle

def round_to_closest_second(timestamp: datetime, sampling_time: int = 20, mode: Literal["ceil", "floor"] = "ceil") -> datetime:
    """
    Util function to parse any timestamp to the closest fixed time frame.

    Args:
        timestamp (datetime): The timestamp to parse.
        sampling_time (int, optional): The second sampling to divide the minutes. Defaults to 20.
        mode (Literal[&quot;ceil&quot;, &quot;floor&quot;], optional): How to round the timestamp, ceil to perform ceil rounding, floor to perform floor rounding. Defaults to "ceil".

    Returns:
        datetime: The parsed timestamp
    """
    
    # Check if the timestamp is already parsed 
    if (timestamp.second % sampling_time) == 0:
        return timestamp
    
    # Creates the boundaries for the interval
    for i in range(int(60/sampling_time)):
        values = list(range(i*sampling_time, (i + 1)*sampling_time))
        s = timestamp.second
        
        # Check if it is the right interval 
        if s in values:
            
            # Parse the timestamp accordingly to the rounding technique
            if mode == "ceil":
                return timestamp + timedelta(seconds = values[-1] - s + 1)
            else: 
                return timestamp - timedelta(seconds = s - values[0])
        
def create_node_hashmap(node: str, jobs: pd.DataFrame, job_start_field: str = "start_time", job_end_field: str = "end_time", job_id_field: str = "job_id", job_nodes_field: str = "nodes", sampling_time: int = 20) -> dict:
    """
    Creates the occupancy hashmap for a given node.

    Args:
        node (str): The node name.
        jobs (pd.DataFrame): The job table.
        job_start_field (str, optional): The field name for the start time of the job in the job table. Defaults to "start_time".
        job_end_field (str, optional): The field name for the end time of the job in the job table. Defaults to "end_time".
        job_id_field (str, optional): The field name for the id of the job in the job table. Defaults to "job_id".
        job_nodes_field (str, optional): The field name for the nodes allocated to the job in the job table. Defaults to "nodes".
        sampling_time (int, optional): The sampling time to use to verify concurrency of jobs. Defaults to 20.

    Returns:
        dict: The node hashmap with the job ids running in all the timestamps.
    
    """
    node_jobs = jobs.loc[jobs[job_nodes_field].apply(lambda ns: node in ns)]
        
    hashmap = {}
    
    for job in node_jobs[[job_id_field, job_start_field, job_end_field]].values:
    
        for t in pd.date_range(round_to_closest_second(job[1], sampling_time = sampling_time, mode = "ceil"), round_to_closest_second(job[-1], sampling_time = sampling_time, mode = "floor"), freq = f"{sampling_time}s").to_pydatetime():
                
            # Populates the hasmap with the id of the job 
            hashmap[str(t)] = hashmap.get(str(t), []) + [job[0]]
    
    return hashmap
                                
def get_non_exclusive_ids(nodes_global_hashmaps: list) -> Iterable:
    """
    Returns the list of ids of non exclusive jobs.

    Args:
        nodes_global_hashmap (dict): The hashmaps of all the nodes to check.

    Returns:
        Iterable: List of the ids of the job which are concurrent.
    """
    non_exclusive_set = set()
    for node_hashmap in nodes_global_hashmaps.values():
        for ts in node_hashmap.values():
            if len(ts) > 1:
                non_exclusive_set.update(list(ts))
    return non_exclusive_set

def extract_job_power(job_data: Iterable, p_table: pd.DataFrame, job_start_field: str = "start_time", job_end_field: str = "end_time", job_id_field: str = "job_id", job_nodes_field: str = "nodes", save_path:str = None) -> np.array:
    """
    Extract the job power consumption from the power table and job data.

    Args:
        job_data (Iterable): The job data.
        ps0_table (pd.DataFrame): The p table containing the power values.
        job_start_field (str, optional): The field name for the start time of the job in the job table. Defaults to "start_time".
        job_end_field (str, optional): The field name for the end time of the job in the job table. Defaults to "end_time".
        job_id_field (str, optional): The field name for the id of the job in the job table. Defaults to "job_id".
        job_nodes_field (str, optional): The field name for the nodes allocated to the job in the job table. Defaults to "nodes".
        save_path (str, optional): The path to save power consumption of jobs, if None no file is saved. Defaults to None.

    Raises:
        Exception: If there is any problem in extracting the power values.

    Returns:
        np.array: The power consumption of the input job, empty array if there are errors in the computation.
    """
    try:
        nodes = job_data[job_nodes_field]
        p_nodes = p_table.loc[p_table["node"].isin(nodes)]
        power = p_nodes.loc[(p_nodes["timestamp"] >= job_data[job_start_field]) & (p_nodes["timestamp"] <= job_data[job_end_field])].\
                groupby(["timestamp"]).\
                    sum()["value"].\
                        values
        if len(power) == 0:
            raise Exception("No power data found.")
        if save_path:
            with open(os.path.join(save_path, f"{str(job_data[job_id_field])}.pkl"),'wb') as f:
                pickle.dump(power, f)
        return power
    except Exception as e:
        print(e)
        return np.array([])


if __name__ == "__main__":
    
    # Example of an extraction pipeline
    
    # Define the number of threads to use for the computation
    n_threads = 1 
    
    # Initialize parallel-pandas, comment if not needed
    ParallelPandas.initialize(n_cpu=os.cpu_count(), split_factor=n_threads, disable_pr_bar=True)
    
    # The path to the job table file
    job_table_path = ""
    
    # The path to the files containing the power values
    p_table_path = ""
    
    # The final path to the output job table
    final_table_path = ""
    
    # Loading of the files
    job_table = pd.read_parquet(job_table_path)
    p_table = pd.read_parquet(p_table_path)
    
    nodes = set()
    
    # Create the list of nodes
    for na in job_table["nodes"].values:
        
        nodes.update(na)
            
    # Create the nodes hashmaps, if n_threads > 1 multiprocessing, serial otherwise
    with Pool(n_threads) as p:
        hashmaps_list = p.starmap_async(create_node_hashmap, [(node, job_table) for node in nodes]).get()
    
    # Compute the ids of job which are not running exclusively on the nodes
    ids_to_exclude = get_non_exclusive_ids(nodes_global_hashmaps = hashmaps_list)
    
    # Filter the exclusive jobs
    job_table_exclusive = job_table[~job_table.job_id.isin(ids_to_exclude)]
    
    # Extract each job power consumption, if not using parallelpandas replace p_apply with apply
    job_table_exclusive["power_consumption"] = job_table.p_apply(lambda j: extract_job_power(j, p_table = p_table), axis = 1)
    
    # Save the final job table to the specified file path
    job_table_exclusive.to_parquet(final_table_path)