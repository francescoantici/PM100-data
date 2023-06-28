import os
from multiprocessing import Pool
from typing import Iterable, Literal
from parallel_pandas import ParallelPandas
import json
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
import pickle

def round_to_closest_second(timestamp: datetime, sampling_time:int = 20, mode: Literal["ceil", "floor"] = "ceil") -> datetime:
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
        
def create_node_hashmap(node:str, jobs:Iterable, job_start_field: str = "start_time", job_end_field: str = "end_time", job_id_field: str = "job_id", job_nodes_field: str = "nodes", sampling_time: int = 20,):
    """_summary_

    Args:
        nodes_names (Iterable): _description_
        jobs (Iterable): _description_
        results_path (str, optional): _description_. Defaults to None.
        job_start_field (str, optional): _description_. Defaults to "start_time".
        job_end_field (str, optional): _description_. Defaults to "end_time".
        job_id_field (str, optional): _description_. Defaults to "job_id".
        job_nodes_field (str, optional): _description_. Defaults to "nodes".
        sampling_time (int, optional): _description_. Defaults to 20.
        multiprocessing (bool, optional): _description_. Defaults to False.

    Returns:
        dict: _description_
    
    """
    hashmap = {}
    
    
    def _populate_hashmap(job, hashmap):
        if node not in job[job_nodes_field]:
            return
        for t in pd.date_range(round_to_closest_second(job[job_start_field], sampling_time = sampling_time, mode = "ceil"), round_to_closest_second(job[job_end_field], sampling_time = sampling_time, mode = "floor"), freq = f"{sampling_time}s").to_pydatetime():
            
            # Populates the hasmap with the id of the job 
            hashmap[str(t)] = hashmap.get(str(t), []) + [job[job_id_field]]
        
    
    jobs.apply(lambda j: _populate_hashmap(j, hashmap), axis = 1)
    
    return node, hashmap
                                
def get_non_exclusive_ids(nodes_global_hashmaps: list) -> Iterable:
    """_summary_

    Args:
        nodes_global_hashmap (dict): _description_

    Returns:
        Iterable: _description_
    """
    non_exclusive_set = set()
    for node_hashmap in nodes_global_hashmaps:
        for ts in node_hashmap.values():
            if len(ts) > 1:
                non_exclusive_set.update(list(ts))
    return non_exclusive_set

def extract_job_power(job_data: Iterable, ps0_table, ps1_table, save_path = None):
    try:
        ts = job_data["start_time"]
        te = job_data["end_time"]
        nodes = list(json.loads(
            job_data["cpus_alloc_layout"].replace("'", '"')).keys())
        ps0_nodes = ps0_table.loc[ps0_table["node"].isin(nodes)]
        ps1_nodes = ps1_table.loc[ps1_table["node"].isin(nodes)]
        ps0_power = ps0_nodes.loc[(ps0_nodes["timestamp"] >= ts) & (
            ps0_nodes["timestamp"] <= te)].groupby(["timestamp"]).sum()["value"].values
        ps1_power = ps1_nodes.loc[(ps1_nodes["timestamp"] >= ts) & (
            ps1_nodes["timestamp"] <= te)].groupby(["timestamp"]).sum()["value"].values
        power = ps0_power + ps1_power
        if len(power) == 0:
            raise Exception("No power data found.")
        if save_path:
            with open(os.path.join(save_path, f"{str(job_data['job_id'])}.pkl"),'wb') as f:
                pickle.dump(power, f)
        return power
    except Exception as e:
        print(e)
        return np.array([])


if __name__ == "__main__":
    
    n_threads = 1 
    
    #initialize parallel-pandas
    ParallelPandas.initialize(n_cpu=os.cpu_count(), split_factor=n_threads, disable_pr_bar=True)
        
    job_table_path = ""
    nodes_list_path = ""
    ps0_table_path = ""
    ps1_table_path = ""
    
    final_table_path = ""
        
    job_table = pd.read_parquet(job_table_path)
    node_list = [node.split("\n")[0] for node in open(nodes_list_path).readlines()]
    ps0_table = pd.read_parquet(ps0_table_path)
    ps1_table = pd.read_parquet(ps1_table_path)
    
    with Pool(n_threads) as p:
        nodes_hashmaps = p.map_async(create_node_hashmap, node_list, kwargs = {"jobs":job_table}).get()
        
    ids_to_exclude = get_non_exclusive_ids(nodes_global_hashmaps = nodes_hashmaps)
    
    job_table_exclusive = job_table[~job_table.job_id.isin(ids_to_exclude)]
    
    job_table_exclusive["p_con"] = job_table.p_apply(lambda j: extract_job_power(j, ps0_table = ps0_table, ps1_table = ps1_table), axis = 1)
    
    job_table_exclusive.to_parquet(final_table_path)
    
    
    