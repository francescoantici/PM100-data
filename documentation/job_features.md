# Job features description 

|Column|Description|Type|
|------|-----------|----|
|cores_alloc_layout|Map: list of cores allocated per node|string|
|cores_allocated|Map: number of cores allocated per node|string|
|cores_per_task|Number of cores required for each task|int|
|derived_ec|Highest exit code of all job steps|string|
|eligible_time|Time job is eligible for running|timestamp|
|end_time|Time of termination|timestamp|
|group_id|Group job submitted as|int|
|job_id|Job ID (anonymized)|int|
|job_state|State of the job, see enum job_states for possible values|string|
|nodes|List of nodes allocated to job|string|
|num_cores_req|Number of cores requested by the user|int|
|num_cores_alloc|Number of cores allocated to the job|int|
|num_gpus_req|Number of GPUs requested by the user|int|
|num_gpus_alloc|Number of GPUs allocated to the job|int|
|num_nodes_req|Number of nodes requested by the user|int|
|num_nodes_alloc|Number of nodes allocated to the job|int|
|mem_req|Amount of memory (RAM) requested by the user|int|
|mem_alloc|Amount of memory (RAM) allocated to the job|int|
|num_tasks|Number of tasks requested by a job or job step|int|
|partition|Name of assigned partition (anonymized)|string|
|power_flags|Power management flags, see SLURM_POWERFLAGS|int|
|priority|Relative priority of the job, 0=held, 1=required nodes DOWN/DRAINED|int|
|qos|Quality of Service (anonymized, categorical)|string|
|req_nodes|Comma-separated list of required nodes|string|
|run_time|Job run time (seconds)|int|
|shared|1 if job can share nodes with other jobs, 0 otherwise|string|
|start_time|Time execution begins (actual or expected)|timestamp|
|state_reason|Reason job still pending or failed,see slurm.h:enum job_state_reason|string|
|submit_time|Time of job submission|timestamp|
|threads_per_core|Threads per core required by job|int|
|time_limit|Maximum run time in minutes or INFINITE|int|
|user_id|User ID for a job or job step (anonymized)|int|
|power_consumption|Power consumption of the job|List[int]|
