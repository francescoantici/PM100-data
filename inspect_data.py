import seaborn as sns 
import matplotlib.pyplot as plt
import pandas as pd 

def load_data(path:str) -> pd.DataFrame:
    """_summary_

    Args:
        path (str): _description_

    Returns:
        pd.DataFrame: _description_
    """
    return pd.read_parquet(path)

if __name__ == "__main__":
    
    sns.set_theme(style="whitegrid")
    
    # Path to the PM100 data
    DATA_PATH = "job_table.parquet"
    
    # Load the dataframe
    df = load_data(DATA_PATH)
    
    # Exit state pie plot
    df = df.replace("OUT_OF_MEMORY", "OOM+NODE FAIL")
    df = df.replace("NODE_FAIL", "OOM+NODE FAIL")
    # Count the values by state
    data = df.job_state.value_counts()
    plt.pie(data.values, labels = data.index.values, colors=sns.color_palette("colorblind"), explode=[0.03]*(len(data)), autopct='%1.0f%%', )
    plt.savefig("plots/state_pie.png")
    plt.clf()
    
    # Plot the duration of the jobs divided by exit state
    # Convert runtime to minutes
    df.run_time = df.run_time.apply(lambda rt: round(int(rt/60), -2)).values
    
    # Plot the histogram
    sns.histplot(df, x = "run_time", hue = "job_state", multiple="dodge", kde = False, log_scale=False, palette=sns.color_palette("colorblind"), hue_order=["COMPLETED", "FAILED", "CANCELLED", "TIMEOUT", "OOM+NODE FAIL"])
    plt.xlabel("Duration (in minutes)")
    plt.ylabel("Number of jobs")
    plt.yscale("log")
    plt.tight_layout()
    plt.savefig("plots/run_time_state.png")
    plt.clf()
    
    # Plot the distribution of the number of GPU allocated to the jobs
    sns.histplot(df, x = "num_gpus_alloc")
    plt.xlabel("Number of GPUs allocated to the job")
    plt.ylabel("Number of jobs")
    plt.yscale("log")
    plt.xscale("log")
    plt.tight_layout()
    plt.savefig("plots/gpus_alloc.png")
    plt.clf()

    # Plot the distribution of the number of cores allocated to the jobs
    sns.histplot(df, x = "num_cores_alloc")
    plt.xlabel("Number of cores allocated to the job")
    plt.ylabel("Number of jobs")
    plt.yscale("log")
    plt.xscale("log")
    plt.tight_layout()
    plt.savefig("plots/cores_alloc.png")
    plt.clf()
    
    # Plot the distribution of the number of nodes allocated to the jobs
    sns.histplot(df, x = "num_nodes_alloc")
    plt.xlabel("Number of nodes allocated to the job")
    plt.ylabel("Number of jobs")
    plt.yscale("log")
    plt.xscale("log")
    plt.tight_layout()
    plt.savefig("plots/nodes_alloc.png")
    plt.clf()

    # Plot the distribution of the amount of memory allocated to the jobs
    sns.histplot(df, x = "mem_alloc")
    plt.xlabel("Amount of memory allocated to the job (in gigabytes)")
    plt.ylabel("Number of jobs")
    plt.yscale("log")
    plt.xscale("log")
    plt.tight_layout()
    plt.savefig("plots/mem_alloc.png")
    plt.clf()
    
    # Plot the distribution of jobs throughout the days
    df["day"] = df.submit_time.apply(lambda t: str(t)[5:10])

    days = df.day.unique()

    days.sort()

    sns.histplot(df, x = "day", kde=True)
    plt.xlabel("Day")
    plt.ylabel("Number of jobs")

    # Plot the ticks to improve readability
    xticks = []
    for day in  plt.gca().get_xticks():
        ym = str(days[day])[:3] + "2020"
        
        if ym in xticks:
            xticks.append("")
        else:
            xticks.append(ym)
            
    plt.xticks(ticks = plt.gca().get_xticks(), labels = xticks)
    plt.savefig("plots/day_dist.png")
    plt.clf()
    
    # Plot several jobs power consumption
    sample = df[df.job_id.isin([3848449, 5165227, 2448430, 2652511, 8296, 5029954, 838942])].sort_values("num_nodes_alloc")

    for i in range(len(sample)):
        
        y = sample.iloc[i].power_consumption
        
        xrange = [j*20 for j in range(len(y))]
        
        if i < 3:
            style = "--"
        else:
            style = "-"
        plt.plot(xrange, y, style)

    plt.xlabel("Seconds")
    plt.legend([f"Job {i+1}" for i in range(len(sample))])
    plt.ylabel("Power consumption (W)")
    plt.tight_layout()
    plt.savefig(f"plots/power_samples.png")
    plt.clf()
    
    # Plot the power consumption values of the jobs with and without the use of the GPU
    df["use_gpu"] = df.num_gpus_alloc.apply(lambda g: g > 0)
        
    power_df = {"power":[], "use_gpu":[], "nodes_allocated":[]}

    for pc in df[["power_consumption", "use_gpu", "num_nodes_alloc"]].values:
                
        power_df["power"] += list(pc[0])
        power_df["use_gpu"] += [pc[1]]*len(pc[0])
        power_df["nodes_allocated"] += [pc[-1]]*len(pc[0])
    
    power_df = pd.DataFrame.from_dict(power_df)

    sns.boxplot(power_df[power_df["nodes_allocated"] == 1], x = "use_gpu", y = "power")
    plt.xlabel("Jobs using")
    plt.xticks([0, 1], ["Cores", "Cores + GPUs"])
    plt.ylabel("Power consumption per job (W)")
    plt.savefig("plots/power_consumption_cpu_gpu_box.png")
    plt.clf()
    
    sns.histplot(power_df, x = "power", hue = "use_gpu")
    plt.xlabel("Power consumption (W)")
    plt.legend(["Cores+GPUs", "Cores"])
    plt.ylabel("Number of values")
    plt.yscale("log")
    plt.xscale("log")
    plt.xticks(plt.gca().get_xticks())
    plt.tight_layout()
    plt.savefig("plots/power_consumption_cpu_gpu_hist.png")
    plt.clf()
