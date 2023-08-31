# PM100 Dataset

This is the official repository for the PM100 HPC job workload dataset, containing the scripts for the creation of the final data. The official dataset can be found in [![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.8129258.svg)](https://doi.org/10.5281/zenodo.8129258).

## Repository structure

- `extract_data.py`: The script containing the functions used to extract the final job power consumption from job traces and power logs. It is showed also an example of a pipeline to extract such values from data structured like the one in [M100 dataset](https://doi.org/10.5281/zenodo.7588814).
- `inspect_data.py`: The script reports the operation performed on the final PM100 data to produce the plots in the `plots` folder. Moreover, it reports a function to load the data and provide examples on how to inspect it.
- `documentation`: The folder contains some documentation of the final dataset, like the job features description.
- `plots` : The folder contains the plots presented in the paper.

## Preliminaries

All the packages used in the project are reported in the `requirements.txt`, the `Python` version used was the `3.6.8`. 

It is a good practice to create a virtual environment and then install the required packages with `pip3 install -r requirements.txt`. 

In order to extract the PM100 dataset from M100, first it is needed to download the correct data from [Zenodo](https://doi.org/10.5281/zenodo.7588814).

After downloading the data relative to the whole period (YY-MM from 20-05 to 20-10) or just a subset of it, the archives must be extracted and the tables which are needed for the scripts are: 

- `year_month=YY-MM/plugin=job_table/metric=job_info_marconi100/a_0.parquet` : Parquet file containing the job data;
- `year_month=YY-MM/plugin=ipmi_pub/metric=ps0_input_power/a_0.parquet` : Parquet file containing the first power socket metrics;
- `year_month=YY-MM/plugin=ipmi_pub/metric=ps1_input_power/a_0.parquet` : Parquet file containing the second power socket metrics.

The job tables related to the different months can be merged by running the `merge_m100_tables.py`, expliciting the path to the downloaded data in the `job_table_data_path` variable.

## Launch the extraction 

Before launching the extraction, the variables `job_table_path`, `ps[0, 1]_table_path` and `final_table_path` must be initialized with the path to the downloaded data and the desidered output file for the dataset.

The result of the execution is a `parquet` file containing the data structured as presented in the `documentation/job_features.md` file.


