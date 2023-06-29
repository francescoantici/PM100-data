# PM100-data

This is the official repository for the PM100 HPC job workload dataset, containing the scripts for the creation of the final data. The official dataset can be found in [zenodo](link a zenodo).

## Repository structure

- `extract_data.py`: The script containing the functions used to extract the final job power consumption from job traces and power logs. It is showed also an example of a pipeline to extract such values from data structured like the one in [M100 dataset](https://doi.org/10.5281/zenodo.7588814).
- `documentation`: The folder contains some documentation of the final dataset, like the job features description.

## Requirements

All the requirements to run the script are reported in the `requirements.txt` file.
