# Post Process

This directory contains scripts for post-processing a sample and analyzing the output.

---
## perf.py
This script generates a plot containing the error of each workload feature per iteration of the post-processing algorithm as a subplot.

### Usage
python3 perf.py 

---
## rd_trace.py
This script creates an RD trace and a RD histogram file from a given sample.

### Usage
python3 hit_rate_err.py -w w97 -r 0.1 -b 4 -a 4 -tr 0.09

---
## plot_all_hr_err.py
This script identifies post-processing output for which hit rate error has not been computed and does so.

### Usage 
 python3 cum_err.py -r 0.1 -tr 0.09