# Log Data Analysis

## Purpose:
Find a use case and generate/download sample data. Create a Spark long running non-optimized job. And then optimize the job using possible Spark configurations to reduce 50% of its execution time. For example if a job takes 2Hours to run, optimize/fine tune the job using Spark configurations to reduce the execution time(eg: 1 hour).

* Input: CSV files with log information from different systems.
* Process: 
  * Segregate required columns
  * Extract month and year from timestamp
  * Pull a summary based on month, year, city and log type
  * Write summary to csv file
  * Find malformed records from log file and move it to another file for next analysis
* Output: CSV files with summary information and malformed records.

## Job Information

* Added more transformations to increase time.

* Used more shuffling transformations to increase time like repartition instead of coalesce.

* Added cases where sort aggregation will be used instead of hash aggregation.

## Job Execution Details Before Optimization:

### Execution Command:

nohup spark-submit --name log_analysis_test --master yarn --deploy-mode cluster --conf spark.executor.memory=1g  --conf spark.executor.cores=1 --conf spark.executor.instances=1 --conf spark.dynamicAllocation.enabled=false /home/hadoop/loganalysis/log_analysis_v1.py > /home/hadoop/loganalysis/log_analysis_v1.log &

### Execution Time:
Start time : 2022-01-17 08:45:26.222936
End time : 2022-01-17 09:48:40.914183

Total time: 1 Hour 3 Minutes 15 Sec

## Job Execution Details After Optimization:

nohup spark-submit --name log_analysis_test --master yarn --deploy-mode cluster --conf spark.executor.memory=2g  --conf spark.executor.cores=3 --conf spark.executor.instances=1 --conf spark.dynamicAllocation.enabled=false --conf spark.sql.adaptive.enabled=false /home/hadoop/loganalysis/log_analysis_v1.py > /home/hadoop/loganalysis/log_analysis_v1.log &

### Execution Time:
Start time : 2022-01-17 10:14:24.386305
End time : 2022-01-17 10:44:23.175671

Total time: 0 Hour 29 Minutes 59 Sec
