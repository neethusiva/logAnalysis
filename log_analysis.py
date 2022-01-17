#!/usr/bin/python
##########################################################################################################################################################
"""
Purpose               : This script load raw data.
Change History Record :
======================================================================================================================================================
Version | DATE         | Created             |  Modifier         |   Change made                
------------------------------------------------------------------------------------------------------------------------------------------------------
1.0     | 2022-01-15   | Neethu Sivaraj Adiyoli      |                   | New scripts    
======================================================================================================================================================
"""
##########################################################################################################################################################
from datetime import datetime
import os
import sys
import traceback
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.types import StructType,StructField
from pyspark.sql.functions import col,sum,avg,max
from os.path import abspath

##########################################################################################################################################################
"""
Description     : This script load raw data.
Input Parameters: <script path> <Config>
Return Type     : None
"""
##########################################################################################################################################################
warehouse_location = abspath('spark-warehouse')

spark = SparkSession.builder.appName("Covid Data Analysis")\
        .config("spark.sql.warehouse.dir", warehouse_location).enableHiveSupport().getOrCreate()

spark.sparkContext.setLogLevel('ERROR')

def main(argument_list):
    #------------------------------------Reading the config file ---------------------------------------#
    print("\nconfig\n")

    inputfile_name="bigeventloggb.csv"
    hdfs_path_dir="/user/loganalysis"
    db_name="log_analysis_db"
    table_name="log_summary_final"

    df_schema = StructType([ \
          StructField("slid",StringType(),True), \
          StructField("machine_name",StringType(),True), \
          StructField("category",StringType(),True), \
          StructField("entrytype", StringType(), True), \
          StructField("message", StringType(), True), \
          StructField("source", StringType(), True), \
          StructField("generatedtime", StringType(), True), \
          StructField("country", StringType(), True), \
          StructField("region", StringType(), True), \
          StructField("city", StringType(), True), \
          StructField("zip", StringType(), True), \
          StructField("timezone", StringType(), True), \
          StructField("isp", StringType(), True) \
      ])
   
    
    try:
        full_log_df = spark.read.csv(hdfs_path_dir+"/source/bigeventloggb*.csv",schema=df_schema,header=True,inferSchema= True)

    
        category_df = full_log_df.select("machine_name","entrytype","city","generatedtime")

        df_name="category_df_tab"
        category_df.createOrReplaceTempView(df_name)


        summary_df_month_year = spark.sql("select entrytype,city,from_unixtime(unix_timestamp(generatedtime ,'yyyy-MM-dd HH:MM:SS'),'M') as month_num,from_unixtime(unix_timestamp(generatedtime ,'yyyy-MM-dd HH:MM:SS'),'MMM') as month_col,from_unixtime(unix_timestamp(generatedtime ,'yyyy-MM-dd HH:MM:SS'),'YYYY') as year_col from {0}".format(df_name))

        df_name="summary_df_month_year_tab"
        summary_df_month_year.createOrReplaceTempView(df_name)
        
        #monthly_log_data = summary_df_month_year.groupBy("month_num","month_col","year_col","city","entrytype").count().orderBy(col("year_col").desc(),col("month_num").cast(IntegerType()).asc())

        monthly_log_data = spark.sql("select month_num,month_col,year_col,city,entrytype,count(1) as count_col from {0} group by month_num,month_col,year_col,city,entrytype order by cast(month_num as int) asc,year_col desc".format(df_name))
       
        malformed_logs_df = monthly_log_data.filter((monthly_log_data.entrytype.isin('Information','Error','Warning') == False))


        malformed_logs_df.repartition(1).write.format("csv").mode("overwrite").save(hdfs_path_dir + "/malformed_logs")

        monthly_log_clean_data = monthly_log_data.select("month_col","year_col","city","entrytype","count_col").filter((monthly_log_data.month_num != '') & (monthly_log_data.entrytype.isin('Information','Error','Warning')))

        monthly_log_clean_data.repartition(1).write.format("csv").mode("overwrite").save(hdfs_path_dir + "/monthly_log_data")

    
    except Exception:
        traceback.print_exc()
        sys.exit("\nError while running query")
    else:
        print("\nData has been loaded into the spark")


############################################################################################
# GLOBAL SECTION
############################################################################################

# Trigger the module
if __name__ == "__main__":
    print('\nExecution started.')
    start_time=datetime.now()
    print('\nStart time is {0}'.format(start_time))
    main(sys.argv)
    end_time=datetime.now()
    print("\nExecution over.End time is {0}".format(end_time))
