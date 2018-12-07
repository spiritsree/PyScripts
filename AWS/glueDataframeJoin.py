'''
Purpose:
  AWS Glue Script to Join, two dataframes (without any related columns in the dataframes). df1 + df2

Use case:
  Usually for DMS output initial load files will not have 'Op' column compared to the CDC files.
  This will add that column to LOAD*.csv files.

Requirements:
  Need to pass 'source_s3_bucket' as an argument to the Glue job
  --source_s3_bucket "<bucket-name>"
'''

import sys
import os
import re
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from pyspark.sql import SparkSession
import boto3
from botocore.exceptions import ClientError

# Getting arguments from glue job parameters
## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv,
                         ['JOB_NAME',
                          'source_s3_bucket'])

def add_column_index(df):
  '''
  This function is to add an temporary index column to a dataframe
  for merging 2 dataframe which does not have any relation.

  add_column_index(df)
  '''
  rowcount = df.count()
  oldColumns = df.columns
  newColumns = oldColumns + ["columnindex"]
  # If schema inference is needed, samplingRatio is used to determined the ratio of rows used for schema inference. By default it is limited to 100 rows
  if rowcount > 40:
    df_indexed = df.rdd.zipWithIndex().map(lambda (row, columnindex): row + (columnindex,)).toDF(sampleRatio=0.2)
  else:
    df_indexed = df.rdd.zipWithIndex().map(lambda (row, columnindex): row + (columnindex,)).toDF()
  oldColumns = df_indexed.columns
  new_df = reduce(lambda data, idx:data.withColumnRenamed(oldColumns[idx], newColumns[idx]), xrange(len(oldColumns)), df_indexed)
  return new_df

def main():
  '''
  Main function which does the inserting of 'Op' column to
  DMS initial load file '.*?/LOAD[0-9]+\.csv'
  '''
  sc = SparkContext()
  glueContext = GlueContext(sc)
  spark = glueContext.spark_session
  job = Job(glueContext)
  job.init(args['JOB_NAME'], args)

  source_bucket = args['source_s3_bucket']

  client = boto3.client('s3')
  paginator = client.get_paginator('list_objects')
  resultPages = paginator.paginate(Bucket=source_bucket)
  for page in resultPages:
    for s3obj in page['Contents']:
      temp_key = s3obj['Key']
      if re.match(r'.*?/LOAD[0-9]+\.csv', temp_key):
        s3_source_path = "s3a://" + source_bucket + "/" + temp_key
        s3_dest_dir = os.path.dirname(s3_source_path)
        csvDf = spark.read.csv(s3_source_path, header=True, quote='"', escape='"')
        rcount = csvDf.count()
        op_data = [('I',),] * rcount
        # Creating a dataframe with one column 'Op' with literal 'I'
        opDf = spark.createDataFrame(op_data, ["Op"])
        csvDfWithIndex = add_column_index(csvDf)
        opDfWithIndex = add_column_index(opDf)

        # Joining the above created 'Op' dataframe with the actual dataframe.
        # This is to make csv result files of DMS same structure since the
        # initial load file will not have 'Op' column.
        # also dropping the index column which is added with add_column_index function.
        csvDf1 = opDfWithIndex.join(csvDfWithIndex, csvDfWithIndex.columnindex == opDfWithIndex.columnindex,'inner').drop("columnindex")
        csvDf2 = csvDf1.repartition(1)
        csvDyF = DynamicFrame.fromDF(csvDf2, glueContext, "csvDyF")
        csvDyF1 = glueContext.write_dynamic_frame.from_options(frame = csvDyF, connection_type = "s3", connection_options = {"path": s3_dest_dir}, format = "csv", transformation_ctx = "csvDyF1")
        # Below cannot be used since spark-csv write does not support null values in dataframe
        # csvDf1.write.csv(s3_dest_dir, mode='append', header=True, nullValue='NA', quoteAll='None')
        client.delete_object(Bucket=source_bucket, Key=temp_key)

  job.commit()

if __name__ == '__main__':
    main()
