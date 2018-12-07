#!/usr/bin/env python3

'''
Sample spark dataframe operations.

run:
    spark-submit pySparkDataframe_sample.py
    OR
    python3 pySparkDataframe_sample.py
'''
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.context import SparkContext

def add_column_index(df):
  '''
  This function is to add an temporary index column to a dataframe
  for merging 2 dataframe which does not have any relation.

  add_column_index(df)
  '''
  rowcount = df.count()
  new_schema = StructType(df.schema.fields + [StructField("idx", LongType(), False),])
  # If schema inference is needed, samplingRatio is used to determined the ratio of rows used for schema inference. By default it is limited to 100 rows
  if rowcount > 40:
    df_idx = df.rdd.zipWithIndex().map(lambda row: row[0] + (row[1],)).toDF(schema=new_schema,sampleRatio=0.2)
  else:
    df_idx = df.rdd.zipWithIndex().map(lambda row: row[0] + (row[1],)).toDF(schema=new_schema)
  return df_idx


def main():
    '''
    some standard spark functions examples
    '''

    # Starting a spark session
    sc = SparkContext()
    sc.setLogLevel("OFF")
    spark = SparkSession.builder.master("local").getOrCreate()

    # Creating a dataframe from data
    l = [(1, 'a', 'b', 'c', 'd'), (1, 'a', 'b', 'c', 'd')]
    df0 = spark.createDataFrame(l, ['col1', 'col2', 'col3', 'col4', 'col5'])

    # Creating a dataframe using rdd
    l = [(2, 'f', 'g'), (2, 'f', 'g')]
    rdd = sc.parallelize(l)
    schema = StructType([
         StructField("col6", IntegerType(), True),
         StructField("col7", StringType(), True),
         StructField("col8", StringType(), True)])
    df1 = spark.createDataFrame(rdd, schema)

    # Joining both df0 and df1
    indexedDf0 = add_column_index(df0)
    indexedDf1 = add_column_index(df1)
    df2 = indexedDf0.join(indexedDf1, indexedDf1.idx == indexedDf0.idx,'inner').drop("idx")
    df2.write.csv("/tmp/file.csv", mode='overwrite', header=True, nullValue='NA', quoteAll=False)

    # Read a CSV file into a dataframe
    df = spark.read.csv("/tmp/file.csv", header=True, quote='"', escape='"')

    # Print the Schema
    df.printSchema()

    # Count the number of rows
    print('Number of rows: {}'.format(df.count()))

    # Show columns
    print('Columns:  {}'.format(df.columns))

    # Display the data
    df.show()


if __name__ == '__main__':
    main()
