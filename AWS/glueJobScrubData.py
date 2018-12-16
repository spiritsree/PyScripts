'''
Purpose:
    The purpose of this glue job script is to encrypt email from source csv and save
    to clean the data and optionally move to target buckets.
    The script will load data from glue data catalog, which will have the path to actual
    csv data.

Requirements:
    Need to pass 'RESULT_S3_BUCKET' to glue job as parameters.
    --RESULT_S3_BUCKET "<result-bucket-name>"

Optional:
    Optionally you can pass 'TARGET_DATA_COPY', 'TARGET_S3_CSV_BUCKET', 'TARGET_S3_PARQUET_BUCKET' to glue job
    --TARGET_DATA_COPY "Y"
    --TARGET_S3_CSV_BUCKET "<target-csv-bucket-name>"
    --TARGET_S3_PARQUET_BUCKET "<target-parquet-bucket-name>"
'''
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace, col, lit, udf
from pyspark.sql.types import StringType
import re
import boto3
from botocore.exceptions import ClientError

## @params: [JOB_NAME]
## reading arguments from job parameters
resolveList = ['JOB_NAME', 'RESULT_S3_BUCKET']
if ('--{}'.format('TARGET_DATA_COPY') in sys.argv):
    resolveList.append('TARGET_DATA_COPY')
    resolveList.append('TARGET_S3_CSV_BUCKET')
    resolveList.append('TARGET_S3_PARQUET_BUCKET')

args = getResolvedOptions(sys.argv, resolveList)

# Initialization
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

result_bucket = args['RESULT_S3_BUCKET']
if 'TARGET_DATA_COPY' in resolveList:
    target_copy_required = args['TARGET_DATA_COPY']
    target_bucket_csv = args['TARGET_S3_CSV_BUCKET']
    target_bucket_parquet = args['TARGET_S3_PARQUET_BUCKET']
else:
    target_copy_required = 'N'

database_name = 'db1'
result_s3_csv_path = 's3://{}/{}_csv'.format(result_bucket,database_name)
result_s3_parquet_path = 's3://{}/{}_parquet'.format(result_bucket,database_name)

# user defined function to hash the email
u_df0 = udf(lambda s: re.sub(r"(^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$)",  lambda m: "{0}".format(hash(m.group(1))), s), StringType())
u_df1 = udf(lambda x: "{0}".format(hash(x)), StringType())
u_df2 =  udf(lambda x: x.replace('\n', ' '), StringType())

# processing table1
datasource0 = glueContext.create_dynamic_frame.from_catalog(database = database_name, table_name = "table1", transformation_ctx = "datasource0")
applymapping0 = ApplyMapping.apply(frame = datasource0, mappings = [("col1", "string", "col1", "string"), ("col2", "long", "col2", "long"), ("col3", "string", "col3", "string"), ("col4", "string", "col4", "string"), ("col5", "long", "col5", "long")], transformation_ctx = "applymapping0")
# convert to a Spark dataframe...
df0 = applymapping0.toDF()
# processing the data
df0a = df0.withColumn("col0", lit(1)).select("col0", "col1", "col2", "col3", "col4","col5").withColumn("col3", u_df0("col3"))
# Remove newline char from col4
df0b = df0a.withColumn("col4", u_df2("col4"))
# Converting dataframe to dynamicframe
df0c = DynamicFrame.fromDF(df0b, glueContext, "df0c")
datasink0 = glueContext.write_dynamic_frame.from_options(frame = df0c, connection_type = "s3", connection_options = {"path": result_s3_csv_path + "/table1"}, format = "csv", format_options = {"writeHeader": "true"}, transformation_ctx = "datasink0")
resolvechoice0 = ResolveChoice.apply(frame = df0c, choice = "make_struct", transformation_ctx = "resolvechoice0")
dropnullfields0 = DropNullFields.apply(frame = resolvechoice0, transformation_ctx = "dropnullfields0")
datasinkpar0 = glueContext.write_dynamic_frame.from_options(frame = dropnullfields0, connection_type = "s3", connection_options = {"path": result_s3_parquet_path + "/table1"}, format = "parquet", transformation_ctx = "datasinkpar0")


if target_copy_required == 'Y':
    s3resource = boto3.resource('s3')
    s3client = boto3.client('s3')
    bucket = s3resource.Bucket(result_bucket)
    csv_objs = list(bucket.objects.filter(Prefix=database_name + '_csv/'))
    parquet_objs = list(bucket.objects.filter(Prefix=database_name + '_parquet/'))
    for key in csv_objs:
        if not re.match(r'.*_\$folder\$$', key.key):
            csv_temp_key = key.key
            csv_target_key = csv_temp_key.replace(database_name + '_csv', database_name)
            csv_target_key = csv_target_key + '.csv'
            try:
                s3client.copy_object(Bucket=target_bucket_csv,
                       Key=csv_target_key,
                       CopySource={'Bucket':result_bucket, 'Key':csv_temp_key},
                       ACL='bucket-owner-full-control',
                       ContentType='text/plain')
                s3client.delete_object(Bucket=result_bucket,
                         Key=csv_temp_key)
            except ClientError as ce:
                if ce.response['Error']['Code'] == 'NoSuchKey':
                    print("Key Error: {}".format(ce))
                    print(ce.response['Error']['Key'])
                else:
                    raise ce
    for key in parquet_objs:
        if not re.match(r'.*_\$folder\$$', key.key):
            par_temp_key = key.key
            par_target_key = par_temp_key.replace(system_name + '_parquet', database_name)
            try:
                s3client.copy_object(Bucket=target_bucket_parquet,
                       Key=par_target_key,
                       CopySource={'Bucket':result_bucket, 'Key':par_temp_key},
                       ACL='bucket-owner-full-control',
                       ContentType='text/plain')
                s3client.delete_object(Bucket=result_bucket,
                         Key=par_temp_key)
            except ClientError as pe:
                if pe.response['Error']['Code'] == 'NoSuchKey':
                    print("Key Error: {}".format(pe))
                    print(pe.response['Error']['Key'])
                else:
                    raise pe


job.commit()
