#!/usr/bin/env python3

'''
This script loads data into RDS from csv files stored in S3.

Requirements:
    * S3 file path should be of format s3://<bucket>/<database>/<table>/<csv_files>
    * The schema should match the csv row.
    * RDS should have S3 access IAM role assigned in db parameter group parameters 'aurora_load_from_s3_role' or 'aws_default_s3_role'.
        Bucket: ListBucket
        Objects: GetObject and GetObjectVersion
    * Same IAM role should be assigned to the aurora cluster (add-role-to-db-cluster).
    * For private RDS to connect to S3, a VPC endpoint should be created for S3 service and update aurora route tables.
    * Update the security group with right rules - egress rule to allow connections to prefix list id of S3 VPC endpoint.
    * DB user should have LOAD FROM S3 privilege on database.
    * DB connection parameters and bucket should be in ENV.
    Refer: https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/AuroraMySQL.Integrating.LoadFromS3.html
'''

import os
import sys
import json
import pymysql.cursors
import boto3
from botocore.exceptions import ClientError
import warnings

class ConfigContext:
    '''
    Global configs
    '''
    def __init__(self):
        '''
        Initialize global configs.
        '''
        self.db_host        = self.environ_or_die('DB_HOST')
        self.db_port        = self.environ_or_die('DB_PORT')
        self.db_user        = self.environ_or_die('DB_USER')
        self.db_password    = self.environ_or_die('DB_PASSWORD')
        self.source_bucket  = self.environ_or_die('SOURCE_BUCKET')
        self.archive_bucket = self.environ_or_die('ARCHIVE_BUCKET')


    def environ_or_die(self,env):
        '''
        Get the variable from environment or die
        '''
        try:
            return os.environ[env]
        except:
            print('Missing {envvar} in environment.'.format(envvar=env))
            sys.exit(1)

CONFIG = ConfigContext()

def data_load(key):
    '''
    Load the data to RDS from s3 csv files.

    data_load('<s3-key>')

    Ignoring error (1451, u'Cannot delete or update a parent row: a foreign key constraint fails )
    '''
    (database,table,file) = key.split('/')
    l_status = ''
    connection = pymysql.connect(host=CONFIG.db_host,
                             user=CONFIG.db_user,
                             password=CONFIG.db_password,
                             db=database,
                             charset='utf8mb4',
                             cursorclass=pymysql.cursors.DictCursor)
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore")
        try:
            with connection.cursor() as cursor:
                cursor.execute('SET FOREIGN_KEY_CHECKS = 0')
                sql = "LOAD DATA FROM S3 's3://{0}/{1}' REPLACE INTO TABLE {2} FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"' ESCAPED BY '\"' LINES TERMINATED BY '\n' IGNORE 1 LINES".format(CONFIG.source_bucket,key,table)
                cursor.execute(sql)
                cursor.execute('SET FOREIGN_KEY_CHECKS = 1')

                # connection is not autocommit by default.
                connection.commit()
                l_status = 'Success'
        except pymysql.err.IntegrityError as e:
            if e.args[0] == 1451:
                # This will ingore Error: 1451, Cannot delete or update a parent row: a foreign key constraint fails
                l_status = 'Success'
            else:
                l_status = e
        except Exception as e:
            l_status = e
        finally:
            connection.close()
            return l_status

def main():
    '''
    Main function

    To get the data from S3 and load to RDS DB
    '''
    client = boto3.client('s3')
    paginator = client.get_paginator('list_objects')
    resultPages = paginator.paginate(Bucket=CONFIG.source_bucket)
    for page in resultPages:
        if 'Contents' in page:
            for s3obj in page['Contents']:
                loadResponse = data_load(s3obj['Key'])
                if not loadResponse == 'Success':
                    print(json.dumps({'status':'Failed', 'Failure':'Loading failed for {file}'.format(file=str(s3obj['Key'])), 'Reason':'Error {response}'.format(response=str(loadResponse))}))

if __name__ == '__main__':
    main()
