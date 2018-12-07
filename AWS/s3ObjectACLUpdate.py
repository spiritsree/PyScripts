#!/usr/bin/env python3

'''
This script will updated the ACL of S3 objects.

Needs SOURCE_BUCKET and CANONICAL_ID as env variable set.

response = client.put_object_acl(
    ACL='private'|'public-read'|'public-read-write'|'authenticated-read'|'aws-exec-read'|'bucket-owner-read'|'bucket-owner-full-control',
    AccessControlPolicy={
        'Grants': [
            {
                'Grantee': {
                    'DisplayName': 'string',
                    'EmailAddress': 'string',
                    'ID': 'string',
                    'Type': 'CanonicalUser'|'AmazonCustomerByEmail'|'Group',
                    'URI': 'string'
                },
                'Permission': 'FULL_CONTROL'|'WRITE'|'WRITE_ACP'|'READ'|'READ_ACP'
            },
        ],
        'Owner': {
            'DisplayName': 'string',
            'ID': 'string'
        }
    },
    Bucket='string',
    GrantFullControl='string',
    GrantRead='string',
    GrantReadACP='string',
    GrantWrite='string',
    GrantWriteACP='string',
    Key='string',
    RequestPayer='requester',
    VersionId='string'
)
'''

import json
import boto3
from botocore.exceptions import ClientError

class ConfigContext:
    '''
    Global configs
    '''
    def __init__(self):
        '''
        Initialize global configs.
        '''
        self.source_bucket  = self.environ_or_die('SOURCE_BUCKET')
        self.canonical_id   = self.environ_or_die('CANONICAL_ID')


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


def main():
    '''
    Main function

    To get S3 objects and update the ACL.
    '''
    permission = 'id={id}'.format(id=CONFIG.canonical_id)
    client = boto3.client('s3')
    paginator = client.get_paginator('list_objects')
    resultPages = paginator.paginate(Bucket=CONFIG.source_bucket)
    for page in resultPages:
        if 'Contents' in page:
            for s3obj in page['Contents']:
                print(s3obj['Key'])
                client.put_object_acl(Bucket=CONFIG.source_bucket,
                    Key=s3obj['Key'],
                    GrantRead=permission)

if __name__ == '__main__':
    main()
