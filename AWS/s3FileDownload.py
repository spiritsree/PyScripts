#!/usr/bin/env python3
'''
This is to download files from an S3 bucket recursively
and selectively based on the prefix match.

Need 2 env variabled set SOURCE_BUCKET, PREFIX and TARGET_DIR.

PREFIX can be the folder name.
'''

import os
import sys
import boto3

class ConfigContext:
    '''
    Global configs
    '''
    def __init__(self):
        '''
        Initialize global configs.
        '''
        self.source_bucket  = self.environ_or_die('SOURCE_BUCKET')
        self.prefix   = self.environ_or_die('PREFIX')
        self.target_dir = self.environ_or_default('TARGET_DIR', os.getcwd())

    def environ_or_die(self,env):
        '''
        Get the variable from environment or die
        '''
        try:
            return os.environ[env]
        except:
            print('Missing {envvar} in environment.'.format(envvar=env))
            sys.exit(1)

    def environ_or_default(self,env,default):
        '''
        Get the variable from environment or default
        '''
        try:
            return os.environ[env]
        except:
            return default

def createFolder(directory):
    '''
    To create a directory/directory path
    '''
    try:
        os.makedirs(directory)
    except OSError:
        print ('Error: Creating directory. ' +  directory)

def dirExist(directory):
    '''
    To check if a dir exists?

    returns true if exists
    '''
    if os.path.exists(directory):
        return True
    else:
        return False

def main():
    '''
    Main function

    To download files from S3 bucket.
    '''
    CONFIG = ConfigContext()
    s3resource = boto3.resource('s3')
    s3client = boto3.client('s3')
    key_obj = []
    bucket = s3resource.Bucket(CONFIG.source_bucket)
    objs = list(bucket.objects.filter(Prefix=CONFIG.prefix))
    for key in objs:
        temp_key = key.key
        key_obj = temp_key.split('/')
        del key_obj[-1]
        out_dir = '{basedir}/{path}'.format(basedir=CONFIG.target_dir, path='/'.join(key_obj))
        if not dirExist(out_dir):
            createFolder(out_dir)
        output_file = '{basedir}/{key}'.format(basedir=CONFIG.target_dir, key=temp_key)
        print('{key}, {out}'.format(key=temp_key, out=output_file))
        s3resource.meta.client.download_file(CONFIG.source_bucket, temp_key, output_file)


if __name__ == '__main__':
    main()
