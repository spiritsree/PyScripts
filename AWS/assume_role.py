#!/usr/bin/env python3

'''
The precendence of the configs etc are given in the doc
https://docs.aws.amazon.com/cli/latest/topic/config-vars.html

Since this is for assuming role (role switching in cli), it uses the config from files and
export all creds to a child shell. Hence this does not use any creds already in env except for these
'AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY' and 'AWS_DEFAULT_REGION' .

----config----
[profile development]
aws_access_key_id=foo
aws_secret_access_key=bar
region=us-west-2
output=json

----credential-----
[development]
aws_access_key_id=foo
aws_secret_access_key=bar

Note: You cannot call AssumeRole by using AWS root account credentials; access is denied.
      You must use credentials for an IAM user or an IAM role to call AssumeRole .
Ref: https://docs.python.org/3/library/configparser.html
     https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sts.html
'''

import os
import subprocess
import sys
import configparser
import boto3

class AWSConfig:
    '''
    AWS Config Class to get the config object with all
    credentials etc.
    '''
    def __init__(self):
        '''
        Initialize class.
        '''
        self.aws_custom_credential = 'AWS_SHARED_CREDENTIALS_FILE'
        self.aws_default_credential = '~/.aws/credentials'
        self.aws_custom_config = 'AWS_PROFILE'
        self.aws_default_config = '~/.aws/config'
        self.assume_config = {}
        self.config_json = {}
        self.config_params = ['aws_access_key_id',
                              'aws_secret_access_key',
                              'region',
                              'role_arn',
                              'output',
                              'mfa_serial']
        for param in self.config_params:
            self.assume_config[param] = ""

    @staticmethod
    def get_env(env):
        '''
        Get the variable from environment if exists
        '''
        try:
            return os.environ[env]
        except KeyError:
            return ''

    @staticmethod
    def file_exists(file):
        '''
        To check if a file exists
        '''
        return bool(os.path.isfile(os.path.expanduser(file)))

    def populate_from_env(self):
        '''
        Populate the config json from env variables
        '''
        access_key_id = self.get_env('AWS_ACCESS_KEY_ID')
        access_key = self.get_env('AWS_SECRET_ACCESS_KEY')
        if access_key_id and access_key:
            self.assume_config['aws_access_key_id'] = access_key_id
            self.assume_config['aws_secret_access_key'] = access_key
        region = self.get_env('AWS_DEFAULT_REGION')
        if region:
            self.assume_config['region'] = region
        default_output = self.get_env('AWS_DEFAULT_OUTPUT')
        if default_output:
            self.assume_config['output'] = default_output

    def populate_from_credentials(self, conf_param, config, profile, f_type):
        '''
        Populate config from credential file
        '''
        if f_type == 'credentials':
            profile_section = profile
        elif f_type == 'configs':
            profile_section = 'profile {}'.format(profile)

        if config.has_section(profile_section):
            if config.has_option(profile_section, conf_param):
                self.assume_config[conf_param] = config.get(profile_section, conf_param)
                return True
            elif config.has_option(profile_section, 'source_profile'):
                source_profile = config.get(profile_section, 'source_profile')

                if f_type == 'credentials':
                    source_profile_section = source_profile
                elif f_type == 'configs':
                    source_profile_section = 'profile {}'.format(source_profile)

                if config.has_section(source_profile_section):
                    if config.has_option(source_profile_section, conf_param):
                        self.assume_config[conf_param] = config.get(source_profile_section,
                                                                    conf_param)
                        return True
                    elif config.has_section('default'):
                        if config.has_option('default', conf_param):
                            self.assume_config[conf_param] = config.get('default', conf_param)
                            return True
                        elif config.has_option('default', 'source_profile'):
                            def_source_profile = config.get('default', 'source_profile')

                            if f_type == 'credentials':
                                def_prof_sec = def_source_profile
                            elif f_type == 'configs':
                                def_prof_sec = 'profile {}'.format(def_source_profile)

                            if config.has_section(def_prof_sec):
                                if config.has_option(def_prof_sec, conf_param):
                                    self.assume_config[conf_param] = config.get(def_prof_sec,
                                                                                conf_param)
                                    return True
            elif config.has_section('default'):
                if config.has_option('default', conf_param):
                    self.assume_config[conf_param] = config.get('default', conf_param)
                    return True
                elif config.has_option('default', 'source_profile'):
                    def_source_profile = config.get('default', 'source_profile')

                    if f_type == 'credentials':
                        def_prof_sec = def_source_profile
                    elif f_type == 'configs':
                        def_prof_sec = 'profile {}'.format(def_source_profile)

                    if config.has_section(def_prof_sec):
                        if config.has_option(def_prof_sec, conf_param):
                            self.assume_config[conf_param] = config.get(def_prof_sec, conf_param)
                            return True
        elif config.has_section('default'):
            if config.has_option('default', conf_param):
                self.assume_config[conf_param] = config.get('default', conf_param)
                return True
            elif config.has_option('default', 'source_profile'):
                def_source_profile = config.get('default', 'source_profile')

                if f_type == 'credentials':
                    def_prof_sec = def_source_profile
                elif f_type == 'configs':
                    def_prof_sec = 'profile {}'.format(def_source_profile)

                if config.has_section(def_prof_sec):
                    if config.has_option(def_prof_sec, conf_param):
                        self.assume_config[conf_param] = config.get(def_prof_sec, conf_param)
                        return True
        return False

    def populate_from_config(self, conf_param, config_obj, profile):
        '''
        Populate config from files
        '''
        for i in config_obj:
            if self.populate_from_credentials(conf_param,
                                              config_obj[i]['config'],
                                              profile,
                                              config_obj[i]['type']):
                return True
        return False

    @staticmethod
    def check_profile_exist(config_obj, profile):
        '''
        Check if profile exists
        '''
        for i in config_obj:
            if config_obj[i]['config'].has_section(profile):
                return True
        return False

    def generate_config(self, profile):
        '''
        To populate the aws config based on the following order

        AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY env variable ->
        AWS_SHARED_CREDENTIALS_FILE -> ~/.aws/credentials -> AWS_PROFILE  ->
        ~/.aws/config
        '''
        custom_cred_file = self.get_env(self.aws_custom_credential)
        custom_config_file = self.get_env(self.aws_custom_config)
        config_files = {}
        config_files[0] = {"file": custom_cred_file, "type": "credentials"}
        config_files[1] = {"file": self.aws_default_credential, "type": "credentials"}
        config_files[2] = {"file": custom_config_file, "type": "configs"}
        config_files[3] = {"file": self.aws_default_config, "type": "configs"}

        for i in range(4):
            custom_cred_config = configparser.ConfigParser()
            self.config_json[i] = {}
            self.config_json[i]['config'] = custom_cred_config
            self.config_json[i]['type'] = config_files[i]['type']
            if config_files[i]['file']:
                if self.file_exists(config_files[i]["file"]):
                    custom_cred_config.read(os.path.expanduser(config_files[i]["file"]))
                    self.config_json[i]['config'] = custom_cred_config

        if not self.check_profile_exist(self.config_json, profile):
            print('Profile "{0}" does not exist !!!'.format(profile))
            sys.exit(1)

        for conf_param in self.config_params:
            self.populate_from_config(conf_param, self.config_json, profile)

        self.populate_from_env()

def generate_assume_role_param(config):
    '''
    To generate sts call parameters
    '''
    config_param = {}
    if 'role_arn' in config:
        config_param['RoleArn'] = config['role_arn']
    if 'role_session_name' in config:
        config_param['RoleSessionName'] = config['role_session_name']
    else:
        config_param['RoleSessionName'] = "assume_role"
    if 'duration_seconds' in config:
        config_param['DurationSeconds'] = config['duration_seconds']
    else:
        config_param['DurationSeconds'] = 3600
    if 'external_id' in config:
        config_param['ExternalId'] = config['external_id']
    if 'mfa_serial' in config:
        config_param['SerialNumber'] = config['mfa_serial']
        token_code = input("Enter MFA Code: ")
        config_param['TokenCode'] = token_code
    return config_param


def main():
    '''
    Main Function
    '''
    if len(sys.argv) <= 1:
        profile = 'default'
    else:
        profile = sys.argv[1]

    # Generate AWS Credentials
    aws_config = AWSConfig()
    aws_config.generate_config(profile)

    # Start boto3 session
    sts_session = boto3.session.Session(aws_access_key_id=aws_config.assume_config['aws_access_key_id'], # pylint: disable=line-too-long
                                        aws_secret_access_key=aws_config.assume_config['aws_secret_access_key']) # pylint: disable=line-too-long

    # Get assume role params required for sts call
    assume_role_params = generate_assume_role_param(aws_config.assume_config)
    sts_client = sts_session.client('sts')
    sts_response = sts_client.assume_role(**assume_role_params)

    # Prepare environment for shell
    expiry_time = int(sts_response['Credentials']['Expiration'].timestamp())
    env = os.environ.copy()
    env['AWS_DEFAULT_REGION'] = aws_config.assume_config['region']
    env['AWS_SESSION_NAME'] = assume_role_params['RoleSessionName']
    env['AWS_ACCESS_KEY_ID'] = sts_response['Credentials']['AccessKeyId']
    env['AWS_SECRET_ACCESS_KEY'] = sts_response['Credentials']['SecretAccessKey']
    env['AWS_SESSION_TOKEN'] = sts_response['Credentials']['SessionToken']
    env['AWS_SESSION_EXPIRATION'] = str(expiry_time)
    env['AWS_TARGET_PROFILE'] = profile

    # Start a subshell with credentials exported
    shell_bin = os.getenv('SHELL', '/bin/bash')
    sub_shell = subprocess.Popen(shell_bin, env=env)
    sub_shell.wait()

if __name__ == '__main__':
    main()
