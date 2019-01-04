#!/usr/bin/env python3

'''
Delete old Launch Configurations.
'''

import boto3
import botocore
from dateutil.tz import tzutc
import datetime

def get_active_lc(client):
    '''
    Get active Launch Configurations (active_lc).
    '''
    ls =''
    active_lc = []
    paginator = client.get_paginator('describe_auto_scaling_groups')
    page_iterator = paginator.paginate(PaginationConfig={'PageSize': 100})

    for as_groups in page_iterator:
        for as_group in as_groups['AutoScalingGroups']:
            active_lc.append(as_group['LaunchConfigurationName'])

    return active_lc

def get_all_lc(client):
    '''
    Get all Launch Configurations (all_lc).
    '''
    all_lc = {}
    paginator = client.get_paginator('describe_launch_configurations')
    page_iterator = paginator.paginate(PaginationConfig={'PageSize': 100})

    for lc_group in page_iterator:
        for lc in lc_group['LaunchConfigurations']:
            all_lc[lc['LaunchConfigurationName']] = lc['CreatedTime'].date().strftime('%Y%m%d')

    return all_lc

def delete_lc(client, active_lc, all_lc, old):
    '''
    Delete LC older than given days
    '''
    for keys in all_lc:
        lc = keys
        cutoff_date = datetime.datetime.today().date() - datetime.timedelta(old)
        if int(cutoff_date.strftime('%Y%m%d')) > int(all_lc[keys]):
            if not lc in active_lc:
                print('Deleting LC {0}'.format(lc))
                try:
                    response = client.delete_launch_configuration(
                                    LaunchConfigurationName=lc
                                )
                except botocore.exceptions.ClientError as e:
                    print('Deleting LC {0} failed with error {1}'.format(lc, e))

    return True


def main():
    '''
    Main function
    '''
    client = boto3.client('autoscaling')
    active_lc = get_active_lc(client)
    all_lc = get_all_lc(client)
    delete_age = 100
    response = delete_lc(client, active_lc, all_lc, delete_age)

if __name__ == '__main__':
    main()
