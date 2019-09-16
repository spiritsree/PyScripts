#!/usr/bin/env python3

'''
This script is to check the dependency of a Security Group with other AWS resources.

Reuirements: boto3-1.9.71 or higher

Supported resources: EC2, ELB, ALB, ElastiCache, RDS, Redshift, EMR, Security Groups, ENI

Usage: checkSecurityGroupDependency.py [-h] (--vpc_id VPC_ID | --sg_id SG_ID | --output [json|csv])
'''
import argparse
import sys
import re
import json
import boto3
import botocore

DATA_JSON = {}
RESOURCE_DATA = {}

def is_group_exist(sg_id, res_name):
    '''
    This will check if given SG group and resource keywords exist
    in RESOURCE_DATA json
    '''
    if sg_id not in RESOURCE_DATA:
        RESOURCE_DATA[sg_id] = {}
    if res_name not in RESOURCE_DATA[sg_id]:
        RESOURCE_DATA[sg_id][res_name] = []
    return True

def get_vpc_id(s_id):
    '''
    Get VPC ID from the security group
    '''
    ec2 = boto3.client('ec2')
    vpc_id = ''
    try:
        response = ec2.describe_security_groups(
            GroupIds=[
                s_id
            ]
        )
        vpc_id = response['SecurityGroups'][0]['VpcId']
    except botocore.exceptions.ClientError as err_string:
        print('ERR: Security Group does not exist !!! {0}'.format(err_string))
        return False

    return vpc_id

def get_ec2_instance(vpc_id):
    '''
    Collect EC2 instances and SGs
    '''
    ec2 = boto3.client('ec2')
    instances = ec2.describe_instances(Filters=[
        {
            'Name': 'vpc-id',
            'Values': [
                vpc_id
            ]
        }
    ])
    for reservation in instances['Reservations']:
        for instance in reservation['Instances']:
            for s_group in instance['SecurityGroups']:
                s_id = s_group['GroupId']
                if is_group_exist(s_id, 'EC2'):
                    RESOURCE_DATA[s_id]['EC2'].append(instance['InstanceId'])
            # Security groups used by network interfaces
            for interfaces in instance['NetworkInterfaces']:
                for groups in interfaces['Groups']:
                    s_id2 = groups['GroupId']
                    if is_group_exist(s_id2, 'EC2'):
                        if instance['InstanceId'] not in RESOURCE_DATA[s_id2]['EC2']:
                            RESOURCE_DATA[s_id2]['EC2'].append(instance['InstanceId'])

def get_elbs():
    '''
    Collect ELB, ALB and used SGs
    '''
    elb = boto3.client('elb')
    elbs = elb.describe_load_balancers()
    for elb_id in elbs['LoadBalancerDescriptions']:
        if 'SecurityGroups' in elb_id:
            for s_id in elb_id['SecurityGroups']:
                if is_group_exist(s_id, 'ELB'):
                    RESOURCE_DATA[s_id]['ELB'].append(elb_id['LoadBalancerName'])
    alb = boto3.client('elbv2')
    albs = alb.describe_load_balancers()
    for alb_id in albs['LoadBalancers']:
        if 'SecurityGroups' in alb_id:
            for s_id2 in alb_id['SecurityGroups']:
                if is_group_exist(s_id2, 'ALB'):
                    RESOURCE_DATA[s_id2]['ALB'].append(alb_id['LoadBalancerName'])

def get_elasticache():
    '''
    Collect Elasticache and used SGs
    '''
    elastic_cache = boto3.client('elasticache')
    ec_cluster = elastic_cache.describe_cache_clusters()
    for cluster in ec_cluster['CacheClusters']:
        for s_group in cluster['SecurityGroups']:
            s_id = s_group['SecurityGroupId']
            if is_group_exist(s_id, 'ElastiCache'):
                RESOURCE_DATA[s_id]['ElastiCache'].append(cluster['CacheClusterId'])

def get_rds():
    '''
    Collect RDS and used SGs
    '''
    rds_client = boto3.client('rds')
    # Check RDS DB security groups
    rds_sg_paginator = rds_client.get_paginator('describe_db_security_groups')
    rds_sg_pages = rds_sg_paginator.paginate()
    for rds_sg_page in rds_sg_pages:
        for rds_sg in rds_sg_page['DBSecurityGroups']:
            for ec2_sg in rds_sg['EC2SecurityGroups']:
                if is_group_exist(ec2_sg['EC2SecurityGroupId'], 'RDS'):
                    RESOURCE_DATA[ec2_sg['EC2SecurityGroupId']]['RDS'].append(rds_sg['DBSecurityGroupName'])
    # Check RDS clusters
    rds_cluster_paginator = rds_client.get_paginator('describe_db_clusters')
    rds_cluster_pages = rds_cluster_paginator.paginate()
    for rds_cluster_page in rds_cluster_pages:
        for cluster in rds_cluster_page['DBClusters']:
            for s_group in cluster['VpcSecurityGroups']:
                s_id = s_group['VpcSecurityGroupId']
                if is_group_exist(s_id, 'RDS'):
                    RESOURCE_DATA[s_id]['RDS'].append(cluster['DBClusterIdentifier'])
    # Check RDS instances
    rds_instance_paginator = rds_client.get_paginator('describe_db_instances')
    rds_instance_pages = rds_instance_paginator.paginate()
    for rds_instance_page in rds_instance_pages:
        for instance in rds_instance_page['DBInstances']:
            for s_group2 in instance['VpcSecurityGroups']:
                s_id2 = s_group2['VpcSecurityGroupId']
                if is_group_exist(s_id2, 'RDS'):
                    RESOURCE_DATA[s_id2]['RDS'].append(instance['DBInstanceIdentifier'])

def get_redshift():
    '''
    Collect Redshift and used SGs
    '''
    rs_client = boto3.client('redshift')
    rs_cluster = rs_client.describe_clusters()
    for cluster in rs_cluster['Clusters']:
        for s_group in cluster['VpcSecurityGroups']:
            s_id = s_group['VpcSecurityGroupId']
            if is_group_exist(s_id, 'Redshift'):
                RESOURCE_DATA[s_id]['Redshift'].append(cluster['ClusterIdentifier'])

def get_emr():
    '''
    Collect EMR and used SGs
    '''
    emr_clusters = []
    emr_client = boto3.client('emr')
    for cluster in emr_client.list_clusters()['Clusters']:
        emr_clusters.append(cluster['Id'])
    for emr_id in emr_clusters:
        emr_cluster_config = emr_client.describe_cluster(
            ClusterId=emr_id
            )
        sg_data = emr_cluster_config['Cluster']['Ec2InstanceAttributes']
        if is_group_exist(sg_data['EmrManagedMasterSecurityGroup'], 'EMR'):
            RESOURCE_DATA[sg_data['EmrManagedMasterSecurityGroup']]['EMR'].append(emr_id)
        if is_group_exist(sg_data['EmrManagedSlaveSecurityGroup'], 'EMR'):
            RESOURCE_DATA[sg_data['EmrManagedSlaveSecurityGroup']]['EMR'].append(emr_id)
        if 'AdditionalMasterSecurityGroups' in sg_data:
            if len(sg_data['AdditionalMasterSecurityGroups']) > 0:
                for s_id in sg_data['AdditionalMasterSecurityGroups']:
                    if is_group_exist(s_id, 'EMR'):
                        RESOURCE_DATA[s_id]['EMR'].append(emr_id)
        if 'AdditionalSlaveSecurityGroups' in sg_data:
            if len(sg_data['AdditionalSlaveSecurityGroups']) > 0:
                for s_id2 in sg_data['AdditionalSlaveSecurityGroups']:
                    if is_group_exist(s_id2, 'EMR'):
                        RESOURCE_DATA[s_id2]['EMR'].append(emr_id)
        if 'ServiceAccessSecurityGroup' in sg_data:
            if is_group_exist(sg_data['ServiceAccessSecurityGroup'], 'EMR'):
                RESOURCE_DATA[sg_data['ServiceAccessSecurityGroup']]['EMR'].append(emr_id)

def get_enis(v_id):
    '''
    Collect ENIs and used SGs
    '''
    ec2 = boto3.client('ec2')
    eni_paginator = ec2.get_paginator('describe_network_interfaces')
    eni_pages = eni_paginator.paginate(
        Filters=[
            {
                'Name': 'vpc-id',
                'Values': [
                    v_id
                ]
            }
        ]
    )
    for eni_page in eni_pages:
        for interface in eni_page['NetworkInterfaces']:
            for s_id in interface['Groups']:
                if is_group_exist(s_id['GroupId'], 'ENI'):
                    RESOURCE_DATA[s_id['GroupId']]['ENI'].append(interface['NetworkInterfaceId'])

def get_lambdas():
    '''
    Collect Lambdas and related SGs
    '''
    lambda_cli = boto3.client('lambda')
    lambda_paginator = lambda_cli.get_paginator('list_functions')
    lambda_pages = lambda_paginator.paginate()
    for lambda_page in lambda_pages:
        for l_fn in lambda_page['Functions']:
            if 'VpcConfig' in l_fn:
                for s_id in l_fn['VpcConfig']['SecurityGroupIds']:
                    if is_group_exist(s_id, 'Lambda'):
                        RESOURCE_DATA[s_id]['Lambda'].append(l_fn['FunctionName'])

def get_dms():
    '''
    Collect DMS instance and associated SGs
    '''
    dms = boto3.client('dms')
    dms_instance_response = dms.describe_replication_instances()
    for dms_instance in dms_instance_response['ReplicationInstances']:
        for s_group in dms_instance['VpcSecurityGroups']:
            s_id = s_group['VpcSecurityGroupId']
            if is_group_exist(s_id, 'DMS'):
                RESOURCE_DATA[s_id]['DMS'].append(dms_instance['ReplicationInstanceIdentifier'])

def get_sgs(v_id):
    '''
    Check if the SG is referenced in any other SGs
    '''
    ec2 = boto3.client('ec2')
    sg_list = ec2.describe_security_groups(Filters=[
        {
            'Name': 'vpc-id',
            'Values': [
                v_id
            ]
        }
    ])
    for item in sg_list['SecurityGroups']:
        g_id = item['GroupId']
        if is_group_exist(g_id, 'SG'):
            sg_paginator = ec2.get_paginator('describe_security_groups')
            sg_pages = sg_paginator.paginate(
                Filters=[{
                    'Name': 'ip-permission.group-id',
                    'Values': [g_id]
                }])
            for sg_page in sg_pages:
                for s_id in sg_page['SecurityGroups']:
                    if s_id['GroupId'] not in RESOURCE_DATA[g_id]['SG']:
                        RESOURCE_DATA[g_id]['SG'].append(s_id['GroupId'])
            sg_pages_egress = sg_paginator.paginate(
                Filters=[{
                    'Name': 'egress.ip-permission.group-id',
                    'Values': [g_id]
                }])
            for sg_page_egress in sg_pages_egress:
                for sg_egress in sg_page_egress['SecurityGroups']:
                    if sg_egress['GroupId'] not in RESOURCE_DATA[g_id]['SG']:
                        RESOURCE_DATA[g_id]['SG'].append(sg_egress['GroupId'])
        if 'Metadata' not in RESOURCE_DATA[g_id]:
            RESOURCE_DATA[g_id]['Metadata'] = {}
        RESOURCE_DATA[g_id]['Metadata']['Name'] = item['GroupName']
        RESOURCE_DATA[g_id]['Metadata']['VPC'] = item['VpcId']

def get_all_sg(v_id):
    '''
    Get all the Security Groups provided a VPC ID
    '''
    sg_list = []
    ec2 = boto3.client('ec2')
    response = ec2.describe_security_groups(Filters=[
        {
            'Name': 'vpc-id',
            'Values': [
                v_id
            ]
        }
    ])
    for item in response['SecurityGroups']:
        g_id = item['GroupId']
        sg_list.append(g_id)
    return sg_list

def get_reource_sg_mapping(v_id):
    '''
    Get all the resources and SGs used by the resources
    '''
    get_ec2_instance(v_id)
    get_elbs()
    get_elasticache()
    get_rds()
    get_redshift()
    get_emr()
    get_enis(v_id)
    get_lambdas()
    get_dms()
    get_sgs(v_id)

    return True

def csv_display():
    '''
    To display the outut in csv format
    Group ID,Group Name,VPC,Used,Comments,Used Resources
    '''
    print('Group ID,Group Name,VPC,Used,Comments,Used Resources')
    for s_id in DATA_JSON:
        s_res = ''
        for aws_res in DATA_JSON[s_id]:
            if aws_res == 'Metadata':
                continue
            else:
                if DATA_JSON[s_id][aws_res]:
                    s_res += '- {0} : '.format(aws_res)
                    s_res += ', '.join(DATA_JSON[s_id][aws_res])
                    s_res += '\n'
        print('{0},{1},{2},{3},{4},"{5}"'.format(
            s_id,
            DATA_JSON[s_id]['Metadata']['Name'],
            DATA_JSON[s_id]['Metadata']['VPC'],
            'YES',
            '',
            s_res))

def main():
    '''
    Main Function
    '''
    parser = argparse.ArgumentParser(description='Security Group Dependency Check')
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--vpc_id', help='Provide a VPC ID')
    group.add_argument('--sg_id', help='Provide a Security Group ID')
    parser.add_argument('--output', choices=['json', 'csv'], default='json')

    args = parser.parse_args()
    vpc_id = ''
    group_id = ''

    if args.vpc_id:
        vpc_id = args.vpc_id
        if re.match('^vpc-[0-9a-f]+$', vpc_id):
            for each_sg in get_all_sg(vpc_id):
                DATA_JSON[each_sg] = {}
        else:
            print('Not a valid VPC ID !!!')
            sys.exit(1)

    if args.sg_id:
        group_id = args.sg_id
        if re.match('^sg-[0-9a-f]+$', group_id):
            vpc_id = get_vpc_id(group_id)
            DATA_JSON[group_id] = {}
        else:
            print('Not a valid security group !!!')
            sys.exit(1)

    # Get all resource SGs
    get_reource_sg_mapping(vpc_id)

    for each_sg_id in DATA_JSON:
        DATA_JSON[each_sg_id] = RESOURCE_DATA[each_sg_id]

    if args.output == 'json':
        print(json.dumps(DATA_JSON))
    elif args.output == 'csv':
        csv_display()

if __name__ == '__main__':
    main()
