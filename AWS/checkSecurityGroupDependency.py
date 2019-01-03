#!/usr/bin/env python3

'''
This script is to check the dependency of a Security Group with other AWS resources.

Reuirements: boto3-1.9.71 or higher

Supported resources: EC2, ELB, ALB, ElastiCache, RDS, Redshift, EMR, Security Groups, ENI

Usage: checkSecurityGroupDependency.py <SecurityGroup_ID>
'''
import boto3
import botocore
import sys
import re

DATA_JSON = {}

def get_vpc_id(id):
    '''
    Get VPC ID from the security group
    '''
    ec2 = boto3.client('ec2')
    vpc_id = ''
    try:
        response = ec2.describe_security_groups(
            GroupIds=[
                id
            ]
        )
        vpc_id = response['SecurityGroups'][0]['VpcId']
    except botocore.exceptions.ClientError as e:
        print('Security Group does not exist !!!')

    return vpc_id

def get_ec2_instance(sg_id, vpc_id):
    '''
    Check if the SG is used by any EC2 instance
    '''
    DATA_JSON['EC2'] = []
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
            for sg in instance['SecurityGroups']:
                if sg['GroupId'] == sg_id:
                    DATA_JSON['EC2'].append(instance['InstanceId'])
            # Security groups used by network interfaces
            for interfaces in instance['NetworkInterfaces']:
                for groups in interfaces['Groups']:
                    if groups['GroupId'] == sg_id:
                        if instance['InstanceId'] not in DATA_JSON['EC2']:
                            DATA_JSON['EC2'].append(instance['InstanceId'])

def get_elbs(sg_id):
    '''
    Check if the SG is used by any ELB or ALB
    '''
    DATA_JSON['ELB'] = []
    DATA_JSON['ALB'] = []
    elb = boto3.client('elb')
    elbs = elb.describe_load_balancers()
    for lb in elbs['LoadBalancerDescriptions']:
        for sg in lb['SecurityGroups']:
            if sg == sg_id:
                DATA_JSON['ELB'].append(lb['LoadBalancerName'])

    alb = boto3.client('elbv2')
    albs = alb.describe_load_balancers()
    for lb2 in albs['LoadBalancers']:
        for sg2 in lb2['SecurityGroups']:
            if sg2 == sg_id:
                DATA_JSON['ALB'].append(lb['LoadBalancerName'])

def get_elasticache(sg_id):
    '''
    Check if the SG is used by any ElastiCache instance
    '''
    DATA_JSON['ElastiCache'] = []
    ec = boto3.client('elasticache')
    ec_cluster = ec.describe_cache_clusters()
    for cluster in ec_cluster['CacheClusters']:
        for sg in cluster['SecurityGroups']:
            if sg['SecurityGroupId'] == sg_id:
                DATA_JSON['ElastiCache'].append(cluster['CacheClusterId'])

def get_rds(sg_id):
    '''
    Check if the SG is used by any RDS instance
    '''
    DATA_JSON['RDS'] = []
    rds = boto3.client('rds')

    # Check RDS DB security groups
    rds_sg_paginator = rds.get_paginator('describe_db_security_groups')
    rds_sg_pages = rds_sg_paginator.paginate()
    for rds_sg_page in rds_sg_pages:
        for rds_sg in rds_sg_page['DBSecurityGroups']:
            for ec2_sg in rds_sg['EC2SecurityGroups']:
                if ec2_sg == sg_id:
                    DATA_JSON['RDS'].append(rds_sg['DBSecurityGroupName'])

    # Check RDS clusters
    rds_cluster_paginator = rds.get_paginator('describe_db_clusters')
    rds_cluster_pages = rds_cluster_paginator.paginate()
    for rds_cluster_page in rds_cluster_pages:
        for cluster in rds_cluster_page['DBClusters']:
            for sg in cluster['VpcSecurityGroups']:
                if sg['VpcSecurityGroupId'] == sg_id:
                    DATA_JSON['RDS'].append(cluster['DBClusterIdentifier'])

    # Check RDS instances
    rds_instance_paginator = rds.get_paginator('describe_db_instances')
    rds_instance_pages = rds_instance_paginator.paginate()
    for rds_instance_page in rds_instance_pages:
        for instance in rds_instance_page['DBInstances']:
            for sg in instance['VpcSecurityGroups']:
                if sg['VpcSecurityGroupId'] == sg_id:
                    DATA_JSON['RDS'].append(instance['DBInstanceIdentifier'])

def get_redshift(sg_id):
    '''
    Check if the SG is used by any Redshift cluster
    '''
    DATA_JSON['Redshift'] = []
    rs = boto3.client('redshift')
    rs_cluster = rs.describe_clusters()
    for cluster in rs_cluster['Clusters']:
        for sg in cluster['VpcSecurityGroups']:
            if sg['VpcSecurityGroupId'] == sg_id:
                DATA_JSON['Redshift'].append(cluster['ClusterIdentifier'])

def get_emr(sg_id):
    '''
    Check if the SG is used by any EMR cluster
    '''
    DATA_JSON['EMR'] = []
    emr_clusters = []
    emr = boto3.client('emr')
    for cluster in emr.list_clusters()['Clusters']:
        emr_clusters.append(cluster['Id'])

    for id in emr_clusters:
        emr_cluster_config = emr.describe_cluster(
            ClusterId=id
            )
        sg_data = emr_cluster_config['Cluster']['Ec2InstanceAttributes']
        if sg_data['EmrManagedMasterSecurityGroup'] == sg_id:
            DATA_JSON['EMR'].append(id)
        elif sg_data['EmrManagedSlaveSecurityGroup'] == sg_id:
            DATA_JSON['EMR'].append(id)
        elif len(sg_data['AdditionalMasterSecurityGroups']) > 0:
            for sg in sg_data['AdditionalMasterSecurityGroups']:
                if sg == sg_id:
                    DATA_JSON['EMR'].append(id)
        elif len(sg_data['AdditionalSlaveSecurityGroups']) > 0:
            for sg in sg_data['AdditionalSlaveSecurityGroups']:
                if sg == sg_id:
                    DATA_JSON['EMR'].append(id)
        elif sg_data['ServiceAccessSecurityGroup'] == sg_id:
            DATA_JSON['EMR'].append(id)

def get_sgs(sg_id):
    '''
    Check if the SG is referenced in any other SGs
    '''
    DATA_JSON['SG'] = []
    ec2 = boto3.client('ec2')
    sg_paginator = ec2.get_paginator('describe_security_groups')
    sg_pages = sg_paginator.paginate(
        Filters=[
            {
                'Name': 'ip-permission.group-id',
                'Values': [
                    sg_id
                ]
            }
        ]
    )
    for sg_page in sg_pages:
        for sg in sg_page['SecurityGroups']:
            if sg['GroupId'] not in DATA_JSON['SG']:
                DATA_JSON['SG'].append(sg['GroupId'])

    sg_pages_egress = sg_paginator.paginate(
        Filters=[
            {
                'Name': 'egress.ip-permission.group-id',
                'Values': [
                    sg_id
                ]
            }
        ]
    )
    for sg_page_egress in sg_pages_egress:
        for sg_egress in sg_page_egress['SecurityGroups']:
            if sg_egress['GroupId'] not in DATA_JSON['SG']:
                DATA_JSON['SG'].append(sg_egress['GroupId'])



def get_enis(sg_id, vpc_id):
    '''
    Check if the SG is associated to any network interfaces
    '''
    DATA_JSON['ENI'] = []
    ec2 = boto3.client('ec2')
    eni_paginator = ec2.get_paginator('describe_network_interfaces')
    eni_pages = eni_paginator.paginate(
        Filters=[
            {
                'Name': 'vpc-id',
                'Values': [
                    vpc_id
                ]
            }
        ]
    )
    for eni_page in eni_pages:
        for interface in eni_page['NetworkInterfaces']:
            for sg in interface['Groups']:
                if sg['GroupId'] == sg_id:
                    DATA_JSON['ENI'].append(interface['NetworkInterfaceId'])

def main():
    '''
    Main Function
    '''
    if not len(sys.argv) > 1:
        print('Please provide a security group !!!')
        sys.exit(1)
    else:
        group_id = sys.argv[1]
    if not re.match('^sg-[0-9a-f]+$', group_id):
        print('Not a valid security group !!!')
        sys.exit(1)
    vpc_id = get_vpc_id(group_id)
    if vpc_id:
        get_ec2_instance(group_id, vpc_id)
        get_elbs(group_id)
        get_elasticache(group_id)
        get_rds(group_id)
        get_redshift(group_id)
        get_emr(group_id)
        get_sgs(group_id)
        get_enis(group_id, vpc_id)
        for key in DATA_JSON.keys():
            if DATA_JSON[key]:
                print('{} resouce with id(s) "{}" uses the security group'.format(key, ', '.join(DATA_JSON[key])))

if __name__ == '__main__':
    main()


