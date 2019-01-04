#!/usr/bin/env python3

'''
Get the S3 Bucket size from cloudwatch metrics
'''

import boto3
from datetime import datetime, timedelta
import csv

def size_fmt(num):
    '''
    formatting the size in human readable format.
    '''
    if num < 1000:
        return '%i' % num + 'B'
    elif 1000 <= num < 1000000:
        return '%.1f' % (float(num)/1000) + 'KB'
    elif 1000000 <= num < 1000000000:
        return '%.1f' % (float(num)/1000000) + 'MB'
    elif 1000000000 <= num < 1000000000000:
        return '%.1f' % (float(num)/1000000000) + 'GB'
    elif 1000000000000 <= num < 1000000000000000:
        return '%.1f' % (float(num)/1000000000000) + 'TB'
    elif 1000000000000000 <= num:
        return '%.1f' % (float(num)/1000000000000000) + 'PB'

def get_bucketSize():
    '''
    Iterate through the list of buckets from S3 and get the used storage size (1 day average) from cloudwatch metrics.
    '''

    session = boto3.session.Session()
    s3 = session.client('s3')

    bucketSize = {}

    for bucket in s3.list_buckets()['Buckets']:
        try:
            location = s3.get_bucket_location(Bucket=bucket['Name'])
            bucketSize[bucket['Name']] = {}
            bucketSize[bucket['Name']]['ST'] = 0
            bucketSize[bucket['Name']]['IA'] = 0
            bucketSize[bucket['Name']]['RR'] = 0
            if location['LocationConstraint'] is None:
                location['LocationConstraint'] = 'us-east-1'

            # Open a cloudwatch session based on the region.
            cw = session.client('cloudwatch', region_name=location['LocationConstraint'])

            # Get S3 bucket avg size metrics for past 24 hours from cloudwatch.
            st_totalsize = cw.get_metric_statistics(Namespace='AWS/S3', MetricName='BucketSizeBytes', Dimensions=[{'Name': 'BucketName', 'Value': bucket['Name']}, {'Name': 'StorageType', 'Value': 'StandardStorage'}], StartTime=((datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%d 00:00:00")), EndTime=datetime.utcnow().strftime("%Y-%m-%d 00:00:00"), Period=86400, Statistics=['Average'], Unit='Bytes')
            ia_totalsize = cw.get_metric_statistics(Namespace='AWS/S3', MetricName='BucketSizeBytes', Dimensions=[{'Name': 'BucketName', 'Value': bucket['Name']}, {'Name': 'StorageType', 'Value': 'StandardIAStorage'}], StartTime=((datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%d 00:00:00")), EndTime=datetime.utcnow().strftime("%Y-%m-%d 00:00:00"), Period=86400, Statistics=['Average'], Unit='Bytes')
            rr_totalsize = cw.get_metric_statistics(Namespace='AWS/S3', MetricName='BucketSizeBytes', Dimensions=[{'Name': 'BucketName', 'Value': bucket['Name']}, {'Name': 'StorageType', 'Value': 'ReducedRedundancyStorage'}], StartTime=((datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%d 00:00:00")), EndTime=datetime.utcnow().strftime("%Y-%m-%d 00:00:00"), Period=86400, Statistics=['Average'], Unit='Bytes')
            if st_totalsize['Datapoints']:
                bucketSize[bucket['Name']]['ST'] = st_totalsize['Datapoints'][0]['Average']
            if ia_totalsize['Datapoints']:
                bucketSize[bucket['Name']]['IA'] = ia_totalsize['Datapoints'][0]['Average']
            if rr_totalsize['Datapoints']:
                bucketSize[bucket['Name']]['RR'] = rr_totalsize['Datapoints'][0]['Average']

        except Exception as e:
            log.error('Getting bucket size failed for bucket ' + str(bucket['Name']) + "with error " + str(e))
            continue

    return bucketSize


def display(bucketSize):
    '''
    To display bucket size in a pretty format.
    '''
    st_total = 0.0
    ia_total = 0.0
    rr_total = 0.0
    g_total = 0.0
    totalSize = {}
    print('{:<4} {:<64} {:<12} {:<9} {:<9} {:<9}'.format('No','Bucket','StandardSize','IASize','RRSize','Total'))
    print('-' * 107)
    count = 0
    for bkt in bucketSize.keys():
        b_total = 0
        st_total += float(bucketSize[bkt]['ST'])
        ia_total += float(bucketSize[bkt]['IA'])
        rr_total += float(bucketSize[bkt]['RR'])
        for st_type in bucketSize[bkt].keys():
            b_total += float(bucketSize[bkt][st_type])
        totalSize[bkt] = b_total
    sortedBucket = [(k, totalSize[k]) for k in sorted(totalSize, key=totalSize.get, reverse=True)]
    for k, v in sortedBucket:
        count = count + 1
        g_total += float(v)
        print('{:<4} {:<64} {:<12} {:<9} {:<9} {:<9}'.format(count, k, size_fmt(bucketSize[k]['ST']), size_fmt(bucketSize[k]['IA']), size_fmt(bucketSize[k]['RR']), size_fmt(v)))

    print ('\n\nStandard Storage - Total: ' + size_fmt(st_total))
    print ('Infrequent Access Storage - Total: ' + size_fmt(ia_total))
    print ('Reduced Redundancy Storage - Total: ' + size_fmt(rr_total))
    print ('\nTotal: ' + size_fmt(g_total) + '\n')

def writeCsv(csv_file,csv_columns,bucketSize):
    '''
    To write the bucket size report in csv file.
    '''
    try:
        with open(csv_file, 'w') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=csv_columns)
            writer.writeheader()
            count = 0
            totalSize = {}
            for bkt in bucketSize.keys():
                b_total = 0
                for st_type in bucketSize[bkt].keys():
                    b_total += bucketSize[bkt][st_type]
                totalSize[bkt] = b_total
            sortedData = [(k, totalSize[k]) for k in sorted(totalSize, key=totalSize.get, reverse=True)]
            for k, v in sortedData:
                count = count + 1
                data_array = { "Row": count, "Bucket": k, "StandardSize": size_fmt(bucketSize[k]['ST']), "IASize": size_fmt(bucketSize[k]['IA']), "RRSize": size_fmt(bucketSize[k]['RR']), "Total": size_fmt(v) }
                writer.writerow(data_array)
    except IOError as e:
        log.error('I/O error in writing file ' + csv_file + " with error " + str(e))
        return 'Error'

    return 'Success'

def main():
    '''
    Main function. Two modes - display and write

    display - shows the result on screen.
    write - writes to csv file
    '''

    mode='display'

    bucketSize = get_bucketSize()
    if mode == 'write':
        csv_file = "/tmp/s3_size.csv"
        csv_columns = ['Row','Bucket','StandardSize','IASize','RRSize','Total']
        wstatus = writeCsv(csv_file,csv_columns,bucketSize)

        if wstatus == 'Error':
            print('Writing the S3 Size data to csv file failed !!!')
        else:
            print('Written the S3 size data to {}'.format(csv_file))
    elif mode == 'display':
        display(bucketSize)


if __name__ == '__main__':
    main()

