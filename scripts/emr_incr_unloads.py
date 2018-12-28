#!/usr/bin/env python
import pprint, deploy_emr as demr
from datetime import datetime, timedelta
from lib import s3_bucket, s3_bucket_intern, inc_datetime_format, region

def main():
    dtf         = inc_datetime_format
    prev_date   = (datetime.utcnow().date() - timedelta(days=2)).strftime(dtf) #20180227 #yesterday
    curr_date   = (datetime.utcnow().date() - timedelta(days=1)).strftime(dtf) #20180228 #today
    s3emr_script= 's3://{bkt_intern}/emr-scripts/mirror_delta.py'.format(bkt_intern=s3_bucket_intern)
    s3iroot     = 's3://{bkt}/emr-workspace-inp/'.format(bkt=s3_bucket) #Yes end '/'
    s3oroot     = 's3://{bkt}/emr-workspace-out/'.format(bkt=s3_bucket) #Yes end '/'
    steps       = []
    tables      = [
        'source.example_1',
        'source.example_2',
        'source.example_3',
    ]
    argd = {
        'my_iroot' : s3iroot, 
        'my_oroot' : s3oroot,
        'my_sloc'  : s3emr_script,
        'my_prev'  : prev_date,
        'my_curr'  : curr_date,
        'my_region': region,
    }
    for label in tables:
        schema, table = label.split('.')
        step = {
            'Name': 'Create Mirror Diff {} {}.{}'.format(curr_date, schema, table),
            'ActionOnFailure': 'CONTINUE',
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': [
                     "spark-submit",
                     "--master", "yarn", 
                     "--num-executors", "29",
                     "--executor-memory", "38g",
                     "--executor-cores", "5",
                     "--driver-memory", "5g",
                     "--deploy-mode", "cluster",
                     "{}".format(s3emr_script), 
                     "{}".format(s3iroot),
                     "{}".format(s3oroot),
                     "{}".format(schema),
                     "{}".format(table),
                     "--prev", "{}".format(prev_date),
                     "--curr", "{}".format(curr_date),
                     "--dup" , "{}".format(duproot),
                ]
            }
        }    
        steps += [step]
    r = demr.run( arg_dict=argd, steps = steps )
    pprint.pprint(r)

if __name__ == '__main__':
    main()
