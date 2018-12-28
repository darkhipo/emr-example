#!/usr/bin/env python3
import os, sys, argparse, pprint, calendar, boto, boto3, boto.emr
from boto.emr.instance_group        import InstanceGroup
from datetime                       import datetime, date, timedelta
from calendar                       import timegm
from lib                            import s3_bucket, s3_bucket_intern

brsloc = "s3n://{bkt}/emr-scripts/bootstrap-emr-unloads.sh".format(bkt=s3_bucket_intern)
floc   = os.path.dirname(os.path.abspath(__file__))
ploc   = os.path.abspath(os.path.join(floc, "..", ".."))
dtf    = "%Y%m%d"

def cli():
    parser = argparse.ArgumentParser(description='Create EMR cluster on AWS. Launch incremental update task on it.')
    parser.add_argument('my_iroot',
                        help='s3 root of the staging area.')
    parser.add_argument('my_oroot',
                        help='s3 location where data is dumped.')
    parser.add_argument('my_schema',
                        help='schema of table to dump.')
    parser.add_argument('my_table',
                        help='dump table name..')
    parser.add_argument('my_sloc',
                        help='s3 path to script for emr to exec.')
    parser.add_argument('--prev',
                        dest='my_prev',
                        default=(datetime.utcnow().date() - timedelta(days=1)).strftime(dtf),
                        help='optional prev date.')
    parser.add_argument('--curr',
                        dest='my_curr',
                        default=(datetime.utcnow().date()).strftime(dtf),
                        help='optional curr date.')
    parser.add_argument('--region',
                        dest='my_region',
                        default='us-west-2',
                        help='AWS region name.')
    args = parser.parse_args()
    return vars(args)

def run(arg_dict=None, steps=None):
    client = boto3.client('emr', region_name=arg_dict['my_region'])
    if(not steps):
        steps=[{
                'Name': 'Unload-Mirror-Diff-Load-{} {}.{}'.format(arg_dict['my_curr'], 
                                                                  arg_dict['my_schema'],
                                                                  arg_dict['my_table']),
                'ActionOnFailure': 'CONTINUE',
                'HadoopJarStep': {
                    'Jar': 'command-runner.jar',
                    'Args': [
                         "spark-submit",
                         "--master", "yarn",
                         "--num-executors", "32", "--executor-memory", "64g", "--executor-cores", "8", "--driver-memory", "8g", 
                         "--deploy-mode", "cluster",
                         "{s3loc}".format(s3loc=arg_dict['my_sloc']),
                         "{iroot}".format(iroot=arg_dict['my_iroot']),
                         "{oroot}".format(oroot=arg_dict['my_oroot']),
                         "{schema}".format(schema=arg_dict['my_schema']),
                         "{table}".format(table=arg_dict['my_table']),
                         "--prev", "{prev}".format(prev=arg_dict['my_prev']),
                         "--curr", "{curr}".format(curr=arg_dict['my_curr'])
                    ]
                }
        }]
    pprint.pprint(steps)
    cluster_id = client.run_job_flow(Name='FullUnload-Mirror-Delta-IncrUpload {}'.format(arg_dict['my_curr']),
            BootstrapActions = [
                {
                    'Name': 'bootstrap',
                    'ScriptBootstrapAction': {
                        'Path': brsloc,
                        'Args': [  ]
                    }
                },
            ],
            ReleaseLabel      = 'emr-5.12.0',
            ServiceRole       = 'stx-apollo-pr-emr-services-role',
            JobFlowRole       = 'stx-apollo-pr-ec2-emr-services-role', 
            LogUri            = 's3://{b_intern}/emr-job-logs/'.format(b_intern=s3_bucket_intern),
            VisibleToAllUsers = True,
            Steps             = steps,
            Tags=[
                {
                    'Key'  : "Purpose",
                    'Value': "Create incremental unloads from consecutive full unloads. Dump to SBUX rendezvous."
                }
            ],
            Applications=[
                {
                    'Name':'Spark'
                },
                {
                    'Name': 'Hadoop'
                },
#                {
#                    'Name': 'Hive'
#                },
#                {
#                    'Name': 'Tez'
#                },
#                {
#                    'Name': 'Zeppelin'
#                },
#                {
#                    'Name': 'Hue'
#                },
#                {
#                    'Name': 'Ganglia'
#                }
            ],
            Configurations=[
#                {
#                    "Classification": "spark-env",
#                    "Properties": {},
#                    "Configurations": [
#                        {
#                            "Classification": "export",
#                            "Properties": {
#                                "PYSPARK_PYTHON": "python34"
#                            },
#                            "Configurations": []
#                        }
#                    ]
#                },
                {
                    'Classification':'emrfs-site',
                    "Properties": {
                        "fs.s3.enableServerSideEncryption":  'true',
                        "fs.s3a.enableServerSideEncryption": 'true',
                        "fs.s3n.enableServerSideEncryption": 'true'
                    },
                    'Configurations': []
                },
                {
                    'Classification':'spark-log4j',
                    "Properties": {
                        "log4j.rootCategory": "WARN, console"
                    },
                    'Configurations': []
                },
                {
                    'Classification': 'spark',
                    'Properties': {
                        'maximizeResourceAllocation': 'true',
                    }
                },
            ],
            #### X
            Instances={
                'KeepJobFlowAliveWhenNoSteps': False, ####
                'TerminationProtected': False,
                'Ec2SubnetId': 'subnet-ff2920a6',
                'Ec2KeyName': "stx-apollo-pr-emr-keypair",
                'EmrManagedMasterSecurityGroup': "sg-03cbf064",
                'EmrManagedSlaveSecurityGroup': "sg-04cbf063",
                'ServiceAccessSecurityGroup': "sg-02cbf065",
                'InstanceGroups': [
                    {
                        'InstanceRole': 'MASTER',
                        'InstanceType': "r4.8xlarge",
                        'InstanceCount': 1,
                        'EbsConfiguration': {
                            "EbsBlockDeviceConfigs": [
                                {
                                    "VolumeSpecification": {
                                        "VolumeType": "gp2",
                                        "SizeInGB": 200 
                                    },
                                    "VolumesPerInstance": 1
                                }
                            ],
                            "EbsOptimized": True
                        }
                    },
                    {
                        'InstanceRole': "CORE",
                        'InstanceType': "r4.8xlarge",
                        'InstanceCount': 4,
                        'EbsConfiguration': {
                               "EbsBlockDeviceConfigs": [
                                   {
                                       "VolumeSpecification": {
                                           "VolumeType": "gp2",
                                           "SizeInGB": 1024
                                       },
                                       "VolumesPerInstance": 1
                                   },
                               ],
                               "EbsOptimized": True
                           }
                    }
                ]
            }
    )
    pprint.pprint(cluster_id)
    return cluster_id

if __name__ == '__main__':
    arg_dict = cli()
    run(arg_dict)
