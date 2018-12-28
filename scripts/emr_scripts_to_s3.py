#!/usr/bin/env python
import os, boto3, pprint
floc = os.path.dirname(os.path.abspath(__file__))
from lib import s3_bucket, s3_bucket_intern, inc_datetime_format, region
def main():
    s3     = boto3.resource("s3")
    sfname = "mirror_delta.py" 
    sflpath= os.path.join(floc, sfname)
    bootnm = "bootstrap-emr.sh"
    boopath= os.path.join(floc, bootnm)
    bucket = s3_bucket_intern
    prefix = "emr-scripts/"
    with open(sflpath, 'rb') as data:
        r = s3.Bucket(bucket).put_object(Key=prefix + sfname, Body=data)
        pprint.pprint('write {}/{}'.format(bucket, prefix+sfname))
        pprint.pprint(r)
    with open(boopath, 'rb') as data:
        r = s3.Bucket(bucket).put_object(Key=prefix + bootnm, Body=data)
        pprint.pprint('write {}/{}'.format(bucket, prefix+bootnm))
        pprint.pprint(r)

if __name__ == '__main__':
    main()
