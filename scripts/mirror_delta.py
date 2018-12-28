#!/usr/bin/env python
################################EMR Mirror Diff###############################################
################################Context#####################################################OK 
import pyspark, pyspark.sql, boto3, argparse
from   datetime      import datetime,  timedelta
from   pyspark       import SparkContext
from   pyspark.sql   import SparkSession
from   pyspark.sql.types import *
import logging
import StringIO ####
logging.basicConfig(filename='log.log',level=logging.DEBUG)

#from __future__ import print_function
#import sys

#def eprint(*args, **kwargs):
#    print(*args, file=sys.stderr, **kwargs)

s3      = boto3.resource('s3')
dtf     = '%Y%m%d' 
appName = 'Unload Delta Mirror'
#try:
spark   = SparkSession.builder.master("yarn").appName(appName).getOrCreate()
#except Exception as e:
#    print(e)
#    spark   = SparkSession.builder.master("local").appName(appName).getOrCreate()
sc      = SparkContext.getOrCreate()
################################Context#####################################################KO
################################Body########################################################OK
### Functions OK
def bucket_split(s3name):                                                                                                                                            
    s3name     = s3name[5:]
    decomposed = s3name.split('/')
    bucket     = decomposed.pop(0)
    prefix     = '/'.join(decomposed)
    prefix     = prefix + '/' if prefix[-1] != '/' else prefix
    return {'bucket': bucket, 'prefix': prefix}

def write_success_file(s3IncreLocation, count):
    s3 = boto3.resource('s3')
    count_str = str(count) + "\n"
    data = StringIO.StringIO(count_str)
    r = bucket_split(s3IncreLocation)
    s3.Bucket(r['bucket']).put_object(Key=r['prefix'] + '_SUCCESS', Body=data)
    print("Write SUCCESS_ {} to {}".format(count, s3IncreLocation))

def write_header_file(s3IncreLocation, s3OriginLocation):
    s3 = boto3.resource('s3')
    hname= '_HEADER_000'

    zsrc = bucket_split(s3OriginLocation)
    zdst = bucket_split(s3IncreLocation)
    copy_source = {
        'Bucket': zsrc['bucket'],
        'Key': zsrc['prefix'] + hname
    }
    s3.Bucket(zdst['bucket']).copy(copy_source, zdst['prefix'] + hname)
    print("Write HEADER from {} to {}".format(s3OriginLocation, s3IncreLocation))

def raw_hash(s):
    return "^".join([s, str(hash(s))])

def hash_and_dump(s3Input, s3Output):
    print("HD I<{}> O<{}>".format(s3Input, s3Output))
    rdd_input = sc.textFile(s3Input)
    try:
        ccc = "org.apache.hadoop.io.compress.GzipCodec"
        rdd_input.map(raw_hash).saveAsTextFile(s3Output, compressionCodecClass=ccc)
    except Exception as e:
        print(e)
        return False
    return True
    #return rdd_input.count()

def diff_hashed_dump(s3PrevHashedLocation, s3CurrHashedLocation, s3IncreLocation, duploc=None):
    print('Diff Hash Dump')

    schema  = StructType([StructField("row", StringType(), False), StructField("hash", LongType(), False)]) ####
    df_curr = spark.read.option("delimiter", "^").schema(schema).csv(s3CurrHashedLocation)
    df_prev = spark.read.option("delimiter", "^").schema(schema).csv(s3PrevHashedLocation)

    # If df_curr.count == df_prev.count then no need to unload
    df_diff = df_curr.select("hash").subtract(df_prev.select("hash")).cache()
    rcount  = df_diff.count()
    df_diff = df_diff.select(df_diff.hash.alias("hash_diff"))
    result  = df_curr.join(df_diff, df_curr.hash == df_diff.hash_diff).select(df_curr.row)
    result.write.mode("overwrite").option("compression", "gzip").text(s3IncreLocation)
    if(duploc):
	result.write.mode("overwrite").option("compression", "gzip").text(duploc)
    return rcount
### Functions KO

def unload_table_diff(s3iroot,
                      s3oroot,
                      schema,
                      table,
                      yesterday_str,
                      today_str,
		      duploc):
    ## Loc OK
    iloc          = s3iroot + '{date_str}/{schema_str}/{table_str}/'
    hloc          = s3iroot + '{date_str}H/{schema_str}/{table_str}/'
    oloc          = s3oroot + '{date_str}T/{schema_str}/{table_str}/'
    ## Loc KO
    ### Locations OK
    s3PrevLocation     = iloc.format(date_str=yesterday_str,
				 schema_str=schema,
				 table_str=table)
    s3PrevHashLocation = hloc.format(date_str=yesterday_str,
				 schema_str=schema,
				 table_str=table)
    s3CurrLocation     = iloc.format(date_str=today_str,
				 schema_str=schema,
				 table_str=table)
    s3CurrHashLocation = hloc.format(date_str=today_str,
				 schema_str=schema,
				 table_str=table)
    s3IncreLocation    = oloc.format(date_str=today_str,
				 schema_str=schema,
				 table_str=table)

    s3IncreLocation1   = '{duproot}/{schema_str}/{table_str}/{date_str}'.format(duproot=duploc,
										  schema_str=schema,
										  table_str=table,
										  date_str=today_str) ####
    ### Locations KO
    print("DELTA<{} AND {}> TO [{}]".format(s3PrevLocation, s3CurrLocation, s3IncreLocation))

    ## Delta and Dump. OK
    hash_and_dump(s3PrevLocation, s3PrevHashLocation)
    hash_and_dump(s3CurrLocation, s3CurrHashLocation)
    if(duploc):
        rcount = diff_hashed_dump(s3PrevHashLocation,
                                  s3CurrHashLocation,
                                  s3IncreLocation,
                                  duploc=s3IncreLocation1)
    print("HLOCS: {} AND {}".format(s3PrevHashLocation, s3CurrHashLocation))
    ## Delta and Dump. KO

    ### Success OK
    print('Num Rows: {}'.format(rcount) )
    write_success_file(s3IncreLocation, rcount)
    if(duploc):
        write_success_file(s3IncreLocation1, rcount)
    ### Success KO

    ### Copy _HEADER_000 file OK
    write_header_file(s3IncreLocation, s3CurrLocation)  ####
    if(duploc):
        write_header_file(s3IncreLocation1, s3CurrLocation) ####
    ### Copy _HEADER_000 file KO
    return rcount

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
    parser.add_argument('--prev',
                        dest='my_prev',
                        default=(datetime.utcnow().date() - timedelta(days=1)).strftime(dtf),
                        help='optional prev date.')
    parser.add_argument('--curr',
                        dest='my_curr',
                        default=(datetime.utcnow().date()).strftime(dtf),
                        help='optional curr date.')
    parser.add_argument('--dup',
                        dest='my_dup',
                        default=None,
                        help='optional second place to put diff.')

    args = parser.parse_args()
    return vars(args)

def main(args):
    unload_table_diff(*args)

if __name__ == '__main__':
    arg_d = cli() 
    args  = (arg_d['my_iroot' ],
             arg_d['my_oroot' ],
             arg_d['my_schema'],
             arg_d['my_table' ],
             arg_d['my_prev'  ],
             arg_d['my_curr'  ],
             arg_d['my_dup'  ])
    main(args)
################################Body########################################################KO
