import re
import os
import time
import json
import pandas as pd

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, lit, explode, split, regexp_extract, col, isnan, isnull, desc, when, sum, to_date, desc, regexp_replace, count, to_timestamp
from pyspark.sql.types import IntegerType, TimestampType

import boto3
import configparser

#import custom module
#from lib import emr_cluster

import logging

# Logging to the terminal
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)

def load_config():
    
    import io
    
    # load configurations
    s3 = boto3.client("s3")
    s3_object = s3.get_object(Bucket='helptheplanet', Key='code/config.cfg')
    body = s3_object['Body']
    config_string = body.read().decode("utf-8")
    config_buffer = io.StringIO(config_string)
    
    return config_buffer

class sparkJob():

    def __init__(self):
        
        logger.info('Loading configuration')
        
        # parsing configuration parameters
        config = configparser.ConfigParser()

        config.read_file(load_config())

        self.S3_BUCKET         = config['S3']['S3_bucket']
        self.S3_LAKE_RAWDATA   = config['S3']['RAW_DATA']
        self.S3_PROC_DATA      = config['S3']['PROC_DATA']
        self.S3_PROC_DATA_JSON = config['S3']['PROC_DATA_JSON']
        self.S3_I94_DATA       = config['S3']['I94_DATA']

        self.S3_bucket_I94 = os.path.join('s3a://', self.S3_BUCKET, self.S3_PROC_DATA, self.S3_I94_DATA)

        self.AWS_ACCESS_KEY_ID     = config['AWS']['AWS_ACCESS_KEY_ID']
        self.AWS_SECRET_ACCESS_KEY = config['AWS']['AWS_SECRET_ACCESS_KEY']

    
    def new_sparkSession(self):
        """
        instantiate a new sparkSession, loading the package needed (parso, spark.sas, hadoop-aws)
        configure for s3a access
        """
        logger.info('Instantiating new sparkSession')
        
        self.spark = SparkSession.builder\
            .config("spark.jars.packages","org.apache.hadoop:hadoop-aws:2.7.0")\
            .enableHiveSupport()\
            .getOrCreate()
        
        hadoopConf = self.spark.sparkContext._jsc.hadoopConfiguration()
        hadoopConf.set("fs.s3a.access.key", self.AWS_ACCESS_KEY_ID)
        hadoopConf.set("fs.s3a.secret.key", self.AWS_SECRET_ACCESS_KEY)
        
        
    def save_query(self, query_name, filename, first_day, last_day, query_description=''):
        
        
        logger.info('Saving query results')
        
        from datetime import datetime
        
        if os.path.isfile(filename):
            with open(filename) as f:
                query_results = json.load(f)
        else:
            query_results = {}
        
        query_name = query_name+'-'+str(int(datetime.timestamp(datetime.now())*1e6))
        query_results[query_name] = []
        
        query_results[query_name].append({
            'name': query_name,
            'description': query_description,
            'start_date': first_day,
            'end_date': last_day,
            'third_dimension': self.z,
            'x': self.x,
            'y': self.y
        })

        with open(filename, 'w') as outfile:
                json.dump(query_results, outfile)
      
                
    def uploadToS3(self, filename):
        '''
        upload query_result file to S3
        '''
        
        logger.info('Uploading to the S3 bucket')
        s3 = boto3.resource('s3',
                    region_name="us-west-2",
                    aws_access_key_id     = self.AWS_ACCESS_KEY_ID,
                    aws_secret_access_key = self.AWS_SECRET_ACCESS_KEY)

        s3.meta.client.upload_file(filename, self.S3_BUCKET, os.path.join('queries', filename))     
        
        os.remove(filename)


class sparkQuery(sparkJob):
    
    def query0(self, query_name, first_day, last_day):

        logger.info(f'Executing the query: {query_name}')

        df_I94 = self.spark.read.format("parquet").load(self.S3_bucket_I94).where((col('arrdate')>=first_day) & (col('arrdate')<=last_day))

        df_query = df_I94.groupBy('arrdate').sum('count').orderBy('arrdate').toPandas()
        
        self.x = df_query['arrdate'].apply(lambda x: time.mktime(x.timetuple())).values.tolist() #modify: date to timestamp to save in json file
        self.y = df_query['sum(count)'].values.tolist()
        self.z = ''
    
   
    def query1(self, query_name, first_day, last_day):
        
        logger.info(f'Executing the query: {query_name}')
        
        df_I94 = self.spark.read.format("parquet").load(self.S3_bucket_I94).where((col('arrdate')>=first_day) & (col('arrdate')<=last_day))
        
        retain_columns = ['i94port', 'arrdate','count']
        df_I94_port_vs_date = df_I94.select(retain_columns).groupBy('i94port').pivot('arrdate').sum('count')
        df_I94_port_vs_date = df_I94_port_vs_date.na.fill(0)
        
        self.x = df_I94_port_vs_date.toPandas().values.tolist()
        self.y = ''
        self.z = ''


if __name__ == "__main__":
    
    query_results_filename = 'query_results.json'
    
    #query0
    first_day = '2016-04-30'
    last_day  = '2016-04-30'
    query_name = 'query_0'
    query_description = 'How are distributed the arrivals in USA, in a given period?'
    emrQuery = sparkQuery()
    emrQuery.new_sparkSession()
    emrQuery.query0(query_name, first_day, last_day)
    emrQuery.save_query(query_name, query_results_filename, first_day, last_day, query_description)
    
    #query1
    first_day = '2016-04-30'
    last_day  = '2016-04-30'
    query_name = 'query_1'
    query_description = 'For each port, in a given period, how many arrivals by day there are?'
    emrQuery.query1(query_name, first_day, last_day)
    emrQuery.save_query(query_name, query_results_filename, first_day, last_day, query_description)
    
    #uploading results file to S3
    emrQuery.uploadToS3(query_results_filename)
    