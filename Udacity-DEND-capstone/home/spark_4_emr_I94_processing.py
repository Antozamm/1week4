
import re
import os
import pandas as pd

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, lit, explode, split, regexp_extract, col, isnan, isnull, desc, when, sum, to_date, desc, regexp_replace, count
from pyspark.sql.types import IntegerType, TimestampType

import boto3
import configparser

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

@udf(TimestampType())
def to_timestamp_udf(x):
    try:
        return pd.to_timedelta(x, unit='D') + pd.Timestamp('1960-1-1')
    except:
        return pd.Timestamp('1900-1-1')
    
class EMRClean():
    
    def __init__(self, test_local=False):
        """
        parse configuration parameter from a configuration file
        set access key 
        """
        config = configparser.ConfigParser()
        config.read_file(load_config())

        self.KEY                   = config['AWS']['AWS_ACCESS_KEY_ID']
        self.SECRET                = config['AWS']['AWS_SECRET_ACCESS_KEY']
        self.S3_BUCKET             = config['S3']['S3_bucket']
        self.PROCESSED_DATA        = config['S3']['PROC_DATA']
        self.AWS_ACCESS_KEY_ID     = config['AWS']['AWS_ACCESS_KEY_ID']
        self.AWS_SECRET_ACCESS_KEY = config['AWS']['AWS_SECRET_ACCESS_KEY']
        self.RAW_DATA              = config['S3']['RAW_DATA']
        
        if test_local:
            self.I94_DATASET_PATH = '../../data/18-83510-I94-Data-2016/'
            self.I94_TEST_FILE    = '../../data/18-83510-I94-Data-2016/i94_apr16_sub.sas7bdat'
        else:
            self.I94_DATASET_PATH = os.path.join('s3a://', self.S3_BUCKET, self.RAW_DATA, '18-83510-I94-Data-2016/')
            self.I94_TEST_FILE    = os.path.join('s3a://', self.S3_BUCKET, self.RAW_DATA, '18-83510-I94-Data-2016/i94_apr16_sub.sas7bdat')

        logger.info('Initialization')

        
                         
    def new_sparkSession(self):
        """
        instantiate a new sparkSession, loading the package needed (parso, spark.sas, hadoop-aws)
        configure for s3a access
        """
        self.spark = SparkSession.builder\
            .config("spark.jars","https://repo1.maven.org/maven2/com/epam/parso/2.0.8/parso-2.0.8.jar,https://repos.spark-packages.org/saurfang/spark-sas7bdat/2.0.0-s_2.11/spark-sas7bdat-2.0.0-s_2.11.jar")\
            .config("spark.jars.packages","org.apache.hadoop:hadoop-aws:2.7.0")\
            .enableHiveSupport()\
            .getOrCreate()
        
        hadoopConf = self.spark.sparkContext._jsc.hadoopConfiguration()
        hadoopConf.set("fs.s3a.access.key", self.AWS_ACCESS_KEY_ID)
        hadoopConf.set("fs.s3a.secret.key", self.AWS_SECRET_ACCESS_KEY)
        logger.info('New sparkSession')



    def df_load(self, test_env=False):
        """
        load the dataframe
        test_env==True => only one test file is loaded
        
        test_env==False => 
                if load_files==None => the complete dataset is loaded
                otherwise only the files in load_files
        """
        
        # load single file, entire dataset or a list of files 
        if test_env:
            df_I94 = self.spark.read.format('com.github.saurfang.sas.spark').load(self.I94_TEST_FILE).persist()
        else:
            df_I94 = spark.read.format('com.github.saurfang.sas.spark').load('{}/*/*.*'.format(self.I94_DATASET_PATH)).persist()

        logger.info('Loading df')
        return df_I94

                         
    def df_clean_schema(self, df):
        """
        the schema applied by Spark use double for numerical type
        this function convert numerical columns to integer 
        """
        # modifying schema: double to integer
        toInt = udf(lambda x: int(x) if x!=None else x, IntegerType())
        df = df.select( [ toInt(colname).alias(colname) if coltype == 'double' else colname for colname, coltype in df_I94.dtypes])
        
        # modifying schema: string to date
        df = df.withColumn('dtadfile', to_date(col('dtadfile'), format='yyyyMMdd'))\
               .withColumn('dtadddto', to_date(col('dtaddto'), format='MMddyyyy'))
        
        df = df.withColumn('arrdate', to_date(to_timestamp_udf(col('arrdate'))))\
               .withColumn('depdate', to_date(to_timestamp_udf(col('depdate'))))


        logger.info('Cleaning schema')
        return df


    def df_clean_data(self, df):
        """
        clean the dataframe
        """
        # handle null, duplicate
        fill_value = False
        df = df.na.fill(fill_value)   #null
        df = df.drop_duplicates()     #duplicates
        logger.info('Cleaning data')
        return df


    def df_save(self, df):
        """
        save the dataframe in parquet format into the S3 bucket
        """
        
        S3_bucket_I94 = os.path.join('s3a://', self.S3_BUCKET, self.PROCESSED_DATA)
        
        df.write.format('parquet').mode('overwrite').partitionBy('i94yr', 'i94mon').save(S3_bucket_I94)
        logger.info('Save parquet')
        

        
if __name__ == "__main__":
    
    emrJob = EMRClean()
    
    emrJob.new_sparkSession()
    
    df_I94 = emrJob.df_load(test_env=1)
    
    df_I94 = emrJob.df_clean_schema(df_I94)
    
    df_I94 = emrJob.df_clean_data(df_I94)
    
    emrJob.df_save(df_I94)
    
    emrJob.spark.stop()
    