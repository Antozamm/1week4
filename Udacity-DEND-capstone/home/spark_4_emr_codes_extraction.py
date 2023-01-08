import pandas as pd
import re
import os
import configparser
import io
import boto3

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, lit, explode, split, regexp_extract, col, isnan, isnull, desc, when, sum, to_date, desc, regexp_replace
from pyspark.sql.types import IntegerType

import logging

# Logging to the terminal
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)

@udf
def match_section(filetext, pattern):
#     pattern = '(\/\* I94CIT & I94RES[^;]+)'
    match = re.search(pattern, filetext)
    return match.group(0)

@udf
def parse_country_name(line):
    pattern = "(?<=')([0-9A-Za-z ,\-()]+)(?=')"
    match = re.search(pattern, line)
    if match:
        return match.group(0)
    else:
        return ''

    

def extract_cit_res(df_label_wholetext):
    """
    extract the I94CIT & I94RES codes from the file I94_SAS_Labels_Descriptions.SAS
    Ex:
    This file
        240 =  'EAST TIMOR'
        692 =  'ECUADOR'
        576 =  'EL SALVADOR'
    produce a 2 columns Spark dataframe with
    |code|country          |
    |240 |EAST TIMOR       |
    |692 |ECUADOR          |
    |576 |EL SALVADOR      |
    """
    pattern = "(\/\* I94CIT & I94RES[^;]+)"

    df_I94CIT_RES = df_label_wholetext.withColumn('I94CIT&RES', match_section("value", lit(pattern))).select("I94CIT&RES")

    df_I94CIT_RES_multi_rows = df_I94CIT_RES.withColumn('I94CIT&RES2', explode(split("I94CIT&RES", "[\r\n]+"))).select('I94CIT&RES2')

    df_I94CIT_RES_multi_rows_filtered = df_I94CIT_RES_multi_rows.filter(df_I94CIT_RES_multi_rows['I94CIT&RES2'].rlike("([0-9]+ *\= *[0-9A-Za-z ',\-()]+)"))\
                                                                .withColumn('code', regexp_extract(df_I94CIT_RES_multi_rows['I94CIT&RES2'], "[0-9]+", 0))\
                                                                .withColumn('country', parse_country_name(col('I94CIT&RES2')))\
                                                                .drop("I94CIT&RES2")
    return df_I94CIT_RES_multi_rows_filtered


def extract_port(df_label_wholetext):
    """
    extract the I94PORT codes from the file I94_SAS_Labels_Descriptions.SAS
    Ex:
    This file
       'ALC'	=	'ALCAN, AK             '
       'ANC'	=	'ANCHORAGE, AK         '
       'BAR'	=	'BAKER AAF - BAKER ISLAND, AK'
    produce a 3 columns Spark dataframe with
    |code|city                     |state|
    |ALC |ALCAN                    |AK   |
    |ANC |ANCHORAGE                |AK   |
    |BAR |BAKER AAF - BAKER ISLAND |AK   | 
    """

    pattern = "(\/\* I94PORT[^;]+)"

    df_I94PORT = df_label_wholetext.withColumn('I94PORT', match_section("value", lit(pattern))).select("I94PORT")

    df_I94PORT_multi_rows = df_I94PORT.withColumn('I94PORT2', explode(split("I94PORT", "[\r\n]+"))).select('I94PORT2')
    
    df_I94PORT_multi_rows_filtered = df_I94PORT_multi_rows.filter(df_I94PORT_multi_rows['I94PORT2'].rlike("([0-9A-Z.' ]+\t*\=\t*[0-9A-Za-z \',\-()\/\.#&]*)"))\
                                                    .withColumn('code', regexp_extract(df_I94PORT_multi_rows['I94PORT2'], "(?<=')[0-9A-Z. ]+(?=')", 0))\
                                                    .withColumn('city_state', regexp_extract(col('I94PORT2'), "(?<=\t')([0-9A-Za-z ,\-()\/\.#&]+)(?=')", 0))\
                                                    .withColumn('city', split(col('city_state'), ',').getItem(0))\
                                                    .withColumn('state', split(col('city_state'), ',').getItem(1))\
                                                    .withColumn('state', regexp_replace(col('state'), ' *$', ''))\
                                                    .drop('I94PORT2')
    
    return df_I94PORT_multi_rows_filtered

def load_config():
    
    # load configurations
    s3 = boto3.client("s3")
    s3_object = s3.get_object(Bucket='helptheplanet', Key='code/config.cfg')
    body = s3_object['Body']
    config_string = body.read().decode("utf-8")
    config_buffer = io.StringIO(config_string)
    
    config = configparser.ConfigParser()
    config.read_file(config_buffer)
    
    return config

def main():
    """
    This code extract the codes from I94_SAS_Labels_Descriptions.SAS
    and save them into an S3 bucket
    """
    
#     S3_bucket_country_codes = 'S3bucket_temp/country_codes'
#     S3_bucket_port_codes    = 'S3bucket_temp/port_codes'
    
    config = load_config()
    
    #set S3 paths
    S3_BUCKET              = config['S3']['S3_bucket']
    S3_PROCESSED_DATA      = config['S3']['PROC_DATA_JSON']
    S3_RAW_DATA            = config['S3']['RAW_DATA']

    
    # instantiate a new sparkSession
    spark = SparkSession.builder.enableHiveSupport()\
                        .config("spark.jars.packages","org.apache.hadoop:hadoop-aws:2.7.0")\
                        .getOrCreate()
    
    # set credentials
    AWS_ACCESS_KEY_ID     = config['AWS']['AWS_ACCESS_KEY_ID']
    AWS_SECRET_ACCESS_KEY = config['AWS']['AWS_SECRET_ACCESS_KEY']
    hadoopConf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoopConf.set("fs.s3a.access.key", AWS_ACCESS_KEY_ID)
    hadoopConf.set("fs.s3a.secret.key", AWS_SECRET_ACCESS_KEY)
    
    
    # load file into dataframe
    df_label_wholetext = spark.read.text(os.path.join('s3a://', S3_BUCKET, S3_RAW_DATA, 'I94_SAS_Labels_Descriptions.SAS'), wholetext=True)
    
    #set paths 
    cityfile_path = 'country_codes/'
    S3_bucket_country_codes = os.path.join('s3://', S3_BUCKET, S3_PROCESSED_DATA, cityfile_path)
    
    # extract city mapping
    df_I94CIT_RES_multi_rows_filtered = extract_cit_res(df_label_wholetext)
    
    # save dataframe as json
    df_I94CIT_RES_multi_rows_filtered.write.mode("overwrite").format('json').save(S3_bucket_country_codes)
    
    #set paths
    portfile_path = 'port_codes/'
    S3_bucket_port_codes = os.path.join('s3://', S3_BUCKET, S3_PROCESSED_DATA, portfile_path )

    # extract port mapping
    df_I94PORT_multi_rows_filtered = extract_port(df_label_wholetext)
    
    # save dataframe as json file
    df_I94PORT_multi_rows_filtered.write.mode("overwrite").format('json').save(S3_bucket_port_codes)
    
    
if __name__ == "__main__":
    main()