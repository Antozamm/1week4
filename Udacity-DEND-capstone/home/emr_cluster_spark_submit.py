

import configparser
from lib import emr_cluster


if __name__ == "__main__":
    
    # Read in configurations parameters
    config_filepath = 'config.cfg'
    config = configparser.ConfigParser()
    config.read_file(open(config_filepath))
     
    # Instantiate a EMRLoader cluster class
    emrCluster = emr_cluster.EMRLoader(config, keepAlive=True)
    
    # define the 2 python files to run in the steps
    spark_script_1 = 'spark_4_emr_codes_extraction.py'
    spark_script_2 = 'spark_4_emr_I94_processing.py'
    
    # define the 2 steps to be executed in the EMR cluster
    # after its creation
    emrCluster.custom_steps = [
        {
            'Name': 'Run spark_script_1',
            'ActionOnFailure': 'CONTINUE',
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': [
                    'spark-submit',
                    '--deploy-mode', 'cluster', 
                    's3://helptheplanet/code/{}'.format(spark_script_1),
                ]
            }
        },
        {
            'Name': 'Run spark_script_2',
            'ActionOnFailure': 'CONTINUE',
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': [
                    'spark-submit',
                    '--jars', 'https://repo1.maven.org/maven2/com/epam/parso/2.0.8/parso-2.0.8.jar,https://repos.spark-packages.org/saurfang/spark-sas7bdat/2.0.0-s_2.11/spark-sas7bdat-2.0.0-s_2.11.jar',
                    '--deploy-mode', 'cluster',
                    's3://helptheplanet/code/{}'.format(spark_script_2),
                ]
            }
        }
    ]
    
    # Create the cluster, AND execute the steps
    emrCluster.create_cluster()