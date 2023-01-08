

import configparser
from lib import emr_cluster


if __name__ == "__main__":
    
    # Read in configurations parameters
    config_filepath = 'config.cfg'
    config = configparser.ConfigParser()
    config.read_file(open(config_filepath))
     
    # Instantiate a EMRLoader cluster class
    emrCluster = emr_cluster.EMRLoader(config, keepAlive=True)
    
    # define the python file to run
    spark_script_1 = 'spark_4_emr-Queries.py'
    
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
        }
    ]
    
    # Create the cluster, AND execute the steps
    emrCluster.create_cluster()