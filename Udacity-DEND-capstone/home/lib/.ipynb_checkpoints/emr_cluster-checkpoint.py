
import boto3
import botocore
import yaml
import time
import logging
import os

# Logging to the terminal
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)


class EMRLoader():

    def __init__(self, config, keepAlive=False):
        self.aws_access_key           = config['AWS']['AWS_ACCESS_KEY_ID']
        self.aws_secret_access_key    = config['AWS']['AWS_SECRET_ACCESS_KEY']
        self.region_name              = config['EMR']['region_name']
        self.cluster_name             = config['EMR']['cluster_name']
        self.instance_count           = config['EMR']['instance_count']
        self.master_instance_type     = config['EMR']['master_instance_type']
        self.slave_instance_type      = config['EMR']['slave_instance_type']
        self.key_name                 = config['EMR']['key_name']
        self.subnet_id                = config['EMR']['subnet_id']
        self.log_uri                  = config['EMR']['log_uri']
        self.release_label            = config['EMR']['release_label']
        self.script_bucket_name       = config['EMR']['script_bucket_name']
        self.bootstrap_path           = config['EMR']['bootstrap_path']
        self.cluster_bootstrap_script = config['EMR']['cluster_bootstrap_script']
        self.master_bootstrap_script  = config['EMR']['master_bootstrap_script']
        self.custom_steps             = []
        self.keepAlive                = keepAlive
        self.iam_role                 = config['IAM_ROLE']['ARN']

    def boto_client(self, service):
        """Provide credentials to boto3 client"""
        client = boto3.client(service,
                              aws_access_key_id=self.aws_access_key,
                              aws_secret_access_key=self.aws_secret_access_key,
                              region_name=self.region_name)
        return client


    def load_cluster(self):
        """Instantiate the boto3 client"""
        response = self.boto_client("emr").run_job_flow(
            Name=self.cluster_name,
            LogUri=self.log_uri,
            ReleaseLabel=self.release_label,
            Instances={
                'MasterInstanceType': self.master_instance_type,
                'SlaveInstanceType': self.slave_instance_type,
                'InstanceCount': int(self.instance_count),
                'KeepJobFlowAliveWhenNoSteps': self.keepAlive,
                'TerminationProtected': False,
                'Ec2KeyName': self.key_name,
                'Ec2SubnetId': self.subnet_id
            },
            Applications=[
                {'Name': 'Spark'}
            ],
            Configurations = [
                  {
                     "Classification": "spark-env",
                     "Configurations": [
                       {
                         "Classification": "export",
                         "Properties": {
                            "PYSPARK_PYTHON": "/usr/bin/python3"
                          }
                       }
                    ]
                  },
                  {
                    "Classification": "spark",
                    "Properties": {
                      "maximizeResourceAllocation": "true"
                    }
                  }
            ],
            BootstrapActions=[
                {
                    'Name': 'Install packages',
                    'ScriptBootstrapAction': {
                        'Path': 's3://{bucket_name}/{script_name}'.format(bucket_name=self.script_bucket_name,
                                                                          script_name=self.bootstrap_path+self.cluster_bootstrap_script)
                    }
                },
            ],
            VisibleToAllUsers=True,
            JobFlowRole='EMR_EC2_DefaultRole',
            ServiceRole='EMR_DefaultRole'
        )

        logger.info(response)
        return response

        
        
    def add_step(self, job_flow_id, master_dns):
        response = self.boto_client("emr").add_job_flow_steps(
            JobFlowId=job_flow_id,
            Steps=self.custom_steps
        )

        logger.info(response)
        return response


    def upload_to_s3(self):
        """Load bootstrap scripts to an S3 bucket. If the bucket doesnt exist,
        create it.
        """
        s3 = self.boto_client("s3")
        try:
            s3.head_bucket(Bucket=self.script_bucket_name)
        except:
            s3.create_bucket(Bucket=self.script_bucket_name)

        cluster_bootstrap_path = self.bootstrap_path + self.cluster_bootstrap_script
        master_bootstrap_path = self.bootstrap_path + self.master_bootstrap_script
        
        if self.cluster_bootstrap_script:
            s3.upload_file(cluster_bootstrap_path, self.script_bucket_name, self.cluster_bootstrap_script)
        if self.master_bootstrap_script:
            s3.upload_file(master_bootstrap_path, self.script_bucket_name, self.master_bootstrap_script)


    def create_cluster(self):
        """Run all the necessary steps to create the cluster and run all
        bootstrap operations.
        """

        # Upload bootstrap scripts to S3
#         if self.script_bucket_name:
#             self.upload_to_s3()

        # Launch the cluster
        self.emr_response = self.load_cluster()
        self.emr_client   = self.boto_client("emr")

        # Monitor cluster creation. Once it is up an running, run the master node bootstrap script
        while True:

            job_response = self.emr_client.describe_cluster(ClusterId=self.emr_response.get("JobFlowId"))

            # Monitoring the cluster creation job status every 10 seconds
            time.sleep(10)

            if job_response.get("Cluster").get("MasterPublicDnsName") is not None:
                master_dns = job_response.get("Cluster").get("MasterPublicDnsName")

            # Getting job status
            job_state = job_response.get("Cluster").get("Status").get("State")
            job_state_reason = job_response.get("Cluster").get("Status").get("StateChangeReason").get("Message")

            # Check if cluster was created succesfully
            if job_state == "WAITING":
                logger.info("Cluster was created")
                cluster_created = True
                break

            # Stop the process if cluster creation failed
            if job_state in ['TERMINATED_WITH_ERRORS']:
                logger.info("Cluster creation: {job_state} "
                              "with reason: {job_state_reason}".format(job_state=job_state, job_state_reason=job_state_reason))
                cluster_created = False
                break

        # If cluster creation was sucessful, run the master node bootstrap script
        if cluster_created:

            logger.info("Adding steps to the job flow")

            add_step_response = self.add_step(self.emr_response.get("JobFlowId"), master_dns)

            while True:
                list_steps_response = self.emr_client.list_steps(ClusterId=self.emr_response.get("JobFlowId"),
                                                            StepStates=["COMPLETED"])
                time.sleep(5)

                if len(list_steps_response.get("Steps")) == len(
                        add_step_response.get("StepIds")):  # make sure that all steps are completed
                    logger.info("STEPS executed - Cluster is shutting down\n")
#                     logger.info("Master DNS: {0}".format(master_dns))
                    self.emr_client.terminate_job_flows(JobFlowIds=[self.emr_response.get("JobFlowId"),])
                    break
                
                #check if step failed and the cluster was terminated then exit the program
                job_response = self.emr_client.describe_cluster(ClusterId=self.emr_response.get("JobFlowId"))
                job_state = job_response.get("Cluster").get("Status").get("State")
                if job_state in ['TERMINATED', 'TERMINATED_WITH_ERRORS']:
                    logger.info("CLUSTER TERMINATED\n")
                    break


# Example: python create_cluster.py /home/user/Documents/config.yml
if __name__ == "__main__":

    import configparser
    
    config = configparser.ConfigParser()

    config.read_file(open('config.cfg'))

    emrCluster = EMRLoader(config)
    
    emrCluster.create_cluster()