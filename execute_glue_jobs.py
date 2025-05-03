import configparser
import boto3
import os

def run_glue_job(job,glue_client):
    """
    This function will run a single Glue job.

    Args:
     job (string): name of the job for Glue to run
     glue_client (class): boto3 Glue client connection session
    """
    
    try:
        job_run_id = glue_client.start_job_run(JobName=job)["JobRunId"]
        print("Running " + job)
        while glue_client.get_job_run(JobName=job, RunId=job_run_id)["JobRun"]["JobRunState"] == "RUNNING":
            False
        if glue_client.get_job_run(JobName=job, RunId=job_run_id)["JobRun"]["JobRunState"] == "SUCCEEDED":
            print(job + " succeeded!")
        if glue_client.get_job_run(JobName=job, RunId=job_run_id)["JobRun"]["JobRunState"] == "FAILED":
            print(job + " failed... investigate logs in AWS Glue console")
    except Exception as e:
        print(e)


def run_glue_job_workflow(job_list,glue_client):
    """
    This function will loop through a sequence of glue jobs to simulate a Glue workflow.

    Args:
     job_list (list string): list of Glue jobs to execute in order
     glue_client (class): boto3 Glue client connection session
    """
    
    print("Running workflow...")
    
    for job in job_list:
        run_glue_job(job,glue_client)


def main():

    aws_creds_path = os.path.expanduser("~\\.aws\\credentials")
    aws_creds = configparser.ConfigParser()
    aws_creds.read(aws_creds_path)

    aws_config_path = os.path.expanduser("~\\.aws\\config")
    aws_config = configparser.ConfigParser()
    aws_config.read(aws_config_path)
    
    job_list = ["customer_landing_to_trusted", 
                "accelerometer_landing_to_trusted", 
                "customer_trusted_to_curated", 
                "step_trainer_trusted", 
                "machine_learning_curated"]

    glue_client = boto3.client("glue", 
                               aws_access_key_id=aws_creds.get("default","aws_access_key_id"),
                               aws_secret_access_key=aws_creds.get("default","aws_secret_access_key"),
                               region_name=aws_config.get("default","region")
                              )

    run_glue_job_workflow(job_list,glue_client)


if __name__ == "__main__":
    main()