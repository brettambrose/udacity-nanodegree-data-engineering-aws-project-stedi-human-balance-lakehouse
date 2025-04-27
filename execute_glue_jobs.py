import configparser
import boto3

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
    config = configparser.ConfigParser()
    config.read_file(open('lakehouse.cfg'))
    
    job_list = ["customer_landing_to_trusted", 
                "accelerometer_landing_to_trusted", 
                "customer_trusted_to_curated", 
                "step_trainer_trusted", 
                "machine_learning_curated"]

    glue_client = boto3.client("glue", 
                               region_name="us-east-1",
                               aws_access_key_id=config.get("AWS","KEY"),
                               aws_secret_access_key=config.get("AWS", "SECRET")
                              )

    run_glue_job_workflow(job_list,glue_client)


if __name__ == "__main__":
    main()