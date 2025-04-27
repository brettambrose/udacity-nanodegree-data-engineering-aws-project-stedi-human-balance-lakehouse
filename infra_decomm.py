import configparser
import boto3
from botocore.exceptions import ClientError

config = configparser.ConfigParser()
config.read_file(open('lakehouse.cfg'))

# AWS CREDENTIALS
KEY           = config.get("AWS","KEY")
SECRET        = config.get("AWS", "SECRET")

# S3
S3_BUCKET_NAME   = config.get("S3","S3_BUCKET_NAME")

# GLUE
GLUE_IAM_ROLE_NAME  = config.get("GLUE","GLUE_IAM_ROLE_NAME")
GLUE_DB_NAME        = config.get("GLUE","GLUE_DB_NAME")

s3 = boto3.resource("s3",
                    region_name="us-east-1",
                    aws_access_key_id=KEY,
                    aws_secret_access_key=SECRET
                   )

s3_client = boto3.client("s3",
                         region_name="us-east-1",
                         aws_access_key_id=KEY,
                         aws_secret_access_key=SECRET
                        )

ec2_client = boto3.client("ec2",
                          region_name="us-east-1",
                          aws_access_key_id=KEY,
                          aws_secret_access_key=SECRET
                         )

iam_client = boto3.client("iam",
                          region_name="us-east-1",
                          aws_access_key_id=KEY,
                          aws_secret_access_key=SECRET
                         )


glue_client = boto3.client("glue",
                          region_name="us-east-1",
                          aws_access_key_id=KEY,
                          aws_secret_access_key=SECRET
                         )

# DELETE GLUE DATABASE

print("Deleting Glue database...")

try:
    glue_response = glue_client.delete_database(Name=GLUE_DB_NAME)
except Exception as e:
    print(e)

# DELETE S3 BUCKET

print("Deleting S3 bucket...")

try:
    s3_object_resp = s3.Bucket(S3_BUCKET_NAME).objects.all().delete()
except Exception as e:
    print(e)

try:
    s3_bucket_resp = s3_client.delete_bucket(Bucket=S3_BUCKET_NAME)
except Exception as e:
    print(e)

# DELETE IAM ROLE

print("Deleting IAM policies and role...")

try:
    iam_policy1_resp = iam_client.delete_role_policy(RoleName=GLUE_IAM_ROLE_NAME, PolicyName="GlueAccess")
except Exception as e:
    print(e)

try:
    iam_policy2_resp = iam_client.delete_role_policy(RoleName=GLUE_IAM_ROLE_NAME, PolicyName="S3Access")
except Exception as e:
    print(e)
    
try:
    iam_role_resp = iam_client.delete_role(RoleName=GLUE_IAM_ROLE_NAME)
except Exception as e:
    print(e)

# DELETE VPC ENDPOINTS

print("Deleting VPC Endpoints...")

try:
    vpc_endpoint_id = ec2_client.describe_vpc_endpoints()["VpcEndpoints"][0]["VpcEndpointId"]
    ec2_client.delete_vpc_endpoints(VpcEndpointIds=[vpc_endpoint_id])
except Exception as e:
    print(e)