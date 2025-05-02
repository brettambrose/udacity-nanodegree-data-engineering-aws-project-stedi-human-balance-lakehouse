import configparser
import boto3
import os

main_config = configparser.ConfigParser()
main_config.read("lakehouse.cfg")

aws_creds_path = os.path.expanduser("~\\.aws\\credentials")
aws_creds = configparser.ConfigParser()
aws_creds.read(aws_creds_path)

aws_config_path = os.path.expanduser("~\\.aws\\config")
aws_config = configparser.ConfigParser()
aws_config.read(aws_config_path)

# IAM
IAM_ROLE_NAME         = main_config.get("IAM_ROLE","IAM_ROLE_NAME")

# S3
S3_BUCKET_NAME        = main_config.get("S3","S3_BUCKET_NAME")

# DB
DB_NAME               = main_config.get("DB","DB_NAME")

# AWS CREDENTIALS & CONFIG
KEY                   = aws_creds.get("default", "aws_access_key_id")
SECRET                = aws_creds.get("default", "aws_secret_access_key")
REGION                = aws_config.get("default", "region")

print("**********************************************")
print("Establishing boto3 resources and clients...")


s3 = boto3.resource("s3",
                    aws_access_key_id=KEY,
                    aws_secret_access_key=SECRET,
                    region_name=REGION
                   )

s3_client = boto3.client("s3",
                         aws_access_key_id=KEY,
                         aws_secret_access_key=SECRET,
                         region_name=REGION
                        )

ec2_client = boto3.client("ec2",
                          aws_access_key_id=KEY,
                          aws_secret_access_key=SECRET,
                          region_name=REGION
                         )

iam_client = boto3.client("iam",
                          aws_access_key_id=KEY,
                          aws_secret_access_key=SECRET,
                          region_name=REGION
                         )


glue_client = boto3.client("glue",
                          aws_access_key_id=KEY,
                          aws_secret_access_key=SECRET,
                          region_name=REGION
                         )

print("**********************************************")
print("Deleting Glue database...")

try:
    glue_response = glue_client.delete_database(Name=DB_NAME)

except Exception as e:
    print(e)

print("**********************************************")
print("Deleting S3 bucket...")

try:
    s3_object_resp = s3.Bucket(S3_BUCKET_NAME).objects.all().delete()

except Exception as e:
    print(e)

try:
    s3_bucket_resp = s3_client.delete_bucket(Bucket=S3_BUCKET_NAME)

except Exception as e:
    print(e)

print("**********************************************")
print("Deleting IAM policies and role...")

try:
    iam_policy1_resp = iam_client.delete_role_policy(
        RoleName=IAM_ROLE_NAME,
        PolicyName="GlueAccess"
        )

except Exception as e:
    print(e)

try:
    iam_policy2_resp = iam_client.delete_role_policy(
        RoleName=IAM_ROLE_NAME,
        PolicyName="S3Access"
        )

except Exception as e:
    print(e)
    
try:
    iam_role_resp = iam_client.delete_role(RoleName=IAM_ROLE_NAME)

except Exception as e:
    print(e)

print("**********************************************")
print("Deleting VPC Endpoints...")

try:
    vpc_endpoint_id = ec2_client.describe_vpc_endpoints()["VpcEndpoints"][0]["VpcEndpointId"]
    ec2_client.delete_vpc_endpoints(VpcEndpointIds=[vpc_endpoint_id])

except Exception as e:
    print(e)