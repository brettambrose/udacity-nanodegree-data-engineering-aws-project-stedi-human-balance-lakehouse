import json
import configparser
import os
import boto3
from botocore.exceptions import ClientError

config = configparser.ConfigParser()
config.read_file(open('lakehouse.cfg'))

# AWS CREDENTIALS
KEY           = config.get("AWS","KEY")
SECRET        = config.get("AWS", "SECRET")

# S3 BUCKET
BUCKET_NAME   = config.get("S3","BUCKET_NAME")

# GLUE IAM ROLE
GLUE_IAM_ROLE_NAME = config.get("GLUE","GLUE_IAM_ROLE_NAME")

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

ec2 = boto3.resource("ec2",
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

# GET VPC ID
vpc_id = ec2_client.describe_vpcs()["Vpcs"][0]["VpcId"]

# GET ROUTE TABLE
route_table_id = ec2_client.describe_route_tables()["RouteTables"][0]["RouteTableId"]

# CREATE VPC GATEWAY WITH S3 GATEWAY ENDPOINT
try:
    ec2_client.create_vpc_endpoint(
        VpcId=vpc_id,
        ServiceName="com.amazonaws.us-east-1.s3",
        RouteTableIds=[route_table_id]
    )
except Exception as e:
    print(e)

# CREATE S3 BUCKET
try:
    s3_client.create_bucket(
        Bucket="stedi-lakehouse-ba"
    )
except Exception as e:
    print(e)

# COPY DATA INTO S3 BUCKET
# Solution found: https://stackoverflow.com/questions/25380774/upload-a-directory-to-s3-with-boto/70841601#70841601
current_dir = os.getcwd() + "\\data"
data_dir = os.listdir(current_dir)

for path, dirs, files in os.walk(current_dir):
    for file in files:
        dest_path = path.replace(current_dir,"").replace(os.sep, "/")[1:]
        s3_file = f"{dest_path}/{file}".replace("//", "/")
        local_file = os.path.join(path, file)
        
        try:
            s3_client.upload_file(local_file, BUCKET_NAME, s3_file)
        except Exception as e:
            print(e)

# CREATE GLUE SERVICE ROLE

try:
    iam_client.create_role(
        Path='/',
        RoleName=GLUE_IAM_ROLE_NAME,
        AssumeRolePolicyDocument=json.dumps(
            {"Version": "2012-10-17",
             "Statement": [{"Effect": "Allow",
                            "Principal": {"Service": "glue.amazonaws.com"},
                            "Action": "sts:AssumeRole"}]})
    )
except Exception as e:
    print(e)

# ATTACH S3 ACCESS POLICY

try:
    iam_client.put_role_policy(
        RoleName=GLUE_IAM_ROLE_NAME,
        PolicyName="S3Access",
        PolicyDocument=json.dumps(
            {"Version": "2012-10-17",
             "Statement": [{"Sid": "ListObjectsInBucket",
                            "Effect": "Allow",
                            "Action": ["s3:ListBucket"],
                            "Resource": ["arn:aws:s3:::" + BUCKET_NAME]},
                           {"Sid": "AllObjectActions",
                            "Effect": "Allow",
                            "Action": ["s3:*Object"],
                            "Resource": ["arn:aws:s3:::" + BUCKET_NAME + "/*"]}]})
    )
except Exception as e:
    print(e)

# ATTACH GLUE POLICIES

try:
    iam_client.put_role_policy(
        RoleName="my-glue-service-role",
        PolicyName="GlueAccess",
        PolicyDocument=json.dumps(
            {"Version": "2012-10-17",
             "Statement": [{"Effect": "Allow",
                            "Action": ["glue:*",
                                       "s3:GetBucketLocation",
                                       "s3:ListBucket",
                                       "s3:ListAllMyBuckets",
                                       "s3:GetBucketAcl",
                                       "ec2:DescribeVpcEndpoints",
                                       "ec2:DescribeRouteTables",
                                       "ec2:CreateNetworkInterface",
                                       "ec2:DeleteNetworkInterface",
                                       "ec2:DescribeNetworkInterfaces",
                                       "ec2:DescribeSecurityGroups",
                                       "ec2:DescribeSubnets",
                                       "ec2:DescribeVpcAttribute",
                                       "iam:ListRolePolicies",
                                       "iam:GetRole",
                                       "iam:GetRolePolicy",
                                       "cloudwatch:PutMetricData"],
                            "Resource": ["*"]},
                           {"Effect": "Allow",
                            "Action": ["s3:CreateBucket",
                                       "s3:PutBucketPublicAccessBlock"],
                            "Resource": ["arn:aws:s3:::aws-glue-*"]},
                           {"Effect": "Allow",
                            "Action": ["s3:GetObject",
                                       "s3:PutObject",
                                       "s3:DeleteObject"],
                            "Resource": ["arn:aws:s3:::aws-glue-*/*",
                                         "arn:aws:s3:::*/*aws-glue-*/*"]},
                           {"Effect": "Allow",
                            "Action": ["s3:GetObject"],
                            "Resource": ["arn:aws:s3:::crawler-public*",
                                         "arn:aws:s3:::aws-glue-*"]},
                           {"Effect": "Allow",
                            "Action": ["logs:CreateLogGroup",
                                       "logs:CreateLogStream",
                                       "logs:PutLogEvents",
                                       "logs:AssociateKmsKey"],
                            "Resource": ["arn:aws:logs:*:*:/aws-glue/*"]},
                           {"Effect": "Allow",
                            "Action": ["ec2:CreateTags",
                                       "ec2:DeleteTags"],
                            "Condition": {"ForAllValues:StringEquals": {"aws:TagKeys": ["aws-glue-service-resource"]}},
                            "Resource": ["arn:aws:ec2:*:*:network-interface/*",
                                         "arn:aws:ec2:*:*:security-group/*",
                                         "arn:aws:ec2:*:*:instance/*"]}]})
    )
except Exception as e:
    print(e)

# DELETE OBJECTS, ENDPOINTS, AND ROLES
# TO DO: Make a separate script for this

"""
s3_client.delete_bucket(Bucket=BUCKET_NAME)
iam_client.delete_role(RoleName=GLUE_IAM_ROLE_NAME), 

"""