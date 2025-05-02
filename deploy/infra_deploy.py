import boto3
import json
import configparser
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
S3_OUTPUT_DIR         = main_config.get("S3","S3_OUTPUT_DIR")

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

iam_client = boto3.client('iam',
                          aws_access_key_id=KEY,
                          aws_secret_access_key=SECRET,
                          region_name=REGION
                         )

ec2 = boto3.resource('ec2',
                     aws_access_key_id=KEY,
                     aws_secret_access_key=SECRET,
                     region_name=REGION
                     )

ec2_client = boto3.client("ec2",
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
print("Creating VPC Endpoint...")

vpc_id = ec2_client.describe_vpcs()["Vpcs"][0]["VpcId"]
route_table_id = ec2_client.describe_route_tables()["RouteTables"][0]["RouteTableId"]

try:
    ec2_client.create_vpc_endpoint(
        VpcId=vpc_id,
        ServiceName="com.amazonaws.us-east-1.s3",
        RouteTableIds=[route_table_id]
    )

except Exception as e:
    print(e)

print("**********************************************")
print("Creating S3 Bucket...")

try:
    s3_client.create_bucket(
        Bucket=S3_BUCKET_NAME
    )

except Exception as e:
    print(e)

print("**********************************************")
print("Creating S3 folder for Athena query output...")

try:
    s3_client.put_object(
        Bucket=S3_BUCKET_NAME, 
        Key=S3_OUTPUT_DIR)
except Exception as e:
    print(e)


print("**********************************************")
print("Creating IAM Role")

try:
    dwhRole = iam_client.create_role(
        Path='/',
        RoleName=IAM_ROLE_NAME,
        AssumeRolePolicyDocument=json.dumps(
            {'Statement': [{'Action': 'sts:AssumeRole',
               'Effect': 'Allow',
               'Principal': {'Service': 'glue.amazonaws.com'}}],
             'Version': '2012-10-17'})
    )    
except Exception as e:
    print(e)

print("**********************************************")
print("Attaching policies to IAM Role")

try:
    iam_client.put_role_policy(
        RoleName=IAM_ROLE_NAME,
        PolicyName="S3Access",
        PolicyDocument=json.dumps(
            {"Version": "2012-10-17",
             "Statement": [{"Sid": "ListObjectsInBucket",
                            "Effect": "Allow",
                            "Action": ["s3:ListBucket"],
                            "Resource": ["arn:aws:s3:::" + S3_BUCKET_NAME]},
                           {"Sid": "AllObjectActions",
                            "Effect": "Allow",
                            "Action": ["s3:*Object"],
                            "Resource": ["arn:aws:s3:::" + S3_BUCKET_NAME + "/*"]}]})
    )

except Exception as e:
    print(e)

try:
    iam_client.put_role_policy(
        RoleName=IAM_ROLE_NAME,
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

print("**********************************************")
print("Updating local .aws/config file with Role ARN")

try:
    aws_config["profile Redshift"]
    print("Role ARN already exists in .aws/config")

    IAM_ROLE_ARN = aws_config.get("profile Redshift","role_arn")
    
except:
    try:
        role_arn = iam_client.get_role(RoleName=IAM_ROLE_NAME)['Role']['Arn']

        aws_config_override = configparser.ConfigParser()
        aws_config_override.read(aws_config_path)

        aws_config_override["profile Redshift"] = {"role_arn": role_arn}
        
        with open(aws_config_path, "w") as configfile:
            aws_config_override.write(configfile)
            
        print("Role ARN added to .aws/config ")

        aws_config.read(aws_config_path)

        IAM_ROLE_ARN = aws_config.get("profile Redshift","role_arn")

    except Exception as e:
        print(e)

print("**********************************************")
print("Creating Glue database...")

try:
    glue_client.create_database(
        DatabaseInput={"Name": DB_NAME}
    )

except Exception as e:
    print(e)