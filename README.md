# Project: STEDI Human Balance Analytics
Building a data lakehouse solution for sensor data that trains a machine learning model.

## Table of Contents
1. [AWS Prerequisites](#aws-prerequsites)
2. [Project Details](#project-details)
3. [Project Summary](#project-summary)
4. [Project Environment](#project-environment)
    1. [AWS Environment](#aws-environment)
    2. [Github Environment](#github-environment)
    3. [Workflow Environment](#workflow-environment-configuration)
5. [Project Datasets](#project-datasets)
6. [Project Requirements](#project-requirements)
    1. [TO DO: Filter Reading Prior to Research Consent Date](#to-do-filter-reading-prior-to-research-consent-date)
    2. [TO DO: Anonymize Data](#to-do-anonymize-data)
7. [Insfrastructure Setup](#infrastructure-setup)
    1. [Option 1 - Using Infrastructure as Code](#option-1---using-infrastructure-as-code)
    2. [Option 2 - Manual via AWS Console](#option-2---manual-via-aws-console)
        1. [Create S3 Bucket](#create-s3-bucket)
        2. [Upload the Data Files into S3](#upload-the-data-files-into-s3)
        3. [Create S3 Directory for Query Results](#create-s3-directory-for-query-results)
        4. [Describe Available VPCs](#describe-available-vpcs)
        5. [Describe Available Route Tables](#describe-available-route-tables)
        6. [Create VPC Gateway with S3 Gateway Endpoint](#create-vpc-gateway-with-s3-gateway-endpoint)
        7. [Create Glue Service Role](#create-glue-service-role)
        8. [Grant Glue Privileges on S3 Bucket](#grant-glue-privileges-on-s3-bucket)
        9. [Attach Glue Policy](#attach-glue-policy)
8. [Job Execution Steps](#job-execution-steps)
9. [Validation](#validation)
10. [Infrastructure Decommission](#infrastructure-decommission)

## AWS PREREQUSITES
1. An AWS Account with an IAM User with the following permissions
    - AdminstratorAccess
    - AmazonRedshiftFullAccess
    - AmazonS3FullAccess
2. Local AWS CLI (download here: https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html)
    - Local .aws file will need to be configured (see: https://docs.aws.amazon.com/cli/v1/userguide/cli-configure-files.html#cli-configure-files-methods)

## Project Details
The STEDI Team has been hard at work developing a hardware STEDI Step Trainer that:
* trains the user to do STEDI balance
* and has sensors on the device that collect data to train a machine-learning algorithm to detect steps;
* has a companion mobile app that collects customer data and interacts with the device sensors.

STEDI has heard from millions of early adopters who are willing to purchase the STEDI Step Trainers and use them.

Several customers have already received their Step Trainers, installed the mobile application, and begun using them together to test their balance. The Step Trainer is just a motion sensor that records the distance of the object detected. The app uses a mobile phone accelerometer to detect motion in the X, Y, and Z directions.

The STEDI team wants to use the motion sensor data to train a machine learning model to detect steps accurately in real-time. Privacy will be a primary consideration in deciding what data can be used.

Some of the early adopters have agreed to share their data for research purposes. Only these customers’ Step Trainer and accelerometer data should be used in the training data for the machine learning model.

## Project Summary
Extract the data produced by the STEDI Step Trainer sensors and the mobile app, and curate them into a data lakehouse solution on AWS so that Data Scientists can train the learning model.

## Project Environment

### AWS Environment

Data from the STEDI Step Trainer and mobile app will be used to develop a lakehouse solution in the cloud that curates the data for the machine learning model using:

* Python and Spark
* AWS Glue
* AWS Athena
* AWS S3

### Github Environment

This github repository will store the SQL scripts and Python code used in the project.

### Workflow Environment Configuration

Python scripts are created using AWS Glue and Glue Studio. These web-based tools and services contain multiple options for editors to write or generate Python code that uses PySpark. The code used in these editors will be saved into this repository.

## Project Datasets

**STEDI** has three [JSON data sources](https://github.com/udacity/nd027-Data-Engineering-Data-Lakes-AWS-Exercises/tree/main/project/starter) to use from the Step Trainer
* customer
* step_trainer
* accelerometer

To download this data...
1. Go to the [nd027-Data-Engineering-Data-Lakes-AWS-Exercises](https://github.com/udacity/nd027-Data-Engineering-Data-Lakes-AWS-Exercises/tree/main) repository and Download ZIP.

![ZIP Download Istructions](/images/2025-04-26%2011_47_13-udacity_nd027-Data-Engineering-Data-Lakes-AWS-Exercises_%20Exercise%20starter%20and%20so.png)

2. Extract the zip file
3. Navigate to **project/starter** in the extracted repo to find the JSON data files within three sub-fulders, which includes:
    * 956 rows in the customer_landing table,
    * 81273 rows in the accelerometer_landing table, and
    * 28680 rows in the step_trainer_landing table


**1 - Customer Records**

This is the data from fulfillment and the STEDI website.

*AWS S3 Bucket URI - s3://cd0030bucket/customers/*

Contains the following fields:
* serialnumber
* sharewithpublicasofdate
* birthday
* registrationdate
* sharewithresearchasofdate
* customername
* email
* lastupdatedate
* phone
* sharewithfriendsasofdate

**2 - Step Trainer Records**

This is the data from the motion sensor.

*AWS S3 Bucket URI - s3://cd0030bucket/step_trainer/*

Contains the following fields: 
* sensorReadingTime
* serialNumber
* distanceFromObject

**3 - Accelerometer Records**

This is data from the mobile app.

*AWS S3 Bucket URI - s3://cd0030bucket/accelerometer/*

Contains the following fields:
timeStamp
* user
* x
* y
* z

## Project Requirements

![Wofklow Diagram](/images/2025-04-26%2012_00_44-STEDI%20Human%20Balance%20Analytics%20-%20Project%20Instructions.png)

To simulate data coming from the various sources, create S3 directories for:
* customer_landing
* step_trainer_landing
* accelerometer_landing

Create **three Glue tables** for each of the three landing zones above.

Query those tables using **Athena** and take a screenshot of each table's result set, save each screenshot as:
* customer_landing(.png,.jpeg, etc.)
* step_trainer_landing(.png,.jpeg, etc.)
* accelerometer_landing(.png,.jpeg, etc.)

Preliminary analysis by the Data Science team determined that the **Accelerometer Records** each match one of the **Customer Records**.

Create two AWS Glue Jobs that do the following:
1. Sanitize the Customer data from the Website (Landing Zone) and only store the Customer Records who agreed to share their data for research purposes (Trusted Zone) - creating a Glue Table called **customer_trusted**.
2. Sanitize the Accelerometer data from the Mobile App (Landing Zone) - and only store Accelerometer Readings from customers who agreed to share their data for research purposes (Trusted Zone) - creating a Glue Table called **accelerometer_trusted**.
3. Validate the Glue job is successful and only contains Customer Records from people who agreed to share their data. 
4. Query the Glue customer_trusted table with Athena and take a screenshot of the data. Name the screenshot customer_trusted(.png,.jpeg, etc.).

The Data Science team also discovered data quality issues with the Customer Data.  Each STEDI Step Trainer purchased by a customer has a unique **serial number**.  However, a defect on the fulfillment website resulted in the same 30 serial numbers being assigned to millions of customers. Only a small subset of customers have received their Step Trainers so far, but they are submitting Step Trainer data over the IoT Network (Landing Zone).  Fortunately, the Step Trainer records contain the correct serial numbers.  The challenge is now associating customers to Step Trainers.

To remediate the data quality issue:

1. **Write a Glue job** that sanitizes the Customer data (Trusted Zone) and creates a Glue Table (Curated Zone) that only includes customers who have accelerometer data and have agreed to share their data for research called **customers_curated.**

Finally, write **two Glue jobs** that do the following tasks:

1. Read the Step Trainer IoT data stream (S3) and populate a Trusted Zone Glue Table called **step_trainer_trusted** that contains the Step Trainer Records data for customers who have accelerometer data and have agreed to share their data for research (customers_curated).

2. Create an aggregated table that has each of the Step Trainer Readings, and the associated accelerometer reading data for the same timestamp, but only for customers who have agreed to share their data, and make a glue table called **machine_learning_curated.**

See the ERD below to understand the desired state.

![Lakehouse ERD](/images/2025-04-26%2012_34_00-STEDI%20Human%20Balance%20Analytics%20-%20Project%20Instructions.png)

### TO DO: Filter Reading Prior to Research Consent Date
When creating the Glue Job to join data from the accelerometer readings and the customer table, filter out any readings that were prior to the research consent date. This will ensure consent was in place at the time that data was gathered. This helps in the case that in the future the customer revokes consent. We can be sure that the data we used for research was used when consent was in place for that particular data.

### TO DO: Anonymize Data
Anonymize the final curated table so that it is not subject to GDPR or other privacy regulations, in case a customer requests deletion of PII, we will not be in violation by retaining PII data --remove email, and any other personally identifying information up front.

## Testing and Validation Steps

For stage of developing the lakehouse, the following row counts should be in each table:

* Landing
    * Customer: 956
    * Accelerometer: 81273
    * Step Trainer: 28680
* Trusted
    * Customer: 482
    * Accelerometer: 40981
    * Step Trainer: 14460
* Curated
    * Customer: 482
    * Machine Learning: 43681

**HINT:** Use Transform - SQL Query nodes whenever possible.  Other node types my give unexpected results.  For example, rather than a Join node, use a SQL node that has two parents, then join them through a SQL query.

## Infrastructure Setup

### Option 1 - Using Infrastructure as Code

**NOTE:** The instructions in the [Option 2 - Manual via AWS Console](#option-2---manual-via-aws-console) section contains all manual steps to setup the infrastructure for this project. 

This can be skipped by running [infra_deploy.py](/deploy/infra_deploy.py) in the [deploy](/deploy/) folder  This script will:
1. Create an IAM Role that has permission to use the Redshift service on AWS
2. Attach the  S3 and Glue policies to the IAM Role
3. Create a VPC Endpoint for S3
4. Create the S3 bucket
5. Load project data from the [data](/data/) folder into S3
6. Create the Glue DB
7. Create the tables in the Glue DB

### Option 2 - Manual via AWS Console

#### Create S3 Bucket

Buckets are storage locations within AWS, that have a hierarchical directory-like structure. Once you create an S3 bucket, you can create as many sub-directories, and files as you want. The bucket is the "parent" of all of these directories and files.

Using the AWS CLI input the following command:

```aws s3 mb s3://stedi-lakehouse-ba```

#### Upload the data files into S3

All data that should be S3 to start this project can be found in the [data](/data/) folder.  Upload each of the following as their own directory (the files will upload with them) using the S3 upload feature in the stedi-lakehouse-ba bucket.

![S3 Bucket Upload](/images/2025-04-27%2016_46_34-stedi-lakehouse-ba%20-%20S3%20bucket%20_%20S3%20_%20us-east-1.png)

#### Create S3 directory for query results

A separate S3 folder is required for Athena query results.  Use the Create Folder feature in S3 in the stedi-lakehouse-ba bucket to do create this.

![S3 Query Result Folder Create](/images/2025-04-27%2016_49_15-stedi-lakehouse-ba%20-%20S3%20bucket%20_%20S3%20_%20us-east-1.png)

#### Describe Available VPCs

This cloud project runs resources within a Virtual Private Cloud (VPC).  Glue jobs will run in a secure zone without access to anything outside the virtual network.

Using the AWS CLI input the following command:

```aws ec2 describe-vpcs```

This should return something like this:

<pre>{
    "Vpcs": [
        {
            "OwnerId": "261621541097",
            "InstanceTenancy": "default",
            "CidrBlockAssociationSet": [
                {
            "CidrBlockAssociationSet": [
                {
                {
                {
                    "AssociationId": "vpc-cidr-assoc-089fbaac9d220fbf5",
                    "CidrBlock": "172.31.0.0/16",
                    "CidrBlockState": {
                        "State": "associated"
                    }
                }
            ],
            "IsDefault": true,
            "BlockPublicAccessStates": {
                "InternetGatewayBlockMode": "off"
            },
            "VpcId": "vpc-0211404bc2a15d3e7",
            "State": "available",
            "CidrBlock": "172.31.0.0/16",
            "DhcpOptionsId": "dopt-0ca67292028043518"
        }
    ]
}</pre>


#### Describe Available Route Tables
A routing table is an entity that stores the network paths to various locations.  In this project, the routing table will store the path to S3 from within the VPC.  A routing table is required to configure with the VPC Gateway used in this project.

In the AWS CLI input the following command:

```aws ec2 describe-route-tables```

This should return something like this:

<pre>
{
    "RouteTables": [
        {
            "Associations": [
                {
                    "Main": true,
                    "RouteTableAssociationId": "rtbassoc-0cdf19a5a21f95cf3",
                    "RouteTableId": "rtb-0faab5ee2c9e05110",
                    "AssociationState": {
                        "State": "associated"
                    }
                }
            ],
            "PropagatingVgws": [],
            "RouteTableId": "rtb-0faab5ee2c9e05110",
            "Routes": [
                {
                    "DestinationCidrBlock": "172.31.0.0/16",
                    "GatewayId": "local",
                    "Origin": "CreateRouteTable",
                    "State": "active"
                },
                {
                    "DestinationCidrBlock": "0.0.0.0/0",
                    "GatewayId": "igw-0720c7075dadd2fdf",
                    "Origin": "CreateRoute",
                    "State": "active"
                }
            ],
            "Tags": [],
            "VpcId": "vpc-0211404bc2a15d3e7",
            "OwnerId": "261621541097"
        }
    ]
}</pre>

#### Create VPC Gateway with S3 Gateway Endpoint

A VPC Gateway is a network entity that gives access to outside networks and resources. S3 will not initially reside in the available VPC.  An S3 Gateway Endpoint will be required to establish a secure connection between the VPC and S3. Once the endpoint is created, the Glue jobs (or any other resource within the VPC) will have a network path to reach S3.

Two steps:
1. Using the **VpcId** retrieved in the [Describe Available VPCs](#describe-available-vpcs) step and the **RouteTableId** [Describe Available Route Tables](#describe-available-route-tables) step,
2. Input the following command in the AWS CLI:

```aws ec2 create-vpc-endpoint --vpc-id vpc-0211404bc2a15d3e7 --service-name com.amazonaws.us-east-1.s3 --route-table-ids rtb-0faab5ee2c9e05110```

This should return something like this:

<pre>{
    "VpcEndpoint": {
        "VpcEndpointId": "vpce-0336baba747e61d9b",
        "VpcEndpointType": "Gateway",
        "VpcId": "vpc-0211404bc2a15d3e7",
        "ServiceName": "com.amazonaws.us-east-1.s3",
        "State": "available",
        "PolicyDocument": "{\"Version\":\"2008-10-17\",\"Statement\":[{\"Effect\":\"Allow\",\"Principal\":\"*\",\"Action\":\"*\",\"Resource\":\"*\"}]}",
        "RouteTableIds": [
            "rtb-0faab5ee2c9e05110"
        ],
        "SubnetIds": [],
        "Groups": [],
        "PrivateDnsEnabled": false,
        "RequesterManaged": false,
        "NetworkInterfaceIds": [],
        "DnsEntries": [],
        "CreationTimestamp": "2025-04-26T17:26:38+00:00",
        "OwnerId": "261621541097"
    }</pre>


#### Create Glue Service Role

AWS uses Identity and Access Management (IAM) service to manage users, and roles (which can be reused by users and services). A Service Role in IAM is a Role that is used by an AWS Service to interact with cloud resources.

Using the AWS CLI input the following command (including the policy):


<pre>aws iam create-role --role-name my-glue-service-role --assume-role-policy-document '{
     "Version": "2012-10-17",
     "Statement": [
         {
             "Effect": "Allow",
             "Principal": {
                 "Service": "glue.amazonaws.com"
             },
             "Action": "sts:AssumeRole"
         }
     ]
 }'</pre>

 This will return something like this:
 
 <pre>{
    "Role": {
        "Path": "/",
        "RoleName": "my-glue-service-role",
        "RoleId": "AROATZ2O35DU272SHMC5Y",
        "Arn": "arn:aws:iam::261621541097:role/my-glue-service-role",
        "CreateDate": "2025-04-26T17:46:28+00:00",
        "AssumeRolePolicyDocument": {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {
                        "Service": "glue.amazonaws.com"
                    },
                    "Action": "sts:AssumeRole"
                }
            ]
        }
    }
}</pre>

#### Grant Glue Privileges on S3 Bucket

Using the AWS CLI input the following command (including the policy):

<pre>aws iam put-role-policy --role-name my-glue-service-role --policy-name S3Access --policy-document '{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "ListObjectsInBucket",
            "Effect": "Allow",
            "Action": [
                "s3:ListBucket"
            ],
            "Resource": [
                "arn:aws:s3:::stedi-lakehouse-ba"
            ]
        },
        {
            "Sid": "AllObjectActions",
            "Effect": "Allow",
            "Action": "s3:*Object",
            "Resource": [
                "arn:aws:s3:::stedi-lakehouse-ba/*"
            ]
        }
    ]
}'</pre>

#### Attach Glue Policy

<pre>aws iam put-role-policy --role-name my-glue-service-role --policy-name GlueAccess --policy-document '{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "glue:*",
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
                "cloudwatch:PutMetricData"
            ],
            "Resource": [
                "*"
            ]
        },
        {
            "Effect": "Allow",
            "Action": [
                "s3:CreateBucket",
                "s3:PutBucketPublicAccessBlock"
            ],
            "Resource": [
                "arn:aws:s3:::aws-glue-*"
            ]
        },
        {
            "Effect": "Allow",
            "Action": [
                "s3:GetObject",
                "s3:PutObject",
                "s3:DeleteObject"
            ],
            "Resource": [
                "arn:aws:s3:::aws-glue-*/*",
                "arn:aws:s3:::*/*aws-glue-*/*"
            ]
        },
        {
            "Effect": "Allow",
            "Action": [
                "s3:GetObject"
            ],
            "Resource": [
                "arn:aws:s3:::crawler-public*",
                "arn:aws:s3:::aws-glue-*"
            ]
        },
        {
            "Effect": "Allow",
            "Action": [
                "logs:CreateLogGroup",
                "logs:CreateLogStream",
                "logs:PutLogEvents",
                "logs:AssociateKmsKey"
            ],
            "Resource": [
                "arn:aws:logs:*:*:/aws-glue/*"
            ]
        },
        {
            "Effect": "Allow",
            "Action": [
                "ec2:CreateTags",
                "ec2:DeleteTags"
            ],
            "Condition": {
                "ForAllValues:StringEquals": {
                    "aws:TagKeys": [
                        "aws-glue-service-resource"
                    ]
                }
            },
            "Resource": [
                "arn:aws:ec2:*:*:network-interface/*",
                "arn:aws:ec2:*:*:security-group/*",
                "arn:aws:ec2:*:*:instance/*"
            ]
        }
    ]
}'</pre>


## Job Execution Steps

Each job (except for the first) is sequentially dependent and must be executed in order. The [execute_glue_jobs.py](/execute_glue_jobs.py) script uses boto3 to initiate a Glue client session to execute these jobs in sequence.

Job order sequence:

1. [customer_landing_to_trusted.py](/jobs/customer_landing_to_trusted.py)
2. [accelerometer_landing_to_trusted.py](/jobs/accelerometer_landing_to_trusted.py)
3. [customer_trusted_to_curated.py](/jobs/customer_trusted_to_curated.py)
4. [step_trainer_trusted.py](/jobs/step_trainer_trusted.py)
5. [machine_learning_curated.py](/jobs/machine_learning_curated.py)

The above scripts are found in the [jobs](/jobs/) folder and were "written" first using the AWS Glue Studio GUI then generated and downloaded into this library.  AWS Glue libs (see https://github.com/awslabs/aws-glue-libs/) are required to run these jobs locally.

## Validation
Once the jobs complete, validate expected record counts using the [validation_queries.sql](/validation/validation_queries.sql) script in AWS Athena.  In the [validation](/validation/) folder, there are reference images for validated result sets AND successful job runs using the AWS Glue job monitoring consoole.

[job_success](/validation/job_success/) - shows an image of successful run for each job.

[query_results](/validation/query_results/) - shows an image of query results for a few sample tables, as well as final validation of record counts for each table


## Infrastructure Decommission

Ensure the following steps are completed when it's time to decommission. [infra_decomm.py](/infra_decomm.py) can be executed to automate this, rather than doing so manually in the AWS console.

1. Delete the Glue database
2. Delete the S3 bucket along with its objects
3. Detach policies from the IAM role and delete the role itself
4. Delete the VPC endpoint