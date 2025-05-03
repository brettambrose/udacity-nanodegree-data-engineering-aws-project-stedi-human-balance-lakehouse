import boto3
import json
import configparser
import os

main_config_path = "lakehouse.cfg"
main_config = configparser.ConfigParser()
main_config.read(main_config_path)

aws_creds_path = os.path.expanduser("~\\.aws\\credentials")
aws_creds = configparser.ConfigParser()
aws_creds.read(aws_creds_path)

aws_config_path = os.path.expanduser("~\\.aws\\config")
aws_config = configparser.ConfigParser()
aws_config.read(aws_config_path)

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

s3_client = boto3.client("s3",
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
print("Loading data into S3....")

# Code attributable to: https://stackoverflow.com/questions/25380774/upload-a-directory-to-s3-with-boto/70841601#70841601

try:
    current_dir = os.getcwd() + "\\data"
    data_dir = os.listdir(current_dir)

    for path, dirs, files in os.walk(current_dir):
        for file in files:
            dest_path = path.replace(current_dir,"").replace(os.sep, "/")[1:]
            s3_file = f"{dest_path}/{file}".replace("//", "/")
            local_file = os.path.join(path, file)

            try:
                s3_client.upload_file(local_file, S3_BUCKET_NAME, s3_file)
            except Exception as e:
                print(e)

            print(s3_file + " loaded into S3")

except Exception as e:
    print(e)

print("**********************************************")
print("Creating Glue landing tables...")

schema_dir = os.getcwd() + "\\schema"

for file in os.listdir(schema_dir):
    try:
        table_name = str(file).replace(".json","")
        s3_subdir = table_name.replace("_landing","")
        json_file = schema_dir + "\\" + file
        
        with open(json_file, "r") as schema:
            schema_json = json.load(schema)
            
            glue_client.create_table(
                DatabaseName=DB_NAME,
                TableInput={
                    "Name": table_name,
                    "StorageDescriptor": {
                        "Columns": schema_json,
                        "Location": f"s3://{S3_BUCKET_NAME}/{s3_subdir}/landing/",
                        "InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
                        "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputForma",
                        "Compressed": False,
                        "SerdeInfo": {"SerializationLibrary": "org.openx.data.jsonserde.JsonSerDe"}},
                    "Parameters": {"classification": "json"},
                    "TableType": "EXTERNAL_TABLE"}
            )

        print(table_name + " created")

    except Exception as e:
        print(e)