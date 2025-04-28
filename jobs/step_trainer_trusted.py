import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from awsglue import DynamicFrame
from pyspark.sql import functions as SqlFuncs

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Customer Curated
CustomerCurated_node1745730465286 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_curated", transformation_ctx="CustomerCurated_node1745730465286")

# Script generated for node Step Trainer Landing
StepTrainerLanding_node1745730513812 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-lakehouse-ba/step_trainer/landing/"], "recurse": True}, transformation_ctx="StepTrainerLanding_node1745730513812")

# Script generated for node Join Step Trainer Landing to Customer Curated and Acclerometer Trusted
SqlQuery0 = '''
select s.*
from step_trainer_landing s
join customer_curated c
 on s.serialnumber = c.serialnumber
'''
JoinStepTrainerLandingtoCustomerCuratedandAcclerometerTrusted_node1745730569985 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"step_trainer_landing":StepTrainerLanding_node1745730513812, "customer_curated":CustomerCurated_node1745730465286}, transformation_ctx = "JoinStepTrainerLandingtoCustomerCuratedandAcclerometerTrusted_node1745730569985")

# Script generated for node Drop Duplicates
DropDuplicates_node1745731231819 =  DynamicFrame.fromDF(JoinStepTrainerLandingtoCustomerCuratedandAcclerometerTrusted_node1745730569985.toDF().dropDuplicates(), glueContext, "DropDuplicates_node1745731231819")

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1745731273312 = glueContext.getSink(path="s3://stedi-lakehouse-ba/step_trainer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="StepTrainerTrusted_node1745731273312")
StepTrainerTrusted_node1745731273312.setCatalogInfo(catalogDatabase="stedi",catalogTableName="step_trainer_trusted")
StepTrainerTrusted_node1745731273312.setFormat("json")
StepTrainerTrusted_node1745731273312.writeFrame(DropDuplicates_node1745731231819)
job.commit()