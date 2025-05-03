import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
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

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node Step Trainer Landing
StepTrainerLanding_node1746297447528 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-lakehouse-ba/step_trainer/landing/"], "recurse": True}, transformation_ctx="StepTrainerLanding_node1746297447528")

# Script generated for node Customer Curated
CustomerCurated_node1746297477035 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-lakehouse-ba/customer/curated/"], "recurse": True}, transformation_ctx="CustomerCurated_node1746297477035")

# Script generated for node Join Step Trainer Landing to Customer Curated
SqlQuery0 = '''
select s.*
from step_trainer_landing s
join customer_curated c
 on s.serialnumber = c.serialnumber
'''
JoinStepTrainerLandingtoCustomerCurated_node1746297495754 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"step_trainer_landing":StepTrainerLanding_node1746297447528, "customer_curated":CustomerCurated_node1746297477035}, transformation_ctx = "JoinStepTrainerLandingtoCustomerCurated_node1746297495754")

# Script generated for node Drop Duplicates
DropDuplicates_node1746297573095 =  DynamicFrame.fromDF(JoinStepTrainerLandingtoCustomerCurated_node1746297495754.toDF().dropDuplicates(), glueContext, "DropDuplicates_node1746297573095")

# Script generated for node Step Trainer Trusted
EvaluateDataQuality().process_rows(frame=DropDuplicates_node1746297573095, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1746297198607", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
StepTrainerTrusted_node1746297579499 = glueContext.getSink(path="s3://stedi-lakehouse-ba/step_trainer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="StepTrainerTrusted_node1746297579499")
StepTrainerTrusted_node1746297579499.setCatalogInfo(catalogDatabase="stedi",catalogTableName="step_trainer_trusted")
StepTrainerTrusted_node1746297579499.setFormat("json")
StepTrainerTrusted_node1746297579499.writeFrame(DropDuplicates_node1746297573095)
job.commit()