import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue import DynamicFrame

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

# Script generated for node Customer Trusted
CustomerTrusted_node1746296432391 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-lakehouse-ba/customer/trusted/"], "recurse": True}, transformation_ctx="CustomerTrusted_node1746296432391")

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1746296904392 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-lakehouse-ba/accelerometer/landing/"], "recurse": True}, transformation_ctx="AccelerometerLanding_node1746296904392")

# Script generated for node Join Accelerometer Landing to Customer Trusted
SqlQuery0 = '''
select a.*
from accelerometer_landing a 
join customer_trusted c 
 on a.user = c.email
'''
JoinAccelerometerLandingtoCustomerTrusted_node1746296481999 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"customer_trusted":CustomerTrusted_node1746296432391, "accelerometer_landing":AccelerometerLanding_node1746296904392}, transformation_ctx = "JoinAccelerometerLandingtoCustomerTrusted_node1746296481999")

# Script generated for node Accelerometer Trusted
EvaluateDataQuality().process_rows(frame=JoinAccelerometerLandingtoCustomerTrusted_node1746296481999, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1746296395948", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AccelerometerTrusted_node1746296572862 = glueContext.getSink(path="s3://stedi-lakehouse-ba/accelerometer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AccelerometerTrusted_node1746296572862")
AccelerometerTrusted_node1746296572862.setCatalogInfo(catalogDatabase="stedi",catalogTableName="accelerometer_trusted")
AccelerometerTrusted_node1746296572862.setFormat("json")
AccelerometerTrusted_node1746296572862.writeFrame(JoinAccelerometerLandingtoCustomerTrusted_node1746296481999)
job.commit()