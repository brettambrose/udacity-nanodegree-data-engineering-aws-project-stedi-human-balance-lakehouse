import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
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

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1745729093809 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-lakehouse-ba/accelerometer/landing/"], "recurse": True}, transformation_ctx="AccelerometerLanding_node1745729093809")

# Script generated for node Customer Trusted
CustomerTrusted_node1745729171858 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_trusted", transformation_ctx="CustomerTrusted_node1745729171858")

# Script generated for node Join Accelerometer Landing to Customer Trusted
SqlQuery0 = '''
select a.*
from accelerometer_landing a 
join customer_trusted c 
 on a.user = c.email
'''
JoinAccelerometerLandingtoCustomerTrusted_node1745729237880 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"accelerometer_landing":AccelerometerLanding_node1745729093809, "customer_trusted":CustomerTrusted_node1745729171858}, transformation_ctx = "JoinAccelerometerLandingtoCustomerTrusted_node1745729237880")

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1745729309525 = glueContext.getSink(path="s3://stedi-lakehouse-ba/accelerometer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AccelerometerTrusted_node1745729309525")
AccelerometerTrusted_node1745729309525.setCatalogInfo(catalogDatabase="stedi",catalogTableName="accelerometer_trusted")
AccelerometerTrusted_node1745729309525.setFormat("json")
AccelerometerTrusted_node1745729309525.writeFrame(JoinAccelerometerLandingtoCustomerTrusted_node1745729237880)
job.commit()