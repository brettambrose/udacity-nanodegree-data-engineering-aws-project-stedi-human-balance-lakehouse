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

# Script generated for node Customer Landing
CustomerLanding_node1745728481261 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_landing", transformation_ctx="CustomerLanding_node1745728481261")

# Script generated for node Privacy Filter
SqlQuery0 = '''
select *
from customer_landing
where sharewithresearchasofdate IS NOT NULL
'''
PrivacyFilter_node1745728686385 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"customer_landing":CustomerLanding_node1745728481261}, transformation_ctx = "PrivacyFilter_node1745728686385")

# Script generated for node Customer Trusted
CustomerTrusted_node1745728781122 = glueContext.getSink(path="s3://stedi-lakehouse-ba/customer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="CustomerTrusted_node1745728781122")
CustomerTrusted_node1745728781122.setCatalogInfo(catalogDatabase="stedi",catalogTableName="customer_trusted")
CustomerTrusted_node1745728781122.setFormat("json")
CustomerTrusted_node1745728781122.writeFrame(PrivacyFilter_node1745728686385)
job.commit()