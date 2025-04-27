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

# Script generated for node Customer Trusted
CustomerTrusted_node1745729750426 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_trusted", transformation_ctx="CustomerTrusted_node1745729750426")

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1745729852647 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_trusted", transformation_ctx="AccelerometerTrusted_node1745729852647")

# Script generated for node Join Customer Trusted to Accelerometer Trusted
SqlQuery0 = '''
select c.*
from customer_trusted c 
join accelerometer_trusted a 
 on c.email = a.user
'''
JoinCustomerTrustedtoAccelerometerTrusted_node1745729880573 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"customer_trusted":CustomerTrusted_node1745729750426, "accelerometer_trusted":AccelerometerTrusted_node1745729852647}, transformation_ctx = "JoinCustomerTrustedtoAccelerometerTrusted_node1745729880573")

# Script generated for node Drop Duplicates
DropDuplicates_node1745731426014 =  DynamicFrame.fromDF(JoinCustomerTrustedtoAccelerometerTrusted_node1745729880573.toDF().dropDuplicates(), glueContext, "DropDuplicates_node1745731426014")

# Script generated for node Customer Curated
CustomerCurated_node1745729939534 = glueContext.getSink(path="s3://stedi-lakehouse-ba/customer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="CustomerCurated_node1745729939534")
CustomerCurated_node1745729939534.setCatalogInfo(catalogDatabase="stedi",catalogTableName="customer_curated")
CustomerCurated_node1745729939534.setFormat("json")
CustomerCurated_node1745729939534.writeFrame(DropDuplicates_node1745731426014)
job.commit()