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

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1745772283838 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="step_trainer_trusted", transformation_ctx="StepTrainerTrusted_node1745772283838")

# Script generated for node Customer Curated
CustomerCurated_node1745772287599 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_curated", transformation_ctx="CustomerCurated_node1745772287599")

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1745772286878 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_trusted", transformation_ctx="AccelerometerTrusted_node1745772286878")

# Script generated for node Join Step Trainer and Accelerometer Trusted to Customer Curated
SqlQuery0 = '''
select
 s.sensorreadingtime,
 s.serialnumber,
 s.distancefromobject,
 a.user,
 a.x,
 a.y,
 a.z,
 c.customername,
 c.phone,
 c.birthday,
 c.registrationdate,
 c.lastupdatedate,
 c.sharewithresearchasofdate,
 c.sharewithpublicasofdate,
 c.sharewithfriendsasofdate
from step_trainer_trusted s
join accelerometer_trusted a
 on s.sensorreadingtime = a.timestamp
join customer_curated c
 on s.serialnumber = c.serialnumber
 and a.user = c.email
'''
JoinStepTrainerandAccelerometerTrustedtoCustomerCurated_node1745772469018 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"step_trainer_trusted":StepTrainerTrusted_node1745772283838, "accelerometer_trusted":AccelerometerTrusted_node1745772286878, "customer_curated":CustomerCurated_node1745772287599}, transformation_ctx = "JoinStepTrainerandAccelerometerTrustedtoCustomerCurated_node1745772469018")

# Script generated for node Machine Learning Curated
MachineLearningCurated_node1745772524333 = glueContext.getSink(path="s3://stedi-lakehouse-ba/machine_learning/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="MachineLearningCurated_node1745772524333")
MachineLearningCurated_node1745772524333.setCatalogInfo(catalogDatabase="stedi",catalogTableName="machine_learning_curated")
MachineLearningCurated_node1745772524333.setFormat("json")
MachineLearningCurated_node1745772524333.writeFrame(JoinStepTrainerandAccelerometerTrustedtoCustomerCurated_node1745772469018)
job.commit()