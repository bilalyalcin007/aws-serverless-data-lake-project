import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrameCollection
from awsgluedq.transforms import EvaluateDataQuality
from awsglue.dynamicframe import DynamicFrame
import re

# Script generated for node Remove Records with NULL
def MyTransform(glueContext, dfc) -> DynamicFrameCollection:
    df = dfc.select(list(dfc.keys())[0]).toDF().na.drop()
    results = DynamicFrame.fromDF(df, glueContext, "results")
    return DynamicFrameCollection({"results": results}, glueContext)
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

# Script generated for node Pickup Zone Lookup
PickupZoneLookup_node1747005198338 = glueContext.create_dynamic_frame.from_catalog(database="nyctaxi_db", table_name="raw_taxi_zone_lookup", transformation_ctx="PickupZoneLookup_node1747005198338")

# Script generated for node Yellow Trip Data
YellowTripData_node1747004247545 = glueContext.create_dynamic_frame.from_catalog(database="nyctaxi_db", table_name="raw_yellow_tripdata", transformation_ctx="YellowTripData_node1747004247545")

# Script generated for node Dropoff Zone Lookup
DropoffZoneLookup_node1747005840043 = glueContext.create_dynamic_frame.from_catalog(database="nyctaxi_db", table_name="raw_taxi_zone_lookup", transformation_ctx="DropoffZoneLookup_node1747005840043")

# Script generated for node Change Schema - Pickup Zone Lookup
ChangeSchemaPickupZoneLookup_node1747005376783 = ApplyMapping.apply(frame=PickupZoneLookup_node1747005198338, mappings=[("locationid", "long", "pu_location_id", "long"), ("borough", "string", "pu_borough", "string"), ("zone", "string", "pu_zone", "string"), ("service_zone", "string", "pu_service_zone", "string")], transformation_ctx="ChangeSchemaPickupZoneLookup_node1747005376783")

# Script generated for node Remove Records with NULL
RemoveRecordswithNULL_node1747004687456 = MyTransform(glueContext, DynamicFrameCollection({"YellowTripData_node1747004247545": YellowTripData_node1747004247545}, glueContext))

# Script generated for node Change Schema - Dropoff Zone Lookup
ChangeSchemaDropoffZoneLookup_node1747005941108 = ApplyMapping.apply(frame=DropoffZoneLookup_node1747005840043, mappings=[("locationid", "long", "do_location_id", "long"), ("borough", "string", "do_borough", "string"), ("zone", "string", "do_zone", "string"), ("service_zone", "string", "do_service_zone", "string")], transformation_ctx="ChangeSchemaDropoffZoneLookup_node1747005941108")

# Script generated for node SelectFromCollection
SelectFromCollection_node1747004820171 = SelectFromCollection.apply(dfc=RemoveRecordswithNULL_node1747004687456, key=list(RemoveRecordswithNULL_node1747004687456.keys())[0], transformation_ctx="SelectFromCollection_node1747004820171")

# Script generated for node Filter - Yellow Trip Data
FilterYellowTripData_node1747004993752 = Filter.apply(frame=SelectFromCollection_node1747004820171, f=lambda row: (bool(re.match("^2020-1", row["tpep_pickup_datetime"]))), transformation_ctx="FilterYellowTripData_node1747004993752")

# Script generated for node Yellow Trips Data + Pickup Zone Lookup
YellowTripsDataPickupZoneLookup_node1747005494964 = Join.apply(frame1=ChangeSchemaPickupZoneLookup_node1747005376783, frame2=FilterYellowTripData_node1747004993752, keys1=["pu_location_id"], keys2=["pulocationid"], transformation_ctx="YellowTripsDataPickupZoneLookup_node1747005494964")

# Script generated for node Yellow Trips Data + Pickup Zone Lookup + Dropoff Zone Lookup
YellowTripsDataPickupZoneLookupDropoffZoneLookup_node1747006038323 = Join.apply(frame1=YellowTripsDataPickupZoneLookup_node1747005494964, frame2=ChangeSchemaDropoffZoneLookup_node1747005941108, keys1=["dolocationid"], keys2=["do_location_id"], transformation_ctx="YellowTripsDataPickupZoneLookupDropoffZoneLookup_node1747006038323")

# Script generated for node Change Schema - Joined Data
ChangeSchemaJoinedData_node1747006240287 = ApplyMapping.apply(frame=YellowTripsDataPickupZoneLookupDropoffZoneLookup_node1747006038323, mappings=[("pu_location_id", "long", "pu_location_id", "long"), ("pu_borough", "string", "pu_borough", "string"), ("pu_zone", "string", "pu_zone", "string"), ("pu_service_zone", "string", "pu_service_zone", "string"), ("vendorid", "long", "vendor_id", "long"), ("tpep_pickup_datetime", "string", "pickup_datetime", "timestamp"), ("tpep_dropoff_datetime", "string", "dropoff_datetime", "timestamp"), ("passenger_count", "long", "passenger_count", "long"), ("trip_distance", "double", "trip_distance", "double"), ("ratecodeid", "long", "ratecodeid", "long"), ("store_and_fwd_flag", "string", "store_and_fwd_flag", "string"), ("payment_type", "long", "payment_type", "long"), ("fare_amount", "double", "fare_amount", "double"), ("extra", "double", "extra", "double"), ("mta_tax", "double", "mta_tax", "double"), ("tip_amount", "double", "tip_amount", "double"), ("tolls_amount", "double", "tolls_amount", "double"), ("improvement_surcharge", "double", "improvement_surcharge", "double"), ("total_amount", "double", "total_amount", "double"), ("congestion_surcharge", "double", "congestion_surcharge", "double"), ("do_location_id", "long", "do_location_id", "long"), ("do_borough", "string", "do_borough", "string"), ("do_zone", "string", "do_zone", "string"), ("do_service_zone", "string", "do_service_zone", "string")], transformation_ctx="ChangeSchemaJoinedData_node1747006240287")

# Script generated for node Transformed Yellow Trip Data
EvaluateDataQuality().process_rows(frame=ChangeSchemaJoinedData_node1747006240287, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1747004076655", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
TransformedYellowTripData_node1747006426487 = glueContext.write_dynamic_frame.from_options(frame=ChangeSchemaJoinedData_node1747006240287, connection_type="s3", format="glueparquet", connection_options={"path": "s3://serverlessanalytics-****-transformed", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="TransformedYellowTripData_node1747006426487")

job.commit()
