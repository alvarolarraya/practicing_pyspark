print("\033[34m---------------------------------------------------------------------------------------------------------------\033[0m")
# What were all the different types of fire calls in 2018?
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, BooleanType, DoubleType

SAMPLE_RATIO=0.0001
FILE_PATH="/Users/alvar/Desktop/practicing pyspark/dataframe-api-exercises/sf-fire-calls.csv"

# get session
spark = SparkSession.builder \
    .appName("Leer CSV y mostrar esquema con sampling") \
    .getOrCreate()

# read sample and infer schema
df_sample = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .option("samplingRatio", SAMPLE_RATIO) \
    .csv(FILE_PATH)

print(f"el fichero sf-fire-calls.csv de {df_sample.count()/SAMPLE_RATIO:,} filas tiene el esquema:")
df_sample.printSchema()
print("\033[34m---------------------------------------------------------------------------------------------------------------\033[0m")
df_sample.show(3,truncate=False, vertical=True)
print("\033[34m---------------------------------------------------------------------------------------------------------------\033[0m")

# definir esquema y leer el dataset completo
schema = StructType([
    StructField("CallNumber", IntegerType(), True),
    StructField("UnitID", StringType(), True),
    StructField("IncidentNumber", IntegerType(), True),
    StructField("CallType", StringType(), True),
    StructField("CallDate", StringType(), True),
    StructField("WatchDate", StringType(), True),
    StructField("CallFinalDisposition", StringType(), True),
    StructField("AvailableDtTm", StringType(), True),
    StructField("Address", StringType(), True),
    StructField("City", StringType(), True),
    StructField("Zipcode", IntegerType(), True),
    StructField("Battalion", StringType(), True),
    StructField("StationArea", IntegerType(), True),
    StructField("Box", IntegerType(), True),
    StructField("OriginalPriority", StringType(), True),
    StructField("Priority", StringType(), True),
    StructField("FinalPriority", IntegerType(), True),
    StructField("ALSUnit", BooleanType(), True),
    StructField("CallTypeGroup", StringType(), True),
    StructField("NumAlarms", IntegerType(), True),
    StructField("UnitType", StringType(), True),
    StructField("UnitSequenceInCallDispatch", IntegerType(), True),
    StructField("FirePreventionDistrict", IntegerType(), True),
    StructField("SupervisorDistrict", IntegerType(), True),
    StructField("Neighborhood", StringType(), True),
    StructField("Location", StringType(), True),
    StructField("RowID", StringType(), True),
    StructField("Delay", DoubleType(), True)
])
df = spark.read \
    .option("header", "true") \
    .schema(schema) \
    .csv(FILE_PATH)

# quedarme con los tipos de llamadas hechas en 2018    
result = (df_sample
            .withColumn('date', to_timestamp(col('CallDate'), "MM/dd/yyyy"))
            .where(year("date") == 2018)
            .select("CallType")
            .distinct())
result.show(truncate=False)
print("\033[34m---------------------------------------------------------------------------------------------------------------\033[0m")
            
