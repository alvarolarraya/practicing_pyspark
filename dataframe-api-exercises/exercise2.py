# What months within the year 2018 saw the highest number of fire calls?
print("\033[34m---------------------------------------------------------------------------------------------------------------\033[0m")
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, BooleanType, DoubleType
from pyspark.sql.functions import monotonically_increasing_id

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

meses=[
    "enero",
    "febrero",
    "marzo",
    "abril",
    "mayo",
    "junio",
    "julio",
    "agosto",
    "septiembre",
    "octubre",
    "noviembre",
    "diciembre"
]
# quedarme con los meses con mas llamadas    
result = (df_sample
            .withColumn('date', to_timestamp(col('CallDate'), "MM/dd/yyyy"))
            .where(year("date") == 2018)
            .groupBy(date_format("date", "MMMM").alias("month"))
            .count()
            .orderBy(desc("count"))
            .withColumn("-", monotonically_increasing_id()+1)
            # reordena las columnas
            .select("-","month","count"))
result.show(truncate=False)
print("\033[34m---------------------------------------------------------------------------------------------------------------\033[0m")
            
