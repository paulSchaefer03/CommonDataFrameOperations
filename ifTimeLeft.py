from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import avg
from pyspark.sql.functions import lower
from pyspark.sql.functions import trim
from pyspark.sql.window import Window
from pyspark.sql.functions import sum
from pyspark.sql.functions import udf
from pyspark.sql.functions import rank
from pyspark.sql.functions import when

# Schema for example data(original data later)
schema = StructType([
    StructField("origin", StringType(), True),
    StructField("destination", StringType(), True),
    StructField("date", StringType(), True),   # YYYYMMDD
    StructField("delay", IntegerType(), True),  # Delay in minutes
    StructField("distance", IntegerType(), True)
])

# Some example data
data = [
    ("JFK", "LAX", "20240101", 10, 4000),
    ("ATL", "ORD", "20240102", -5, 700),
    ("SFO", "SEA", "20240103", 30, 800),
    ("SEA", "LAX", "20240104", 45, 1000),
    ("ORD", "DFW", "20240105", 0, 1200),
    ("JFK", "ATL", "20240106", -10, 500),
    ("LAX", "ORD", "20240107", 5, 1750)
]

# DataFrame with schema
df = spark.createDataFrame(data, schema)

# Register the DataFrame as a temp View, only for this session
df.show()

# Aggregation, average delay per origin
df.groupBy("origin").agg(avg("delay").alias("avg_delay")).show()

#String manipulation
df.withColumn("origin_lower", lower(df.origin)).show()


#Using UDFs
def classify_delay(delay):
    if delay < 0:
        return "Früh"
    elif delay == 0:
        return "Pünktlich"
    elif delay < 30:
        return "Leichte Verspätung"
    else:
        return "Starke Verspätung"

# Register the UDF
classify_delay_udf = udf(classify_delay, StringType())

# add a new column with the classification
df.withColumn("delay_category", classify_delay_udf(df.delay)).show()

# Window functions
windowSpec = Window.partitionBy("origin").orderBy(df.delay.desc())
df.withColumn("delay_rank", rank().over(windowSpec)).show()