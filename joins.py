#Joining DataFrames please first create the DataFrames from the previous snippet

# Join departure delays data (foo) with airport info

foo.join(airportsna, airportsna.IATA == foo.origin
).select("City", "State", "date", "delay", "distance", "destination").show()

# In SQL
spark.sql("""
 SELECT a.City, a.State, f.date, f.delay, f.distance, f.destination
   FROM foo f
   JOIN airports_na a
     ON a.IATA = f.origin
""").show()

# Join departure delays data (foo) with airport info, using a cross join
foo.filter(foo.delay < 30).crossJoin(airportsna).select("City", "State", "date", "delay", "distance", "destination").show()

# In SQL
spark.sql("""
    SELECT a.City, a.State, f.date, f.delay, f.distance, f.destination
    FROM foo f
    CROSS JOIN airports_na a
    WHERE delay < 30
""").show()


# Join with Common DataFrame operations (select, filter, groupBy, orderBy, agg, avg, max, min, sum, count)
from pyspark.sql.functions import avg
from pyspark.sql.functions import max
from pyspark.sql.functions import avg, udf
from pyspark.sql.types import StringType

# Average delay per origin
avg_delay = fooBigger.groupBy("origin").agg(avg("delay").alias("avg_delay"))
avg_delay.show()

avg_delay.join(airportsna, airportsna.IATA == avg_delay.origin, "inner") \
         .select("City", "State", "avg_delay") \
         .show()


# Maximum delay per origin
max_delay = fooBigger.groupBy("origin").agg(max("delay").alias("max_delay"))

# Join the result with the airport info
ranked_delays = max_delay.join(airportsna, airportsna.IATA == max_delay.origin, "inner") \
    .select("City", "State", "max_delay") \
    .orderBy("max_delay", ascending=False) 

ranked_delays.show()




# Join with UDFs

# Average delay per origin with classification

# UDF for classification 
def classify_airport(delay):
    if delay < 5:
        return "Pünktlich"
    elif delay < 20:
        return "Leichte Verspätung"
    else:
        return "Schwere Verspätung"

# Register the UDF
classify_airport_udf = udf(classify_airport, StringType())

# Add a new column with the classification
avg_delay = avg_delay.withColumn("delay_category", classify_airport_udf(avg_delay.avg_delay))

# Join the result with the airport info
classified_airports = avg_delay.join(airportsna, airportsna.IATA == avg_delay.origin, "inner") \
    .select("City", "State", "avg_delay", "delay_category")

classified_airports.show()