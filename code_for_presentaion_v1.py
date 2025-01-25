# Set file paths(absolute path) for the data sets
from pyspark.sql.functions import expr
tripdelaysFilePath = "C:/Users/pauls/Documents/Hochschule_Fulda/Master/Semester_1/Advanced_Big_Data/Final_Presentation/CommonDataFrameOperations/databricks-datasets/learning-spark-v2/flights/departuredelays.csv"
airportsnaFilePath = "C:/Users/pauls/Documents/Hochschule_Fulda/Master/Semester_1/Advanced_Big_Data/Final_Presentation/CommonDataFrameOperations/databricks-datasets/learning-spark-v2/flights/airport-codes-na.txt"

# Obtain airports data set
airportsna = (spark.read
.format("csv")
.options(header="true", inferSchema="true", sep="\t")
.load(airportsnaFilePath))
airportsna.createOrReplaceTempView("airports_na")

# Obtain departure delays data set
departureDelays = (spark.read
.format("csv")
.options(header="true")
.load(tripdelaysFilePath))
departureDelays = (departureDelays
.withColumn("delay", expr("CAST(delay as INT) as delay"))
.withColumn("distance", expr("CAST(distance as INT) as distance")))
departureDelays.createOrReplaceTempView("departureDelays")

# Create temporary small table
foo = (departureDelays
.filter(expr("""origin == 'SEA' and destination == 'SFO' and date like '01010%' and delay > 0""")))
foo.createOrReplaceTempView("foo")

# DataFrame airports_na
spark.sql("SELECT * FROM airports_na LIMIT 10").show()

# DataFrame departureDelays
spark.sql("SELECT * FROM departureDelays LIMIT 10").show()

# DataFrame foo(no Limit it only contains 3 rows)
spark.sql("SELECT * FROM foo").show()

# Union two tables
bar = departureDelays.union(foo)
bar.createOrReplaceTempView("bar")

# Show the union (we only see the same as departureDelays)
spark.sql("SELECT * FROM bar LIMIT 10").show()

# Show the union (filtering for SEA and SFO in a specific time range), know we see the duplicateted 3 rows from foo
bar.filter(expr("""origin == 'SEA' AND destination == 'SFO' AND date LIKE '01010%' AND delay > 0""")).show()
# Or using SQL
spark.sql("""
 SELECT * 
  FROM bar 
 WHERE origin = 'SEA' 
   AND destination = 'SFO' 
   AND date LIKE '01010%' 
   AND delay > 0
 """).show()


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

# Join departure delays data (foo) with airport info, using a full join
foo.join(airportsna, airportsna.IATA == foo.origin, "full"
).select("City", "State", "date", "delay", "distance", "destination").show()

# In SQL
spark.sql("""
 SELECT a.City, a.State, f.date, f.delay, f.distance, f.destination
    FROM foo f
    FULL JOIN airports_na a
      ON a.IATA = f.origin
""").show()

