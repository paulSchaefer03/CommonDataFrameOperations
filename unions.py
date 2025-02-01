# Set file paths(absolute path) for the data sets
from pyspark.sql.functions import expr
tripdelaysFilePath = "C:/Users/pauls/Documents/Hochschule_Fulda/Master/Semester_1/Advanced_Big_Data/Final_Presentation/CommonDataFrameOperations/databricks-datasets/learning-spark-v2/flights/departuredelays.csv"
airportsnaFilePath = "C:/Users/pauls/Documents/Hochschule_Fulda/Master/Semester_1/Advanced_Big_Data/Final_Presentation/CommonDataFrameOperations/databricks-datasets/learning-spark-v2/flights/airport-codes-na.txt"

# Obtain airports data set
airportsna = (spark.read
.format("csv")
.options(header="true", inferSchema="true", sep="\t")
.load(airportsnaFilePath))
# Register the DataFrame as a temp View, only for this session
# Optionally, you can create a global temp view for cross-session access: createGlobalTempView()
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

# Create a slighty bigger table
fooBigger = (departureDelays
.filter(expr("""date like '01010%'""")))
fooBigger.createOrReplaceTempView("fooBigger")

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

#If we want to remove duplicates we can use dropDuplicates() or distinct()
bar.filter(expr("""origin == 'SEA' AND destination == 'SFO' AND date LIKE '01010%' AND delay > 0""")).dropDuplicates().show()
















#________________________________________________________________________________________________________________________

# What if the DataFrames have a different schema, we have to do some Modifications ?
# Old Flights (without Airline column)
oldFlights = spark.createDataFrame([
    ("JFK", "LAX", "2023-12-31", 5),
    ("ATL", "ORD", "2023-12-30", -2),
    ("SFO", "SEA", "2023-12-29", 10)
], ["origin", "destination", "date", "delay"])

# New Flights (with Airline column)
newFlights = spark.createDataFrame([
    ("JFK", "LAX", "2024-01-01", 3, "American"),
    ("ATL", "ORD", "2024-01-02", -1, "Delta"),
    ("SFO", "SEA", "2024-01-03", 12, "United")
], ["origin", "destination", "date", "delay", "airline"])

# Add a new column for airline to the oldFlights DataFrame with a default value
oldFlightsFormatted = oldFlights.selectExpr("origin", "destination", "date", "delay", "NULL as airline")
newFlightsFormatted = newFlights.selectExpr("origin", "destination", "date", "delay", "airline")

# Combine both DataFrames
combinedFlights = oldFlightsFormatted.union(newFlightsFormatted)
# Show the results
combinedFlights.show()
