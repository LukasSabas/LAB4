import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.sql.functions import col, to_timestamp, desc
from pyspark.sql.window import Window

# Function to calculate each vessels traveled distance
def calculate_total_distance_per_vessel(df):
    # Step 1: Filter out bad rows
    df_clean = df.filter(F.col("Latitude").isNotNull() & F.col("Longitude").isNotNull() & F.col("Timestamp").isNotNull())

    # Step 2: Define window to order by timestamp within each vessel
    windowSpec = Window.partitionBy("MMSI").orderBy("Timestamp")

    # Step 3: Add previous point's coordinates and timestamp
    df_with_prev = df_clean.withColumn("prev_lat", F.lag("Latitude").over(windowSpec)) \
                           .withColumn("prev_lon", F.lag("Longitude").over(windowSpec)) \
                           .withColumn("prev_timestamp", F.lag("Timestamp").over(windowSpec))

    # Step 4: Filter out rows where previous data is NULL
    df_with_prev_filtered = df_with_prev.filter(F.col("prev_lat").isNotNull() & F.col("prev_lon").isNotNull() & F.col("prev_timestamp").isNotNull())

    # Step 5: Compute time difference between current and previous points (in seconds)
    df_with_prev_filtered = df_with_prev_filtered.withColumn("time_diff_sec", 
                                                           (F.col("Timestamp").cast("long") - F.col("prev_timestamp").cast("long")))

    # Step 6: Haversine formula to compute distance between consecutive points
    R = 6371.0  # Earth's radius in kilometers

    df_haversine = df_with_prev_filtered \
        .withColumn("lat1", F.radians(F.col("prev_lat"))) \
        .withColumn("lat2", F.radians(F.col("Latitude"))) \
        .withColumn("lon1", F.radians(F.col("prev_lon"))) \
        .withColumn("lon2", F.radians(F.col("Longitude"))) \
        .withColumn("dlat", F.col("lat2") - F.col("lat1")) \
        .withColumn("dlon", F.col("lon2") - F.col("lon1")) \
        .withColumn("a", (F.sin(F.col("dlat") / 2)**2) + (F.cos(F.col("lat1")) * F.cos(F.col("lat2")) * F.sin(F.col("dlon") / 2)**2)) \
        .withColumn("c", 2 * F.atan2(F.sqrt(F.col("a")), F.sqrt(1 - F.col("a")))) \
        .withColumn("distance_km", R * F.col("c"))

    # Step 8: Calculate speed and filter out unrealistic speeds
    max_speed_kmh = 100 
    df_haversine = df_haversine.withColumn("speed_kmh", F.col("distance_km") / (F.col("time_diff_sec") / 3600))  # Speed in km/h
    df_haversine = df_haversine.filter(F.col("speed_kmh") <= max_speed_kmh)  # Remove rows where speed exceeds max_speed_kmh

    # Step 9: Aggregate by MMSI to get the total distance traveled by each vessel
    df_total_distance = df_haversine.groupBy("MMSI").sum("distance_km") \
        .withColumnRenamed("sum(distance_km)", "total_distance_km")

    # Step 10: Handle vessels with no valid distance (if they only have one point or missing data)
    df_total_distance = df_total_distance.na.fill({"total_distance_km": 0})

    return df_total_distance


# Printing the resutls
def print_longest_route(longest_route):
    # Sort by total_distance_km in descending order and get the first row
    longest_route = df_total_distance.orderBy(desc("total_distance_km")).first()

    print("\n" * 2)
    print("#" * 50)
    print(f"# {'Vessel MMSI: ' + str(longest_route['MMSI']) + ', Total Distance: ' + str(longest_route['total_distance_km']) + ' km'}")
    print("#" * 50)
    print("\n" * 2) 

################################################################################
##########################----  MAIN  ----######################################
# Setting the Python environment explicitly (becasue not working otherwise)
os.environ['PYSPARK_PYTHON'] = r'C:\Users\37068\AppData\Local\Programs\Python\Python39\python.exe'
os.environ['PYSPARK_DRIVER_PYTHON'] = r'C:\Users\37068\AppData\Local\Programs\Python\Python39\python.exe'

# Creating Spark session
spark = SparkSession.builder \
    .appName("Lab4") \
    .master("local[*]") \
    .getOrCreate()

# Defining schema manually to ensure correct types (only taking needed columns)
schema = StructType([
    StructField("# Timestamp", StringType(), True),
    StructField("Type of mobile", StringType(), True),
    StructField("MMSI", StringType(), True),
    StructField("Latitude", StringType(), True),
    StructField("Longitude", StringType(), True),
])

# Reading CSV
df_raw = spark.read.csv(
    "\\aisdk-2024-05-04.csv",
    schema=schema,
    header=True,
    sep=","
)


# Casting and parseing necessary columns
df = df_raw.withColumn("Latitude", col("Latitude").cast(DoubleType())) \
           .withColumn("Longitude", col("Longitude").cast(DoubleType())) \
           .withColumn("Timestamp", to_timestamp(col("# Timestamp"), "dd/MM/yyyy HH:mm:ss"))

# Calculating distance traveled by each vessel
df_total_distance = calculate_total_distance_per_vessel(df)
print_longest_route(df_total_distance)

