import folium
from pyspark.sql import SparkSession
from pyspark.sql.functions import *


def earthquake_classifier(input_value):
    if input_value is not None:
        try:
            input_value = float(input_value)
        except ValueError:
            return None
        if input_value < 4.0:
            return "Low"
        elif 4.0 <= input_value < 6.0:
            return "Medium"
        elif input_value >= 6.0:
            return "High"
        else:
            return None
    return None


# Define the UDF for earthquake classification
earthquake_classifier_udf = udf(earthquake_classifier)

# Initialize SparkSession
spark = SparkSession.builder.appName("Earthquake Dataset").getOrCreate()

try:
    # Load data into PySpark DataFrame
    data = spark.read.csv("database.csv", sep=",", header=True)

    # Creating Timestamp column from Date and Time columns
    data = data.withColumn("Timestamp", to_timestamp(concat(col("Date"), lit(" "), col("Time")), "MM/dd/yyyy HH:mm:ss"))

    # Filtering the dataset to include earthquakes with a magnitude greater than 5.0
    filtered_dataframe = data.filter(col("Magnitude") > 5.0)

    # Displaying average Depth and Magnitude by Earthquake type
    avg_stats = data.groupBy("Type").agg(round(avg("Depth"), 2).alias("Avg_Depth"),
                                         round(avg("Magnitude"), 2).alias("Avg_Magnitude"))

    # Classifying earthquake levels
    data = data.withColumn("Earthquake Levels", earthquake_classifier_udf(data.Magnitude))

    # Calculating distance from reference point in kilometers
    REF_LATITUDE = 0.0
    REF_LONGITUDE = 0.0
    ref_latitude_col = lit(REF_LATITUDE)
    ref_longitude_col = lit(REF_LONGITUDE)
    data = data.withColumn('Distance in kms',
                           round((acos((sin(radians(ref_latitude_col)) * sin(radians(col("Latitude")))) +
                                       ((cos(radians(ref_latitude_col)) * cos(radians(col("Latitude")))) *
                                        (cos(radians(ref_longitude_col) - radians(col("Longitude")))))
                                       ) * lit(6371.0)), 4))

    # Create Folium map
    map = folium.Map(location=[0, 0], zoom_start=2)

    # Iterate over each row in the DataFrame and add markers to the map
    for row in data.collect():
        latitude = row['Latitude']
        longitude = row['Longitude']
        magnitude = row['Magnitude']

        # Create marker with popup message
        folium.Marker([latitude, longitude], popup=f"Magnitude: {magnitude}").add_to(map)

    # Save the map
    map.save("earthquake_map.html")

    # Display average statistics
    print("\n\nDisplaying Average Depth and Magnitude of Earthquakes for each Earthquake Type : ")
    avg_stats.show()

    # Display earthquakes magnitude > 5.0
    print("\n\nDisplaying dataframe with earthquakes magnitude greater than 5.0 : ")
    filtered_dataframe.show(10)

    # Display final dataframe
    print("\n\nDisplaying final dataframe with all transformations : ")
    data.show(10)

    # Write transformed DataFrame to CSV
    data.coalesce(1).write.csv(path="transformed_df", header=True, mode="overwrite")

except Exception as e:
    print("An error occurred:", str(e))

finally:
    # Stop SparkSession
    spark.stop()
