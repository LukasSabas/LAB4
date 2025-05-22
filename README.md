# LAB4
Laboratory work for PySpark

# Vessel Distance Calculation with PySpark

This project calculates the total traveled distance for each vessel (identified by MMSI) using AIS data.
The process is done using **PySpark**, which enables efficient handling of large datasets.

The dataset used contains ship positions over time, and the main goal is to compute the **cumulative distance traveled by each vessel**,
based on timestamps, and to identify the vessel with the longest route.

---

## PySpark

- AIS datasets can is large, and PySpark offers scalability and distributed processing..
- Built-in support for **window functions**, **timestamp operations**, and **schema enforcement** simplifies data transformations.

---

## Steps and Justification

1. **Data Reading and Schema Definition**
   - Read only the necessary columns to reduce memory usage.
   - Explicitly define the schema to avoid incorrect type inference.

2. **Cleaning the Data**
   - Remove rows with null latitude, longitude, or timestamp to avoid computation errors.

3. **Window Function for Point Pairing**
   - Use `Window.partitionBy("MMSI").orderBy("Timestamp")` to access the previous coordinate per vessel.
   - Needed to calculate distance between sequential points.

4. **Distance Calculation**
   - Use the **Haversine formula** to compute the spherical distance between two GPS points.
   - Earth's radius is assumed as 6371 km.

5. **Speed Filtering**
   - Calculate speed between points and filter out unrealistic speeds (>100 km/h) to remove potential data errors or anomalies.

6. **Aggregation**
   - Sum up valid distances per MMSI to compute total distance per vessel.

7. **Result Display**
   - Identify and print the vessel that traveled the longest distance.

---

## Results
The vessel with MMSI = 219133000 has traveled the longest distance under above mentioned conditions. 
The distance was 787.2966 km.
