# 🗽 NYC Yellow Taxi Data Analysis using PySpark

## 🎯 Objective

Analyze **NYC Yellow Taxi January 2018 Parquet Data** using PySpark in Databricks to extract meaningful insights from passenger rides, vendor performance, route activity, and more.

---

## 📦 Dataset Details

- **File Name:** yellow_tripdata_2018-01.parquet  
- **Format:** Parquet  
- **Source:** [NYC TLC Trip Data](https://www.nyc.gov/html/tlc/html/about/trip_record_data.shtml)

---

## ⚙️ Technologies Used

- Apache Spark (PySpark)
- Databricks
- Azure Data Lake (optional)
- Python

---

## 🚀 Step 1: Load Dataset

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("NYC Taxi Data Analysis") \
    .getOrCreate()

df = spark.read.option("header", "true") \
    .option("inferSchema", "true") \
    .parquet("/mnt/data/yellow_tripdata_2018-01.parquet")

df.show(5)
```

---

## ✅ Query 1: Add a "Revenue" Column

```python
from pyspark.sql.functions import col

df = df.withColumn("Revenue", 
    col("fare_amount") + col("extra") + col("mta_tax") +
    col("improvement_surcharge") + col("tip_amount") +
    col("tolls_amount") + col("total_amount")
)

df.select("Revenue").show(5)
```

---

## ✅ Query 2: Total Passengers by Pickup Area

```python
df.groupBy("PULocationID") \
  .sum("passenger_count") \
  .withColumnRenamed("sum(passenger_count)", "total_passengers") \
  .orderBy("total_passengers", ascending=False) \
  .show()
```

---

## ✅ Query 3: Average Fare / Total Earnings by Vendor

```python
df.groupBy("VendorID") \
  .avg("total_amount") \
  .withColumnRenamed("avg(total_amount)", "average_earning") \
  .show()
```

---

## ✅ Query 4: Count of Payments by Payment Mode

```python
df.groupBy("payment_type") \
  .count() \
  .withColumnRenamed("count", "payment_count") \
  .orderBy("payment_count", ascending=False) \
  .show()
```

---

## ✅ Query 5: Top 2 Earning Vendors on a Specific Date

```python
from pyspark.sql.functions import to_date

specific_date = "2018-01-15"

df_filtered = df.filter(to_date("tpep_pickup_datetime") == specific_date)

df_filtered.groupBy("VendorID") \
  .agg({
    "total_amount": "sum",
    "passenger_count": "sum",
    "trip_distance": "sum"
  }) \
  .withColumnRenamed("sum(total_amount)", "total_earning") \
  .withColumnRenamed("sum(passenger_count)", "total_passengers") \
  .withColumnRenamed("sum(trip_distance)", "total_distance") \
  .orderBy("total_earning", ascending=False) \
  .show(2)
```

---

## ✅ Query 6: Route with Most Passengers

```python
df.groupBy("PULocationID", "DOLocationID") \
  .sum("passenger_count") \
  .withColumnRenamed("sum(passenger_count)", "total_passengers") \
  .orderBy("total_passengers", ascending=False) \
  .show(1)
```

---

## ✅ Query 7: Top Pickup Locations in Last 10 Seconds

```python
from pyspark.sql.functions import unix_timestamp

df = df.withColumn("pickup_unix", unix_timestamp("tpep_pickup_datetime"))

latest_time = df.select("pickup_unix") \
  .orderBy("pickup_unix", ascending=False) \
  .first()[0]

df.filter(col("pickup_unix") > (latest_time - 10)) \
  .groupBy("PULocationID") \
  .sum("passenger_count") \
  .withColumnRenamed("sum(passenger_count)", "total_passengers") \
  .orderBy("total_passengers", ascending=False) \
  .show()
```

---

## 📌 How to Run This

1. Open your Databricks Workspace  
2. Upload `yellow_tripdata_2018-01.parquet` to path `/mnt/data/`  
3. Create new notebook and copy-paste this code  
4. Run each query one-by-one to see the results

---

## ✅ Done!

This completes the NYC Taxi data assignment using PySpark and Databricks.

