from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, DoubleType
import joblib
import pandas as pd
import os

# Load the trained ML model
model = joblib.load("train_model.pkl")

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("DisasterSeverityPrediction") \
    .master("local[*]") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ✅ Updated schema with Disaster_ID
schema = StructType() \
    .add("Disaster_ID", StringType()) \
    .add("Disaster_Type", StringType()) \
    .add("Location", StringType()) \
    .add("Magnitude", DoubleType()) \
    .add("Date", StringType()) \
    .add("Fatalities", DoubleType()) \
    .add("Economic_Loss", DoubleType()) \
    .add("Latitude", DoubleType()) \
    .add("Longitude", DoubleType())

# Read Kafka stream
df_raw = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "disaster-alerts") \
    .option("startingOffsets", "latest") \
    .load()

# Extract and parse the JSON value
df_json = df_raw.selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), schema).alias("data")) \
    .select("data.*")

# Define prediction logic
def predict_severity(batch_df, batch_id):
    if batch_df.count() == 0:
        return

    pdf = batch_df.toPandas().fillna(0)

    try:
        # Predict severity
        X = pdf[['Latitude', 'Longitude', 'Magnitude']]
        predictions = model.predict(X)
        pdf['Predicted_Severity'] = predictions

        # ✅ Print results with Disaster_ID
        print("\n=========== 🔮 Disaster Prediction Batch ===========")
        print(pdf[['Disaster_ID', 'Disaster_Type', 'Location', 'Magnitude', 'Latitude', 'Longitude', 'Date', 'Predicted_Severity']])
        print("====================================================\n")

        # ✅ Save critical alerts
        critical_alerts = pdf[pdf['Predicted_Severity'] > 8]
        if not critical_alerts.empty:
            output_file = "critical_alerts.csv"
            write_header = not os.path.exists(output_file)
            critical_alerts.to_csv(output_file, mode='a', header=write_header, index=False)
            print(f"⚠️  {len(critical_alerts)} critical alerts saved to {output_file}")

    except Exception as e:
        print(f"❌ Error during prediction: {e}")

# Start the streaming query
query = df_json.writeStream \
    .outputMode("append") \
    .foreachBatch(predict_severity) \
    .start()

query.awaitTermination()
