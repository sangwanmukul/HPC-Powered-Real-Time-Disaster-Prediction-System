from kafka import KafkaProducer
import pandas as pd
import time
import json
import numpy as np
import sys

# Load main and cleaned datasets
raw_df = pd.read_csv('natural_disasters_2024.csv')
geo_df = pd.read_csv('cleaned_disasters.csv')

# Clean and rename raw dataset
raw_df.columns = [col.strip() for col in raw_df.columns]
raw_df.rename(columns={'Economic_Loss($)': 'Economic_Loss'}, inplace=True)

# Add Disaster_ID to geo_df (assuming matching order for first N rows)
geo_df = geo_df.copy()
geo_df['Disaster_ID'] = raw_df['Disaster_ID']

# Merge by Disaster_ID
merged_df = pd.merge(raw_df, geo_df[['Disaster_ID', 'Latitude', 'Longitude']], on='Disaster_ID', how='left')

# Fix data types
merged_df['Magnitude'] = pd.to_numeric(merged_df['Magnitude'], errors='coerce').fillna(0)
merged_df['Fatalities'] = pd.to_numeric(merged_df['Fatalities'], errors='coerce').fillna(0)
merged_df['Economic_Loss'] = pd.to_numeric(merged_df['Economic_Loss'], errors='coerce').fillna(0)
merged_df['Date'] = pd.to_datetime(merged_df['Date'], errors='coerce').dt.strftime('%Y-%m-%d %H:%M:%S')
merged_df['Date'] = merged_df['Date'].fillna('1970-01-01 00:00:00')

# Convert to records
records = merged_df.to_dict(orient='records')

# Initialize Kafka producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

# Initialize performance tracking
success_count = 0
failure_count = 0
send_times = []
record_sizes = []

print("🚀 Sending disaster events to Kafka topic: 'disaster-alerts'...")
start_time = time.time()

for i, record in enumerate(records):
    try:
        record_json = json.dumps(record)
        byte_data = record_json.encode('utf-8')
        record_size = sys.getsizeof(byte_data)
        send_start = time.time()

        producer.send('disaster-alerts', value=record)
        producer.flush()

        send_end = time.time()
        elapsed = send_end - send_start
        send_times.append(elapsed)
        record_sizes.append(record_size)

        success_count += 1
        print(f"✅ Sent ({i + 1}/{len(records)}): {record}")
        time.sleep(1)

    except Exception as e:
        failure_count += 1
        print(f"❌ Failed to send record ({i + 1}): {e}")

end_time = time.time()

# Performance stats
total_time = end_time - start_time
avg_speed = success_count / total_time if total_time > 0 else 0
total_bytes_sent = sum(record_sizes)
throughput = total_bytes_sent / total_time if total_time > 0 else 0

# Advanced stats
send_times_np = np.array(send_times)
min_send = np.min(send_times_np)
max_send = np.max(send_times_np)
mean_send = np.mean(send_times_np)
median_send = np.median(send_times_np)
std_send = np.std(send_times_np)

# Output performance report
print("\n📊 Kafka Producer Performance Metrics:")
print(f"🔢 Total Records: {len(records)}")
print(f"✅ Successfully Sent: {success_count}")
print(f"❌ Failed Sends: {failure_count}")
print(f"⏱️ Total Time Taken: {total_time:.2f} seconds")
print(f"⚡ Average Send Speed: {avg_speed:.2f} records/second")
print(f"📦 Total Data Sent: {total_bytes_sent / 1024:.2f} KB")
print(f"🚀 Throughput: {throughput:.2f} bytes/second")
print(f"📈 Min Send Time: {min_send:.4f} sec")
print(f"📉 Max Send Time: {max_send:.4f} sec")
print(f"📊 Mean Send Time: {mean_send:.4f} sec")
print(f"📏 Median Send Time: {median_send:.4f} sec")
print(f"📐 Std Dev Send Time: {std_send:.4f} sec")

print("\n🎉 All disaster events have been sent to Kafka.")
