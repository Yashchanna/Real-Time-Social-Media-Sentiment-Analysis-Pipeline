# ==========================================
# Imports
# ==========================================

from azure.eventhub import EventHubProducerClient, EventData
import pandas as pd
import json

# ==========================================
# Event Hub Connection String
# ==========================================

CONNECTION_STRING = (
    "Endpoint=sb://event-real-time-sentiment-project.servicebus.windows.net/;"
    "SharedAccessKeyName=sentiment-demo-policy;"
    "SharedAccessKey=xxxxxxxx;"
    "EntityPath=users-hub"
)

EVENT_HUB_NAME = "users-hub"

producer = EventHubProducerClient.from_connection_string(
    conn_str=CONNECTION_STRING
)

# ==========================================
# Read CSV
# ==========================================

CSV_PATH = r"C:\Real_Time_Sentiment_Project\bronze_users_raw.csv"

df = pd.read_csv(CSV_PATH)

print(f"Loaded {len(df)} records from Bronze CSV.")
print(f"Detected schema: {list(df.columns)}")
print("Starting Users Producer...")

# ==========================================
# Create Batch
# ==========================================

batch = producer.create_batch()

record_count = 0

# ==========================================
# Send Events
# ==========================================

for _, row in df.iterrows():

    # Automatically detect entire schema
    event = row.to_dict()

    # Convert missing values
    event = {
        key: (None if pd.isna(value) else value)
        for key, value in event.items()
    }

    # Convert dictionary to JSON
    event_json = json.dumps(event, default=str)

    try:
        batch.add(EventData(event_json))
        record_count += 1

    except ValueError:
        # Current batch is full
        producer.send_batch(batch)

        print(f"{record_count} records sent...")

        # Create a new batch
        batch = producer.create_batch()

        # Add current event
        batch.add(EventData(event_json))

        record_count += 1

# ==========================================
# Send Remaining Records
# ==========================================

if len(batch) > 0:
    producer.send_batch(batch)

# ==========================================
# Close Producer
# ==========================================

producer.close()

print(
    f"Successfully sent {record_count} records "
    f"to Users Event Hub."
)
