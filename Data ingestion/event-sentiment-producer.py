# ==========================================
# Imports
# ==========================================

from azure.eventhub import EventHubProducerClient, EventData
import json
import pandas as pd
from urllib.parse import urlsplit

# ==========================================
# Event Hub Connection String
# ==========================================

CONNECTION_STRING = (
    "Endpoint=sb://event-real-time-sentiment-project.servicebus.windows.net/;"
    "SharedAccessKeyName=sentiment-demo-policy;"
    "SharedAccessKey=xxxxxxxxxxxxxxxxxxxxxxxxxxxx"
)

EVENT_HUB_NAME = "event-hub"

producer = EventHubProducerClient.from_connection_string(
    conn_str=CONNECTION_STRING,
    eventhub_name=EVENT_HUB_NAME
)

# ==========================================
# Bronze CSV SAS URL
# ==========================================

storage_sas_url = dbutils.secrets.get(
    "azure_storage",
    "bronze_sentiment_raw_sas_url"
)

parts = urlsplit(storage_sas_url)
file_url = (
    storage_sas_url
    if parts.path.endswith("/bronze_sentiment_raw.csv")
    else f"{parts.scheme}://{parts.netloc}{parts.path.rstrip('/')}/sentiment_raw/bronze_sentiment_raw.csv?{parts.query}"
)

# ==========================================
# Read CSV from Blob using pandas
# ==========================================

df = pd.read_csv(file_url)

print(f"Loaded {len(df)} records from ADLS.")
print("Starting Sentiment Producer...")

# ==========================================
# Create Event Hub Batch
# ==========================================

batch = producer.create_batch()

record_count = 0

# ==========================================
# Send Records to Event Hub
# ==========================================

for _, row in df.iterrows():

    # Convert entire row to dictionary
    # No manual metadata detection required
    event = row.to_dict()

    # Convert NaN values to None
    event = {
        key: (None if str(value) == "nan" else value)
        for key, value in event.items()
    }

    # Convert timestamp / other non-JSON objects safely
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

        # Add current event to new batch
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

print(f"Successfully sent {record_count} records to Event Hub.")
