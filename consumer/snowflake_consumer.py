import json
import os
from dotenv import load_dotenv
from kafka import KafkaConsumer
import snowflake.connector

load_dotenv()

conn = snowflake.connector.connect(
    account=os.getenv('SNOWFLAKE_ACCOUNT'),
    user=os.getenv('SNOWFLAKE_USER'),
    password=os.getenv('SNOWFLAKE_PASSWORD'),
    warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
    database=os.getenv('SNOWFLAKE_DATABASE'),
    schema=os.getenv('SNOWFLAKE_SCHEMA')
)
cursor = conn.cursor()

consumer = KafkaConsumer(
    'financial_transactions',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='earliest',
    group_id='fintech-group'
)

print("Consumer started, waiting for messages...")
for message in consumer:
    txn = message.value
    cursor.execute("""
        INSERT INTO RAW_TRANSACTIONS
        (transaction_id, user_id, amount, currency,
         merchant, category, status, timestamp)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
    """, (
        txn['transaction_id'], txn['user_id'],
        txn['amount'], txn['currency'],
        txn['merchant'], txn['category'],
        txn['status'], txn['timestamp']
    ))
    conn.commit()
    print(f"Inserted: {txn['transaction_id']} into Snowflake")
