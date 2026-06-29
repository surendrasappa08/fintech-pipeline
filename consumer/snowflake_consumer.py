import json
from kafka import KafkaConsumer
import snowflake.connector

# Snowflake connection
conn = snowflake.connector.connect(
    account='DU35004.us-east-2.aws',
    user='SURENDRA08',
    password='Surendra@080320',
    warehouse='FINTECH_WH',
    database='FINTECH_DB',
    schema='TRANSACTIONS'
)
cursor = conn.cursor()

# Kafka consumer
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
