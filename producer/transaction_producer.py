import json
import random
import time
from datetime import datetime
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

merchants = ['Amazon', 'Walmart', 'Target', 'Starbucks', 'Netflix']
categories = ['Shopping', 'Grocery', 'Entertainment', 'Food', 'Travel']
statuses = ['approved', 'declined', 'pending']

def generate_transaction():
    return {
        'transaction_id': f'TXN{random.randint(100000, 999999)}',
        'user_id': f'USR{random.randint(1, 500)}',
        'amount': round(random.uniform(1.0, 5000.0), 2),
        'currency': 'USD',
        'merchant': random.choice(merchants),
        'category': random.choice(categories),
        'status': random.choice(statuses),
        'timestamp': datetime.utcnow().isoformat()
    }

print("Starting transaction producer...")
while True:
    txn = generate_transaction()
    producer.send('financial_transactions', value=txn)
    print(f"Sent: {txn['transaction_id']} | ${txn['amount']} | {txn['merchant']}")
    time.sleep(1)