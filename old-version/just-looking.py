from kafka import KafkaConsumer

consumer = KafkaConsumer(
    'test',
    bootstrap_servers=['172.20.0.51:9092'],
    auto_offset_reset='earliest',
    consumer_timeout_ms=5000
)

for msg in consumer:
    print(msg.value.decode('utf-8'))
    break
