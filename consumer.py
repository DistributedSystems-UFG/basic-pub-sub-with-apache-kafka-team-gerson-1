from kafka import KafkaConsumer
from const import *
import sys

try:
  topics = sys.argv[1:]
except Exception:
  print ('Usage: python3 consumer <topic_name>')
  exit(1)

consumer = KafkaConsumer(bootstrap_servers=[f'{BROKER_ADDR}:{BROKER_PORT}'])

print(f'Starting to listen on {topics}')
consumer.subscribe(topics)
for msg in consumer:
    print (msg.value)
