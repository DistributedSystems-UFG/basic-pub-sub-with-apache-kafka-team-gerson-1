from kafka import KafkaProducer
from const import *
import sys

try:
    topics = sys.argv[1:]
except Exception:
    print ('Usage: python3 producer <topic_name>')
    exit(1)
    
print(BROKER_ADDR)
producer = KafkaProducer(bootstrap_servers=[f'{BROKER_ADDR}:{BROKER_PORT}'])

for topic in topics:
    for i in range(100):
        msg = f'My {str(i)}st message for topic {topic} '
        print (f'Sending message: {msg}')
        producer.send(topic, value=msg.encode())

producer.flush()
