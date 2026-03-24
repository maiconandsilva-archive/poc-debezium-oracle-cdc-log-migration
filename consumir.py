from kafka import KafkaConsumer
import json
import os

# Debezium Oracle topic pattern is usually: <topic.prefix>.<schema>.<table>
TOPIC_NAME = os.getenv('TOPIC_NAME', 'server1.TEST.CUSTOMERS')
BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:29092').split(',')

consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers=BOOTSTRAP_SERVERS,
    auto_offset_reset='earliest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print(f"Listening on topic {TOPIC_NAME}...")

with open("captura_cdc_time.json", "a") as f:
    try:
        for message in consumer:
            print(f"Full Message received: {message.value}") # Check if 'payload' is actually there
            payload = message.value.get('payload', {})
            data_after = payload.get('after')
            
            if data_after:
                nome_com_tempo = data_after['FIRST_NAME']
                # O campo 'source' do Debezium contém o timestamp do Oracle
                ts_ms = payload.get('source', {}).get('ts_ms')
                
                print(f"Captured from Kafka: {nome_com_tempo} | DB_Timestamp: {ts_ms}")
                
                # Salva o JSON bruto para análise profunda posterior
                f.write(json.dumps(message.value) + "\n")
                f.flush()

    except KeyboardInterrupt:
        print("\nStopped.")
