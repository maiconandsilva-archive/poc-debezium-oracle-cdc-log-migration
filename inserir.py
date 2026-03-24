import oracledb
import time
from datetime import datetime

# Configurações (Ajuste se necessário)
conn_params = {
    "user": "test",
    "password": "test",
    "dsn": "localhost:1521/FREEPDB1"
}

try:
    connection = oracledb.connect(**conn_params)
    cursor = connection.cursor()
    
    print("Inserting 100 records with timestamps...")
    
    for i in range(1, 101):
        agora = datetime.now()
        timestamp_str = agora.strftime("%H:%M:%S.%f")[:-3] # HH:MM:SS.mmm
        data_str = agora.strftime("%d/%m/%Y")
        
        sql = "INSERT INTO TEST.CUSTOMERS (first_name, last_name, email) VALUES (:1, :2, :3)"
        # Usando o tempo no nome para facilitar a visualização no Kafka
        cursor.execute(sql, [f"Time_{timestamp_str}", f"Data_{data_str}", f"user{i}@poc.com"])
        
        connection.commit()
        print(f"[{i}/100] Inserted at {timestamp_str}")
        
        # Delay curto de 0.2s para gerar timestamps diferentes
        time.sleep(0.2)

    print("\nLoad completed successfully!")

except Exception as e:
    print(f"Connection or SQL error: {e}")
finally:
    if 'connection' in locals():
        connection.close()
