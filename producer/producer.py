import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer

# Configurações do Kafka
KAFKA_TOPIC = 'ecommerce_sales'
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'

# Inicializa o Producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Categorias e produtos para dados fakes
CATEGORIES = ['Electronics', 'Home & Garden', 'Fashion', 'Sports', 'Books']
PRODUCTS = {
    'Electronics': ['Smartphone', 'Laptop', 'Headphones', 'Smartwatch'],
    'Home & Garden': ['Coffee Maker', 'Desk Lamp', 'Garden Tools', 'Cushion'],
    'Fashion': ['T-shirt', 'Jeans', 'Sneakers', 'Jacket'],
    'Sports': ['Yoga Mat', 'Dumbbells', 'Running Shoes', 'Water Bottle'],
    'Books': ['Fiction Novel', 'Cookbook', 'History Book', 'Sci-Fi Book']
}

def generate_sale():
    category = random.choice(CATEGORIES)
    product = random.choice(PRODUCTS[category])
    return {
        'order_id': random.randint(10000, 99999),
        'customer_id': random.randint(1, 1000),
        'product_name': product,
        'category': category,
        'price': round(random.uniform(10.0, 1000.0), 2),
        'quantity': random.randint(1, 5),
        'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    }

if __name__ == "__main__":
    print(f"Iniciando envio de dados para o tópico: {KAFKA_TOPIC}")
    try:
        while True:
            sale = generate_sale()
            producer.send(KAFKA_TOPIC, sale)
            print(f"Venda enviada: {sale}")
            time.sleep(random.uniform(0.5, 2.0))  # Simula tempo real variável
    except KeyboardInterrupt:
        print("Encerrando o Producer...")
    finally:
        producer.close()
