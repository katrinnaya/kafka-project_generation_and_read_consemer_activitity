from random import randint, choice
from datetime import datetime
from json import dumps
from kafka import KafkaProducer
from time import sleep

def generate_user_action():
    user_ids = list(range(5))  # Предположим, что у нас 5 уникальных пользователей
    actions = ["login", "logout", "purchase", "click"]
    
    return {
        "user_id": choice(user_ids),
        "action": choice(actions),
        "timestamp": datetime.now().isoformat()
    }

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: dumps(x).encode('utf-8')
)

if __name__ == "__main__":
    while True:
        data = generate_user_action()
        print(f"Sending message: {data}")
        producer.send("example_topic", value=data)
        producer.flush()  # Отправляет сообщения немедленно
        # Пауза перед генерацией следующего события
        sleep(randint(1, 5))
