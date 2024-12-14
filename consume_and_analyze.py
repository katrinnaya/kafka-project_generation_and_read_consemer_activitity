from collections import defaultdict, Counter
from json import loads
from kafka import KafkaConsumer

consumer = KafkaConsumer(
    'example_topic',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',  # Читаем с начала топика
    enable_auto_commit=True,
    group_id='my-group',
    value_deserializer=lambda x: loads(x.decode('utf-8'))
)

user_actions = defaultdict(Counter)

try:
    for message in consumer:
        data = message.value
        user_id = data['user_id']
        action = data['action']
        user_actions[user_id].update([action])
except KeyboardInterrupt:
    pass

finally:
    print("\nActions by users:")
    for user_id, actions in user_actions.items():
        print(f"\nUser {user_id}:")
        for action, count in actions.items():
            print(f"{action}: {count}")

