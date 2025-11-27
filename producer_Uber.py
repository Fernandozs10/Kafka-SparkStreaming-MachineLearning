from kafka import KafkaProducer
import json, random, time
from datetime import datetime

# clúster MSK
producer = KafkaProducer(
    bootstrap_servers=[
        "b-1.democluster1.du8hev.c19.kafka.us-east-1.amazonaws.com:9092",
        "b-2.democluster1.du8hev.c19.kafka.us-east-1.amazonaws.com:9092",
        "b-3.democluster1.du8hev.c19.kafka.us-east-1.amazonaws.com:9092"
    ],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

weather_summaries = ["clear", "light rain", "moderate rain", "snow"]
car_names = ["Sedan", "SUV", "Black Car"]

lat_min, lat_max = 42.2148, 42.3661
lon_min, lon_max = -71.1054, -71.033

while True:
    now = datetime.now()
    data = {
        "cab_type": "Uber",  # solo Uber
        "short_summary": random.choice(weather_summaries),
        "name": random.choice(car_names),
        "distance": round(random.uniform(0.5, 20), 2),
        "hour": now.hour,
        "day": now.day,
        "month": now.month,
        "temperature": round(random.uniform(-5, 35), 1),
        "precipIntensity": round(random.uniform(0, 10), 2),
        "surge_multiplier": round(random.uniform(1, 3), 1),
        "latitude": round(random.uniform(lat_min, lat_max), 6),
        "longitude": round(random.uniform(lon_min, lon_max), 6)
    }
    
    producer.send('rides_data', value=data)
    print("Lyft Enviado:", data)
    time.sleep(1)
