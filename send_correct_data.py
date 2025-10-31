import json
import requests
from kafka import KafkaProducer

# Configuration
KAFKA_SERVER = 'localhost:9092'
TOPIC = 'airports'
API_KEY = '21abb329efbf2d1b48917ee728d40144'
API_URL = 'https://api.core.openaip.net/api/airports'

print("=== ENVOI DES DONNÉES OPENAIP CORRECTES ===")

# 1. Récupérer les vraies données de l'API
headers = {'X-OpenAIP-API-Key': API_KEY}
response = requests.get(API_URL, headers=headers)

if response.status_code == 200:
    data = response.json()
    items = data.get('items', [])
    print(f"✅ {len(items)} aéroports récupérés de l'API OpenAIP")
    
    # 2. Se connecter à Kafka
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_SERVER],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    
    # 3. Envoyer les données CORRECTES
    count = 0
    for airport in items:
        # Vérifier que l'aéroport a des coordonnées valides
        if (airport.get('geometry') and 
            airport.get('geometry', {}).get('coordinates') and 
            len(airport['geometry']['coordinates']) >= 2):
            
            # Créer le message avec la structure CORRECTE
            message = {
                '_id': airport.get('_id', ''),
                'name': airport.get('name', ''),
                'country': airport.get('country', ''),
                'geometry': airport.get('geometry', {})
            }
            
            producer.send(TOPIC, value=message)
            count += 1
            
            if count <= 5:  # Afficher les 5 premiers
                coords = airport['geometry']['coordinates']
                print(f"✈️  {count}: {message['name']} - Lat: {coords[1]}, Lon: {coords[0]}")
    
    producer.flush()
    print(f"🎯 {count} aéroports valides envoyés à Kafka")
    
else:
    print(f"❌ Erreur API: {response.status_code}")
    print(response.text)
