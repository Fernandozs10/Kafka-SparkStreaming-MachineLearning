Real-Time & Batch Taxi Price Prediction with Kafka, AWS y Dashboard

Este proyecto implementa un pipeline completo para la predicción de precios de viajes en taxi utilizando Apache Kafka para la ingesta en tiempo real, tres consumidores en Python para el procesamiento batch, almacenamiento en AWS S3 y un dashboard HTML interactivo para visualizar los resultados.

Características principales

1. Productor Kafka (Producer)

El productor simula el envío continuo de registros de viajes (rides) al tópico prediction. Cada registro contiene los atributos necesarios para la predicción del precio.

Atributos de los Registros:

distance: Distancia del viaje.

hour, day, month: Componentes temporales.

temperature: Temperatura ambiente.

precipIntensity: Intensidad de precipitación.

surge_multiplier: Multiplicador de tarifa dinámica.

latitude, longitude: Coordenadas del viaje.

cab_type, name: Tipo de vehículo y servicio.

predicted_price: El precio final predicho.

2. Consumer Python → JSON → S3

Un consumidor de Kafka escrito en Python (consumer_prediction.py) escucha el tópico prediction.

Recopila un batch de mensajes.

Genera un archivo JSON con todos los registros consumidos: rides_predictions.json.

Sube el archivo JSON a AWS S3 en la ruta especificada.

Cada ejecución del consumidor sobrescribe el archivo en S3 con el batch de predicciones más reciente.

Ruta de destino en S3:

s3://mybuckbig123/batch_results/rides_predictions.json


3. Dashboard HTML + Chart.js

El dashboard es una aplicación web estática (index.html) que utiliza la librería Chart.js.

Lee el archivo rides_predictions.json (asumiendo que se ha descargado localmente desde S3 o se ha copiado al directorio del dashboard).

Visualiza los datos de las predicciones a través de diferentes gráficos.

Ejemplos de Métricas Visualizadas:

Precio predicho por tipo de viaje (cab_type).

Distribución de la distancia por viaje.

Gráfico de temperatura vs. precio.

Impacto del surge_multiplier en los precios.

Cualquier métrica adicional que se extraiga del JSON.
