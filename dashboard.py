import streamlit as st
from kafka import KafkaConsumer
import pandas as pd
import json
import plotly.express as px
import time

TOPIC_NAME = "hotSpotsgra"
BOOTSTRAP_SERVERS = [
    "b-1.democluster1.du8hev.c19.kafka.us-east-1.amazonaws.com:9092",
    "b-2.democluster1.du8hev.c19.kafka.us-east-1.amazonaws.com:9092",
    "b-3.democluster1.du8hev.c19.kafka.us-east-1.amazonaws.com:9092"
]

st.set_page_config(page_title="Real-Time Uber/Lyft Hotspots", layout="wide")
st.title(" Detector de Hotspots en Vivo (Modo Polling)")

@st.cache_resource
def get_consumer():
    return KafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        auto_offset_reset='earliest',
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        group_id="streamlit-viewer-v1" # Group ID ayuda a no perder el hilo
    )

try:
    consumer = get_consumer()
except Exception as e:
    st.error(f"Error conectando a Kafka: {e}")
    st.stop()

map_placeholder = st.empty()
metrics_placeholder = st.empty()

if 'data_buffer' not in st.session_state:
    st.session_state['data_buffer'] = pd.DataFrame(columns=['lat', 'lon', 'intensity', 'time'])

st.write(f"Escuchando tópico: `{TOPIC_NAME}`... El mapa se actualizará abajo.")

try:
    while True:
        message_batch = consumer.poll(timeout_ms=1000)

        if message_batch:
            for topic_partition, messages in message_batch.items():
                for message in messages:
                    new_data = message.value

                    row = {
                        'lat': float(new_data['lat']),
                        'lon': float(new_data['lon']),
                        'intensity': int(new_data['intensity']),
                        'time': new_data['time']
                    }

                    # Añadir al buffer
                    new_df = pd.DataFrame([row])
                    st.session_state['data_buffer'] = pd.concat(
                        [st.session_state['data_buffer'], new_df],
                        ignore_index=True
                    )

            if len(st.session_state['data_buffer']) > 500:
                st.session_state['data_buffer'] = st.session_state['data_buffer'].iloc[-500:]
            with map_placeholder.container():
                if not st.session_state['data_buffer'].empty:
                    fig = px.density_mapbox(
                        st.session_state['data_buffer'],
                        lat='lat',
                        lon='lon',
                        z='intensity',
                        radius=25,
                        center=dict(lat=42.3601, lon=-71.0589),
                        zoom=11,
                        mapbox_style="open-street-map",
                        color_continuous_scale="Viridis"
                    )
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.info("Esperando primeros datos...")

            with metrics_placeholder.container():
                if not st.session_state['data_buffer'].empty:
                    latest_time = st.session_state['data_buffer']['time'].iloc[-1]
                    count = len(st.session_state['data_buffer'])
                    st.metric(label="Última Actualización", value=str(latest_time), delta=f"{count} puntos activos")

        time.sleep(0.2)

except KeyboardInterrupt:
    st.write("Dashboard detenido.")
