import logging
from typing import List

from fastapi import FastAPI
from redis import Redis
from paho.mqtt import client as mqtt

from app.adapters.store_api_adapter import StoreApiAdapter
from app.entities.processed_agent_data import ProcessedAgentData
from config import (
    STORE_API_BASE_URL,
    REDIS_HOST,
    REDIS_PORT,
    BATCH_SIZE,
    MQTT_TOPIC,
    MQTT_BROKER_HOST,
    MQTT_BROKER_PORT,
)

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] [%(levelname)s] [%(module)s] %(message)s",
    handlers=[
        logging.FileHandler("app.log"),
        logging.StreamHandler()
    ]
)

redis_client = Redis(host=REDIS_HOST, port=REDIS_PORT)
store_gateway = StoreApiAdapter(STORE_API_BASE_URL)

app = FastAPI()


@app.post("/processed_agent_data")
async def save_processed_agent_data(processed_agent_data: ProcessedAgentData):
    print(processed_agent_data)
    redis_client.lpush(
        "processed_agent_data",
        processed_agent_data.model_dump_json()
    )
    processed_agent_data_batch: List[ProcessedAgentData] = []
    if redis_client.llen("processed_agent_data") >= BATCH_SIZE:
        for _ in range(BATCH_SIZE):
            processed_agent_data = ProcessedAgentData.model_validate_json(
                redis_client.lpop("processed_agent_data")
            )
            processed_agent_data_batch.append(processed_agent_data)
        store_gateway.save_data(processed_agent_data_batch)

    return {"status": "ok"}


client = mqtt.Client()


def on_connect(client, userdata, flags, rc):
    if rc == 0:
        logging.info("Connected to MQTT broker")
        client.subscribe(MQTT_TOPIC)
    else:
        logging.info(f"Failed to connect to MQTT broker with code: {rc}")


def on_message(client, userdata, msg):
    batch_data = None
    try:
        payload = msg.payload.decode("utf-8")
        processed_agent_data =ProcessedAgentData.model_validate_json(payload, strict=True)

        redis_client.lpush(
            "processed_agent_data",
            processed_agent_data.model_dump_json()
        )
        processed_agent_data: List[ProcessedAgentData] = []
        if redis_client.llen("processed_agent_data") >= BATCH_SIZE:
            for _ in range(BATCH_SIZE):
                processed_agent_data = ProcessedAgentData.model_validate_json(
                    redis_client.lpop("processed_agent_data")
                )
                processed_agent_data.append(processed_agent_data)
            store_gateway.save_data(processed_agent_data)
        
        return {"status": "ok"}
    except Exception as e:
        logging.info(f"Error processing MQTT message: {e}")


client.on_connect = on_connect
client.on_message = on_message
client.connect(MQTT_BROKER_HOST, MQTT_BROKER_PORT)

client.loop_start()
