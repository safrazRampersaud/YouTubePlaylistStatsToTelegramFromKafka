import logging
import requests
from kafka import KafkaConsumer
import json
from driver_config import driver_config as config


def process_consumer():
    youtube_playlist_topic = config["kafka"]["youtube_playlist_topic"]
    bootstrap_servers = config["kafka"]["bootstrap_servers"]
    telegram_token = config["telegram"]["token"]
    telegram_chat_id = config["telegram"]["chat_id"]

    consumer = KafkaConsumer(
        youtube_playlist_topic,
        group_id=None,
        bootstrap_servers=[bootstrap_servers],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        consumer_timeout_ms=1000
    )

    for message in consumer:
        consumed_msg = str(message.value.decode('utf-8'))
        logging.info(f"consumed message for Telegram: {consumed_msg}")
        res = json.loads(consumed_msg)
        UNIT_KEY = next(iter(res))
        prepared_msg = f'''TITLE={res[UNIT_KEY]["TITLE"]}|VIEWS={res[UNIT_KEY]["VIEWS"]}|LIKES={res[UNIT_KEY]["LIKES"]}|COMMENTS={res[UNIT_KEY]["COMMENTS"]}'''
        url_req = "https://api.telegram.org/bot" + telegram_token + \
                  "/sendMessage" + "?chat_id=" + telegram_chat_id + \
                  "&text=" + prepared_msg
        results = requests.get(url_req)
        print(results.json())
    consumer.close()
