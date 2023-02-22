import logging
from kafka import KafkaProducer
from json import dumps
from driver_config import driver_config as config
import requests
import json

logger = logging.getLogger(__name__)


def fetch_playlist_items_page(google_api_key, youtube_playlist_id, page_token=None):
    response = requests.get("https://www.googleapis.com/youtube/v3/playlistItems",
                            params={
                                "key": google_api_key,
                                "playlistId": youtube_playlist_id,
                                "part": "contentDetails",
                                "pageToken": page_token
                            })

    payload = json.loads(response.text)
    return payload


def fetch_playlist_items(google_api_key, youtube_playlist_id, page_token=None):
    payload = fetch_playlist_items_page(google_api_key, youtube_playlist_id, page_token)

    yield from payload.get("items")

    next_page_token = payload.get("nextPageToken")

    if next_page_token is not None:
        yield from fetch_playlist_items(google_api_key, youtube_playlist_id, next_page_token)


def fetch_videos_page(google_api_key, video_id, page_token=None):
    response = requests.get("https://www.googleapis.com/youtube/v3/videos",
                            params={
                                "key": google_api_key,
                                "id": video_id,
                                "part": "snippet,statistics",
                                "pageToken": page_token
                            })
    payload = json.loads(response.text)
    return payload


def fetch_videos(google_api_key, video_id, page_token=None):
    payload = fetch_videos_page(google_api_key, video_id, page_token)

    yield from payload.get("items")

    next_page_token = payload.get("nextPageToken")

    if next_page_token is not None:
        yield from fetch_videos(google_api_key, video_id, next_page_token)


def summarize_video(video):
    return {
        "video_id": video["id"],
        "title": video["snippet"]["title"],
        "views": int(video['statistics'].get("viewCount", 0)),
        "likes": int(video['statistics'].get("likeCount", 0)),
        "comments": int(video['statistics'].get("commentCount", 0))
    }


def on_delivery(err, record):
    pass


def process_producer():

    google_api_key = config["youtube"]["google_api_key"]
    youtube_playlist_id = config["youtube"]["youtube_playlist_id"]
    youtube_playlist_topic = config["kafka"]["youtube_playlist_topic"]
    bootstrap_servers = config["kafka"]["bootstrap_servers"]

    producer = KafkaProducer(bootstrap_servers=bootstrap_servers, value_serializer=lambda x: dumps(x).encode('utf-8'))
    for video_item in fetch_playlist_items(google_api_key, youtube_playlist_id):
        video_id = video_item["contentDetails"]["videoId"]
        for video in fetch_videos(google_api_key, video_id):
            data = {
                    video_id: {
                        "TITLE": video["snippet"]["title"],
                        "VIEWS": int(video['statistics'].get("viewCount", 0)),
                        "LIKES": int(video['statistics'].get("likeCount", 0)),
                        "COMMENTS": int(video['statistics'].get("commentCount", 0))
                    }
                }
            producer.send(topic=youtube_playlist_topic, value=data)
            logging.info(f'''data={data}''')

    producer.flush()
    producer.close()
