import redis
import logging
from typing import Callable
from models import App_Channels, Request
import sys
import json

from request_lifecycle import Status


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] (%(processName)-10s) %(message)s',
    datefmt='%H:%M:%S',
    handlers=[logging.StreamHandler(sys.stdout)]
)


class RedisClient:
    def __init__(self, channels: App_Channels, host: str = 'localhost', port: int = 6379):
        self.instance = redis.Redis(host=host, port=port, decode_responses=True)
        self.channels = channels
        self.logger = logging.getLogger(__name__)
        self._pubsub = self.instance.pubsub()

    def pub_request(self, request: Request):
        channel = self.channels.request_channel 
        self.instance.publish(channel, request.model_dump_json())
        self.logger.info(f"Published {request.id} to {channel}")

    def start_listening(self, callback: Callable[[dict], None]):
        channel = self.channels.request_channel
        self._pubsub.subscribe(channel)

        for message in self._pubsub.listen():
            if message["type"] == "message":
                callback(message)

    def create_request_hash(self, request: Request, expirytime: int):
        key = f"request_data:{request.id}"
        self.instance.hset(key, mapping={
            "id": request.id,
            "client_id": request.client_id,
            "content": request.content,
            "status": Status.PENDING.value,
            "created_at": request.timestamp,
            "duration": f"{0}s"
        })
        self.instance.expire(key, expirytime)

    def update_request_hash(self, request_id: str, data: dict):
        key = f"request_data:{request_id}"
        self.instance.hset(key, mapping=data)

    def create_claim(self, request_id: str, name: str):
        key = f"claim:{request_id}"
        return self.instance.set(key, name, nx=True, ex=60)
    
    def publish_progress(self, request_id: str, data: dict):
        channel = f"progress:{request_id}"
        self.instance.publish(channel, json.dumps(data))

    def stream_progress(self, request_id: str):
        pubsub = self.instance.pubsub()
        pubsub.subscribe(f"progress:{request_id}")

        try:
            for message in pubsub.listen():
                if message['type'] != 'message':
                    continue
                
                data = message['data']
                self.logger.info(f"Stream payload for {request_id}: {data}")
                yield f"data: {data}\n\n"
        finally:
            pubsub.close()

    def get_completed_requests(self, limit: int = 20):
        ids = self.instance.lrange("requests:completed", 0, limit - 1)
        self.logger.info(f"Retrieved {len(ids)} completed request IDs")

        results = []
        for req_id in ids:
            data = self.instance.hgetall(f"request_data:{req_id}")
            if not data:
                continue
            results.append(data)

        return results