import redis
import logging
import sys
import json
import time

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] (%(processName)-10s) %(message)s',
    datefmt='%H:%M:%S',
    handlers=[logging.StreamHandler(sys.stdout)]
)

class RedisClient:
    def __init__(self, host: str = 'localhost', port: int = 6379):
        self.instance = redis.Redis(host=host, port=port, decode_responses=True)
        self.logger = logging.getLogger(__name__)

    # methods for bm
    def init_metrics(self, request_id: str):
        key = f"metrics:{request_id}"
        self.instance.hset(key, mapping = {
            "t0": time.time(),
            "impl": "mp-bus",
            "request_id": request_id
        })
        self.instance.expire(key, 86400)
    
    def record_metric(self, request_id: str, timestamp_name: str, value: float):
        key = f"metrics:{request_id}"
        self.instance.hset(key, timestamp_name, value)
    

    def complete_metrics(self, request_id: str, implementation: str = "mp-bus"):
        key = f"metrics:{request_id}"
        self.instance.lpush(f"metrics:raw:{implementation}", request_id)
        self.logger.info(f"Completed metrics collection for {request_id}")