import logging
import sys
import random
import time
import json

from redis_client import RedisClient
from request_lifecycle import Status

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] (%(processName)-10s) %(message)s',
    datefmt='%H:%M:%S',
    handlers=[logging.StreamHandler(sys.stdout)]
)

class Server:
    def __init__(self, name, redis_config: dict, channels):
        self.name = name
        self._logger = logging.getLogger(__name__)

        self._redis_client = RedisClient(
            channels=channels,
            **redis_config
        )

    def start(self):
        self._redis_client.start_listening(
            callback=self._handle_incoming_request
        )

    def _handle_incoming_request(self, message):
        request = json.loads(message['data'])
        req_id = request['id']
        
        # For benchmarking, process all requests (no random skipping)
        self._logger.info(f"Server {self.name}: Interested in {req_id}. Processing...")
        time.sleep(random.uniform(0.1, 0.5))

        if self._redis_client.create_claim(req_id, self.name):
            self._logger.info(f"*** Server {self.name}: WON THE RACE for {req_id}! ***")

            self._redis_client.record_metric(req_id, "t2", time.time())
        
            update_data = {
                "status": Status.PICKED_UP.value,
                "picked_up_by": self.name
            }
            self._redis_client.update_request_hash(req_id, update_data)

            # Once updated, now we need to do some progress on the task
            # Reduced processing time for benchmarking (5-10 seconds instead of 20-25)
            duration = random.randint(5, 10)
            self._redis_client.publish_progress(req_id, {
                "event": "picked_up",
                "server": self.name
            })

            self._redis_client.record_metric(req_id, "t3", time.time())

            for i in range(duration):
                time.sleep(1)
                self._redis_client.publish_progress(req_id, {
                    "event": "progress",
                    "server": self.name,
                    "step": i + 1,
                    "total": duration
                })

            self._redis_client.publish_progress(req_id, {
                "event": "completed",
                "server": self.name
            })

            self._redis_client.record_metric(req_id, "t4", time.time())
            
            self._redis_client.update_request_hash(req_id, {
                "status": Status.COMPLETED.value,
                "completed_at": time.time(),
                "duration": f"{duration}s"
            })

            self._redis_client.instance.lpush("requests:completed", req_id)

            self._redis_client.complete_metrics(req_id)
            
        else:
            self._logger.info(f"Server {self.name}: LOST. Request taken by another server")