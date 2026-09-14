import sys
import logging
import argparse
import multiprocessing

from models import App_Channels
from server import Server

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] (%(processName)-10s) %(message)s',
    datefmt='%H:%M:%S',
    handlers=[logging.StreamHandler(sys.stdout)]
)

def run_server(name, redis_config, channels):
    server = Server(name, redis_config, channels)
    server.start()


class System:
    def __init__(self, total_servers: int):
        self._logger = logging.getLogger(__name__)
        self._redis_config = {
            "host": "localhost",
            "port": 6379
        }

        # self._queue = multiprocessing.Queue() # was maybe a leftover. doesnt seem to have any role
        self._channels = App_Channels(request_channel="broadcast:requests")        

        self._logger.info("Spinning up servers..")
        for i in range(total_servers):
            server_name = f"SERV-{i}"
            multiprocessing.Process(
                target=run_server,
                args=(server_name, self._redis_config, self._channels),
                name=server_name
            ).start()
            self._logger.info(f"Server {server_name} initialized successfully..")

        self._logger.info("Successfully initialized system!")
    

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--servers", type=int, required=True) 

    args = parser.parse_args()
    System(args.servers)


if __name__ == '__main__':
    main()