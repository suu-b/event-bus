from fastapi import FastAPI
from fastapi.responses import StreamingResponse
from models import Request
import uvicorn
import logging
import sys
import multiprocessing
import time
import random
import asyncio
import json
import argparse

from models import App_Channels
from fastapi.middleware.cors import CORSMiddleware
from redis_client import RedisClient

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] (%(processName)-10s) %(message)s',
    datefmt='%H:%M:%S',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)


def init_global_scope():
    manager = multiprocessing.Manager()
    return {
        "manager": manager,
        "requests": manager.dict(),
        "request_locks": manager.dict(),
        "broadcast": manager.Condition()
    }


def garbage_collector(global_scope):
    TTL_SECONDS = 36000

    while True:
        time.sleep(60)

        current_time = time.time()
        keys_to_delete = []

        for req_id, req_data in dict(global_scope['requests']).items():
            is_processed = req_data['status'] == 'completed'
            is_expired = (current_time - req_data['timestamp']) > TTL_SECONDS

            if is_processed and is_expired:
                keys_to_delete.append(req_id)

        if keys_to_delete:
            for req_id in keys_to_delete:
                if req_id in global_scope['requests']:
                    del global_scope['requests'][req_id]

                if req_id in global_scope['request_locks']:
                    del global_scope['request_locks'][req_id]

            logger.info(
                f"GC: Successfully cleared {len(keys_to_delete)} completed requests."
            )


def server_process(name, global_scope):
    logger.info(f"--- Server {name} is now on VIGIL ---")

    redis_client = RedisClient()

    while True:
        with global_scope['broadcast']:
            global_scope['broadcast'].wait()

        current_keys = [
            req_id
            for req_id, req_data in dict(global_scope['requests']).items()
            if req_data['status'] == 'pending'
        ]

        for req_id in current_keys:
            if req_id not in global_scope['requests']:
                continue

            req = global_scope['requests'][req_id]

            if req['status'] != 'pending':
                continue

            decision = random.choice([0, 1])

            if decision == 1:
                delay = random.uniform(1, 2)
                time.sleep(delay)

                if req_id not in global_scope['request_locks']:
                    continue

                with global_scope['request_locks'][req_id]:
                    if req_id not in global_scope['requests']:
                        continue

                    req = global_scope['requests'][req_id]

                    if req['status'] == 'pending':
                        local_copy = req.copy()
                        local_copy['status'] = 'picked_up'
                        local_copy['picked_up_by'] = name

                        redis_client.record_metric(
                            req_id,
                            "t2",
                            time.time()
                        )

                        global_scope['requests'][req_id] = local_copy

                        logger.info(
                            f"*** Server {name}: CLAIMED {req_id}! ***"
                        )

                        process_time = random.randint(20, 25)
                        time.sleep(process_time)

                        if req_id not in global_scope['requests']:
                            continue

                        completed_copy = local_copy.copy()
                        completed_copy['status'] = 'completed'
                        completed_copy['completed_at'] = time.time()
                        completed_copy['duration'] = process_time

                        global_scope['requests'][req_id] = completed_copy

                        redis_client.record_metric(req_id,"t3",time.time())
                        redis_client.complete_metrics(req_id)

                        logger.info(
                            f"Server {name}: COMPLETED {req_id} "
                            f"after {process_time:.2f}s"
                        )

                    else:
                        logger.info(
                            f"Server {name}: Too slow for {req_id} "
                            f"(already taken)"
                        )
            else:
                logger.info(
                    f"Server {name}: Decided to skip {req_id}"
                )


def gateway_process(gateway_queue, global_scope):
    logger.info("Gateway is listening...")

    redis_client = RedisClient()

    while True:
        request = gateway_queue.get()

        if request is None:
            break

        req_id = request['id']

        logger.info(
            f"Gateway: Posting {request['id']} to the board."
        )

        global_scope['request_locks'][req_id] = (
            global_scope['manager'].Lock()
        )

        global_scope['requests'][req_id] = request
        redis_client.record_metric(
            request['id'],
            "t1",
            time.time()
        )

        with global_scope['broadcast']:
            global_scope['broadcast'].notify_all()


class System:
    def __init__(self, total_servers: int):
        self._logger = logging.getLogger(__name__)

        global_scope = init_global_scope()
        self._global_scope = global_scope
        self._logger.info("Global scope initialized")

        self.gateway_queue = multiprocessing.Queue()

        # self._queue = multiprocessing.Queue() # was maybe a leftover. doesnt seem to have any role
        self._channels = App_Channels(
            request_channel="broadcast:requests"
        )

        self._redis_client = RedisClient()
        self._processes = []

        self._logger.info("Spinning up servers..")

        for i in range(total_servers):
            server_name = f"Srv-{i}"

            process = multiprocessing.Process(
                target=server_process,
                args=(server_name, global_scope),
                name=server_name,
                daemon=True
            )

            process.start()
            self._processes.append(process)

            self._logger.info(
                f"Server {server_name} initialized successfully.."
            )

        logger.info("Spinning up the gateway")

        gateway = multiprocessing.Process(
            target=gateway_process,
            args=(self.gateway_queue, global_scope),
            name="Gateway",
            daemon=True
        )

        gateway.start()
        self._processes.append(gateway)

        logger.info("Spinning up the garbage collector")

        gc_process = multiprocessing.Process(
            target=garbage_collector,
            args=(global_scope,),
            name="GarbageCollector",
            daemon=True
        )

        gc_process.start()
        self._processes.append(gc_process)

        self._logger.info("Successfully initialized system!")

    def create_new_request(self, client_name):
        req_id = (
            f"REQ-{client_name}-"
            f"{time.time_ns()}-"
            f"{random.randint(1000, 9999)}"
        )

        created_at = time.time()

        self._redis_client.record_metric(req_id,"t0",created_at)

        request = {
            "id": req_id,
            "client_id": client_name,
            "status": "pending",
            "picked_up_by": None,
            "timestamp": created_at
        }

        self.gateway_queue.put(request)

        return request


app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], 
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

system = None

@app.post("/request")
def create_request(req: Request):
    logger.info(f"Received request payload: {req.model_dump()}")
    if system is None:
        return {"status": "error", "message": "System not initialized"}
    new_request = system.create_new_request(req.client_id)
    logger.info(f"Request {new_request['id']} submitted successfully")
    return {"status": "submitted", "request_id": new_request['id']}

@app.get("/stream/{request_id}")
async def stream(request_id: str):
    if system is None:
        return {"status": "error", "message": "System not initialized"}
    
    async def event_generator():
        while True:
            if request_id in system._global_scope['requests']:
                request_data = dict(
                    system._global_scope['requests'][request_id]
                )

                current_status = request_data['status']

                # Format the message to be more client-friendly
                if current_status == 'pending':
                    message = "Request pending"
                elif current_status == 'picked_up':
                    message = (
                        f"Request picked up by "
                        f"{request_data.get('picked_up_by')}"
                    )
                elif current_status == 'completed':
                    message = (
                        f"Request completed by "
                        f"{request_data.get('picked_up_by')}"
                    )
                else:
                    message = (
                        f"Request status: {current_status}"
                    )

                data = {
                    "request_id": request_data['id'],
                    "status": current_status,
                    "picked_up_by": request_data.get(
                        'picked_up_by'
                    ),
                    "timestamp": request_data['timestamp'],
                    "message": message
                }

                yield f"data: {json.dumps(data)}\n\n"

                if current_status == 'completed':
                    break

            else:
                yield (
                    f"data: {json.dumps({'status': 'pending', 'message': 'Waiting for request...'})}\n\n"
                )

            await asyncio.sleep(1)
    
    return StreamingResponse(event_generator(), media_type="text/event-stream")

@app.get("/history")
def get_history(limit: int = 100):
    logger.info(f"Fetching history with limit: {limit}")
    if system is None:
        return {"status": "error", "message": "System not initialized"}
    
    requests_data = [k for k in system._global_scope['requests'].values() if k['status'] == 'completed']

    results = []
    for data in requests_data:        
        result_item = {
            "request_id": data.get("id"),
            "client_id": data.get("client_id"),
            "status": data.get("status"),
            "server_id": data.get("picked_up_by"),
            "created_at": data.get("timestamp"),
            "duration": data.get("duration"),
            "completed_at": data.get("completed_at"),
        }
        logger.debug(f"History payload: {result_item}")
        results.append(result_item)

    # Apply limit
    results = results[:limit]
    
    logger.info(f"Returning {len(results)} historical records")
    return results

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Event Dispatch Engine with multiprocessing')
    parser.add_argument('--servers', type=int, default=3, help='Number of server processes to spawn')
    parser.add_argument('--port', type=int, default=8000, help='Port to run the FastAPI server on')
    args = parser.parse_args()
    
    multiprocessing.set_start_method('fork')
    multiprocessing.freeze_support()
    system = System(total_servers=args.servers)
    uvicorn.run(app=app, host='0.0.0.0', port=args.port)