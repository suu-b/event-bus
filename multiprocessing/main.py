import multiprocessing
import sys
import time
import random
import logging


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
        "broadcast": manager.Condition(),
    }

def create_new_request(client_name):
    return {
        "id": f"REQ-{client_name}-{int(time.time())}",
        "content": f"Task from {client_name}",
        "status": "pending", 
        "picked_up_by": None,
        "timestamp": time.time()
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
            
            logger.info(f"GC: Successfully cleared {len(keys_to_delete)} completed requests.")

def server_process(name, global_scope):
    logger.info(f"--- Server {name} is now on VIGIL ---")

    while True:
        with global_scope['broadcast']:
            global_scope['broadcast'].wait()

        current_keys = [
            req_id for req_id, req_data in dict(global_scope['requests']).items() 
            if req_data['status'] == 'pending'
        ]
        
        for req_id in current_keys:
            req = global_scope['requests'][req_id]

            if req['status'] == 'picked_up':
                continue
    
            decision = random.choice([0, 1])
            if decision == 1:
                delay = random.uniform(1, 2)
                time.sleep(delay)

                with global_scope['request_locks'][req_id]:
                    req = global_scope['requests'][req_id]
                    
                    if req['status'] == 'pending':
                        local_copy = req.copy()
                        local_copy['status'] = 'picked_up'
                        local_copy['picked_up_by'] = name
                        
                        global_scope['requests'][req_id] = local_copy
                        
                        logger.info(f"*** Server {name}: CLAIMED {req_id}! ***")
                        
                        process_time = random.uniform(1, 3)
                        time.sleep(process_time)
                        
                        completed_copy = local_copy.copy()
                        completed_copy['status'] = 'completed'
                        global_scope['requests'][req_id] = completed_copy
                        
                        logger.info(f"Server {name}: COMPLETED {req_id} after {process_time:.2f}s")
                        
                    else:
                        logger.info(f"Server {name}: Too slow for {req_id} (already taken)")
            else:
                logger.info(f"Server {name}: Decided to skip {req_id}")

def gateway_process(gateway_queue, global_scope):
    logger.info("Gateway is listening...")

    while True:
        request = gateway_queue.get() 
        req_id = request['id']
        logger.info(f"Gateway: Posting {request['id']} to the board.")

        global_scope['request_locks'][req_id] = global_scope['manager'].Lock()
        global_scope['requests'][req_id] = request

        with global_scope['broadcast']:
            global_scope['broadcast'].notify_all()

def client_process(client_id, gateway_queue, activation_event):
    logger.info(f"Client {client_id} is dormant.")
    
    while True:
        activation_event.wait()
        
        request = create_new_request(f"Client-{client_id}")
        logger.info(f"Client {client_id}: Submitting {request['id']} to Gateway.")
        
        gateway_queue.put(request)
        
        activation_event.clear()
        logger.info(f"Client {client_id}: Returning to dormancy.")


# Flow/ just listing all the deps
# master process creates a global scope and a queue to temporarily hold unassigned requests
# 
# All the processes created:
# there are three daemon serves created in parallel. They all require the global scope.
# A new gateway process is created
# A garbage collector process is created
# 
# 
# Clients are created. These clients would make the request
# each client recieves a trigger event and the gateway queue

# Client waits till its trigger event is set
# Once triggered, the client creates a request and puts it in the gateway queue
# Client clears its trigger event
# 
# The global scope is essentially a shared ds that contains a manager, a broadcast condition (manager.condition()), dict to contain locks for each request, and a dict to contain the requests themselves
# 
# the gatway process continously checks the gateway queue and adds new requests to the global scope, puts the shared manager lock so that processes can lock requests and notifies all once a new request is added
# 
# The server process sleeps till the broadcast signal is sent (using notify_all)
# They find which request is pending
# randomly decide to take it up, think for some (delay = random.uniform(1, 2); time.sleep(delay))
# pick it up, wait for some time, mark it as completed
# 
# 
# Garbage collector runs every minute to clear completed requests
def master_process():
    global_scope = init_global_scope()
    logger.info("Global scope initialized")
    gateway_queue = multiprocessing.Queue()
    
    number_of_servers = 3

    logger.info(f"Spawning {number_of_servers} server processes")
    for i in range(number_of_servers):
        p = multiprocessing.Process(target=server_process, args=(f"Srv-{i}", global_scope), daemon=True)
        p.start()
    logger.info(f"Spawned all!")

    logger.info("Spinning up the gateway")

    multiprocessing.Process(
        target=gateway_process, 
        args=(gateway_queue, global_scope), 
        name="Gateway", 
        daemon=True
    ).start()

    logger.info("Spinning up the garbage collector")
    multiprocessing.Process(
        target=garbage_collector,
        args=(global_scope,),
        name="GarbageCollector",
        daemon=True
    ).start()

    number_of_clients = 2
    client_triggers = {}
    logger.info(f"Spawning {number_of_clients} client processes")
    for i in range(number_of_clients):
        trigger = multiprocessing.Event()
        client_triggers[i] = trigger
        p = multiprocessing.Process(target=client_process, args=(i, gateway_queue, trigger), daemon=True)
        p.start()

    time.sleep(1) # let all spawn
    logger.info(f"Spawned all!")


    logger.info("\nREADY. Type '0' to trigger Client 0, '1' for Client 1, or 'q' to quit.")
    while True:
        cmd = input("Command > ").strip()
        if cmd == 'q':
            break
        elif cmd.isdigit() and int(cmd) in client_triggers:
            client_triggers[int(cmd)].set()
        else:
            logger.info("Invalid command. Try 0, 1, or q.")


if __name__ == "__main__":
    master_process()