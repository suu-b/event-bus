import redis
import time
import sys
import argparse

def check_completion():
    r = redis.Redis(host='localhost', port=6379, decode_responses=True)
    
    try:
        r.ping()
        print("Connected to Redis")
    except:
        print("Error: Cannot connect to Redis")
        sys.exit(1)
    
    # Check for both implementation styles
    request_keys = r.keys("request_data:*")
    metrics_keys = r.keys("metrics:*")
    
    # Filter only hash keys (not list keys like metrics:raw:*)
    metrics_hash_keys = [key for key in metrics_keys if r.type(key) == 'hash']
    
    # Count completed metrics (those with t4 timestamp)
    completed_count = 0
    for key in metrics_hash_keys:
        metrics = r.hgetall(key)
        if 't4' in metrics:
            completed_count += 1
    
    # Use request_data if available, otherwise use metrics
    if request_keys:
        total_requests = len(request_keys)
        # Also check the completed list for redis-bus
        list_completed = r.llen("requests:completed")
        if list_completed > completed_count:
            completed_count = list_completed
    else:
        total_requests = len(metrics_hash_keys)
    
    print(f"Total requests submitted: {total_requests}")
    print(f"Requests completed: {completed_count}")
    
    if completed_count >= total_requests and total_requests > 0:
        print("All requests have been completed!")
        return True
    else:
        remaining = max(0, total_requests - completed_count)
        print(f"{remaining} requests still processing...")
        return False

def wait_for_completion(check_interval=5, timeout=300):
    start_time = time.time()
    
    while time.time() - start_time < timeout:
        if check_completion():
            print(f"\nSystem completed in {time.time() - start_time:.1f} seconds")
            return True
        
        time.sleep(check_interval)
    
    print(f"\nTimeout reached after {timeout} seconds")
    return False

def main():
    parser = argparse.ArgumentParser(description='Monitor request completion')
    parser.add_argument('--wait', action='store_true', help='Wait for completion')
    parser.add_argument('--interval', type=int, default=5, help='Check interval in seconds')
    parser.add_argument('--timeout', type=int, default=300, help='Timeout in seconds')
    
    args = parser.parse_args()
    
    if args.wait:
        wait_for_completion(args.interval, args.timeout)
    else:
        check_completion()

if __name__ == '__main__':
    main()