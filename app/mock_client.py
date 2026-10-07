import requests
import random
import string
import time
import argparse
import sys
from typing import Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

class RequestClient:
    def __init__(self, base_url: str = "http://localhost:8000"):
        self.base_url = base_url
        self.client_id = f"CL-{random.randint(1000, 9999)}"
        
    def generate_request_id(self) -> str:
        return f"REQ-{''.join(random.choices(string.ascii_lowercase + string.digits, k=8))}"
    
    def send_request(self, content: str, track_progress: bool = False) -> dict:
        request_id = self.generate_request_id()
        
        payload = {
            "id": request_id,
            "client_id": self.client_id,
            "content": content
        }
        
        try:
            response = requests.post(
                f"{self.base_url}/request",
                json=payload,
                headers={"Content-Type": "application/json"}
            )
            
            if response.status_code == 200:
                result = response.json()
                print(f"Request {result['request_id']} submitted successfully")
                
                if track_progress:
                    self.stream_progress(result['request_id'])
                
                return {"success": True, "request_id": result['request_id'], "response": result}
            else:
                print(f"Request failed with status {response.status_code}")
                return {"success": False, "error": response.text}
                
        except requests.exceptions.RequestException as e:
            print(f"Request failed: {e}")
            return {"success": False, "error": str(e)}
    
    def stream_progress(self, request_id: str):
        try:
            response = requests.get(
                f"{self.base_url}/stream/{request_id}",
                stream=True
            )
            
            print(f"  Streaming progress for {request_id}...")
            
            for line in response.iter_lines():
                if line:
                    line_text = line.decode('utf-8')
                    if line_text.startswith('data: '):
                        data = line_text[6:]  # Remove 'data: ' prefix
                        print(f"    Update: {data}")
                        
        except requests.exceptions.RequestException as e:
            print(f"  Streaming failed: {e}")
    
    def wait_for_completion(self, request_id: str, timeout: int = 300) -> bool:
        import json
        start_time = time.time()
        
        try:
            response = requests.get(
                f"{self.base_url}/stream/{request_id}",
                stream=True,
                timeout=timeout + 10  # Give extra time for connection
            )
            
            print(f"  Waiting for completion of {request_id}...")
            
            for line in response.iter_lines():
                if time.time() - start_time > timeout:
                    print(f"  ⚠ Timeout waiting for {request_id}")
                    return False
                    
                if line:
                    line_text = line.decode('utf-8')
                    if line_text.startswith('data: '):
                        data_str = line_text[6:]  # Remove 'data: ' prefix
                        try:
                            data = json.loads(data_str)
                            if data.get('event') == 'completed':
                                print(f"  ✓ {request_id} completed")
                                return True
                        except json.JSONDecodeError:
                            pass  # Skip invalid JSON
                        
        except requests.exceptions.RequestException as e:
            print(f"  ✗ Error waiting for completion: {e}")
            return False
            
        return False

    def send_batch_requests(self, count: int, content: str = "Automated task", 
                          delay: float = 0.0, track_progress: bool = False, 
                          wait_for_completion: bool = False, timeout: int = 300,
                          parallel: bool = False, max_workers: int = 10) -> list:
        print(f"Sending {count} requests as client {self.client_id}")
        if parallel:
            print(f"Using parallel sending with {max_workers} workers")
        print("=" * 50)
        
        results = []
        start_time = time.time()
        
        if parallel:
            # Parallel mode for benchmarking - no waiting, maximum concurrency
            results = self._send_parallel_requests(count, content, False,  # No track_progress
                                                   False, timeout, max_workers)  # No wait
        else:
            # Serial mode for manual testing - supports tracking and waiting
            for i in range(count):
                task_content = f"{content} #{i+1}" if count > 1 else content
                result = self.send_request(task_content, track_progress)
                results.append(result)
                
                if result.get("success") and wait_for_completion:
                    completed = self.wait_for_completion(result['request_id'], timeout)
                    result['completed'] = completed
                
                if delay > 0 and i < count - 1:
                    time.sleep(delay)
        
        total_time = time.time() - start_time
        successful = sum(1 for r in results if r.get("success"))
        
        if wait_for_completion:
            completed_count = sum(1 for r in results if r.get("completed"))
            print("=" * 50)
            print(f"Submitted {successful}/{count} requests in {total_time:.2f} seconds")
            print(f"Completed {completed_count}/{successful} requests")
            if completed_count > 0:
                completion_time = time.time() - start_time
                print(f"Total completion time: {completion_time:.2f} seconds")
                print(f"Average completion rate: {completed_count/completion_time:.2f} requests/second")
        else:
            print("=" * 50)
            print(f"Completed {successful}/{count} requests in {total_time:.2f} seconds")
            print(f"Average rate: {count/total_time:.2f} requests/second")
        
        return results
    
    def _send_parallel_requests(self, count: int, content: str, track_progress: bool,
                               wait_for_completion: bool, timeout: int, max_workers: int) -> list:
        """Send requests in parallel using ThreadPoolExecutor."""
        results = [None] * count  # Pre-allocate list to maintain order
        
        def send_and_wait(index: int) -> tuple:
            task_content = f"{content} #{index+1}" if count > 1 else content
            result = self.send_request(task_content, track_progress)
            
            if result.get("success") and wait_for_completion:
                completed = self.wait_for_completion(result['request_id'], timeout)
                result['completed'] = completed
            
            return index, result
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all requests at once
            futures = {
                executor.submit(send_and_wait, i): i 
                for i in range(count)
            }
            
            # Collect results as they complete
            for future in as_completed(futures):
                try:
                    index, result = future.result()
                    results[index] = result
                except Exception as e:
                    index = futures[future]
                    print(f"Request {index} failed with exception: {e}")
                    results[index] = {"success": False, "error": str(e)}
        
        return results

def main():
    parser = argparse.ArgumentParser(description='Generate requests for the event dispatch system')
    parser.add_argument('--count', type=int, required=True,
                       help='Number of requests to send')
    parser.add_argument('--content', type=str, default='Automated task',
                       help='Content for the requests (default: "Automated task")')
    parser.add_argument('--delay', type=float, default=0.0,
                       help='Delay between requests in seconds (serial mode only, default: 0.0)')
    parser.add_argument('--track', action='store_true',
                       help='Track progress for each request via streaming (serial mode only)')
    parser.add_argument('--wait', action='store_true',
                       help='Wait for each request to complete (serial mode only)')
    parser.add_argument('--timeout', type=int, default=300,
                       help='Timeout for waiting for completion (default: 300 seconds)')
    parser.add_argument('--parallel', action='store_true',
                       help='Send requests in parallel for benchmarking (no tracking/waiting)')
    parser.add_argument('--workers', type=int, default=10,
                       help='Number of parallel workers (default: 10)')
    parser.add_argument('--url', type=str, default='http://localhost:8000',
                       help='Base URL of the server (default: http://localhost:8000)')
    args = parser.parse_args()
    
    if args.count <= 0:
        print("Error: Count must be greater than 0")
        sys.exit(1)
    
    if args.parallel:
        if args.track:
            print("Warning: --track is disabled in parallel mode")
        if args.wait:
            print("Warning: --wait is disabled in parallel mode (use monitor.py for completion tracking)")
        if args.delay > 0:
            print("Warning: --delay is disabled in parallel mode")
    
    try:
        client = RequestClient(base_url=args.url)
        results = client.send_batch_requests(
            count=args.count,
            content=args.content,
            delay=args.delay,
            track_progress=args.track if not args.parallel else False,
            wait_for_completion=args.wait if not args.parallel else False,
            timeout=args.timeout,
            parallel=args.parallel,
            max_workers=args.workers
        )
        
        successful = sum(1 for r in results if r.get("success"))
        
        if args.wait and not args.parallel:
            completed = sum(1 for r in results if r.get("completed"))
            if completed == successful:
                print(f"\n✓ All {successful} requests completed successfully")
                sys.exit(0)
            else:
                print(f"\n⚠ Only {completed}/{successful} requests completed")
                sys.exit(1)
        else:
            if successful == args.count:
                print(f"\n✓ All {args.count} requests submitted successfully")
                sys.exit(0)
            else:
                print(f"\n⚠ Only {successful}/{args.count} requests succeeded")
                sys.exit(1)
            
    except KeyboardInterrupt:
        print("\n\nInterrupted by user")
        sys.exit(130)
    except Exception as e:
        print(f"\nError: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == '__main__':
    main()