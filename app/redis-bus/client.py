import requests
import time
import uuid
import json

BASE_URL = "http://localhost:8000"

def create_request():
    return {
        "id": f"REQ-{uuid.uuid4()}",
        "client_id": f"CLIENT-{uuid.uuid4()}",
        "content": "some work to be done",
        "timestamp": time.time()
    }

def stream_updates(request_id):
    url = f"{BASE_URL}/stream/{request_id}"

    with requests.get(url, stream=True) as r:
        for line in r.iter_lines():
            if not line:
                continue

            decoded = line.decode()

            if decoded.startswith("data: "):
                payload = decoded.replace("data: ", "", 1)

                try:
                    data = json.loads(payload)
                except:
                    print(f"[{request_id}] RAW:", payload)
                    continue

                print(f"[{request_id}] {data}")

                if data.get("event") == "completed":
                    print(f"[{request_id}] DONE")
                    break


def main():
    req = create_request()

    try:
        res = requests.post(f"{BASE_URL}/request", json=req)
        data = res.json()
        request_id = data["request_id"]
        print(f"Submitted {request_id}")
        stream_updates(request_id)

    except Exception as e:
        print(f"Error: {e}")


if __name__ == "__main__":
    main()