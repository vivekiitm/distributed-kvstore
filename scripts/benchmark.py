import requests
import time

URL = "http://127.0.0.1:8000"

def benchmark(n=1000):
    start = time.time()

    for i in range(n):
        requests.put(f"{URL}/key/key{i}", params={"value": f"value{i}"})

    end = time.time()
    print(f"Writes/sec: {n / (end - start):.2f}")

if __name__ == "__main__":
    benchmark(2000)
