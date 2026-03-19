import os
import json

class WALStorage:
    def __init__(self, node_id):
        self.file = f"data/{node_id}.log"
        os.makedirs("data", exist_ok=True)

    def append(self, key, value):
        with open(self.file, "a") as f:
            f.write(json.dumps({"key": key, "value": value}) + "\n")

    def load(self):
        data = {}
        if not os.path.exists(self.file):
            return data

        with open(self.file, "r") as f:
            for line in f:
                entry = json.loads(line.strip())
                data[entry["key"]] = entry["value"]
        return data
