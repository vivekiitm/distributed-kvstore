from fastapi import FastAPI
from kvstore.node import Node

app = FastAPI()

node = Node(node_id="node1")

@app.put("/key/{key}")
def put_key(key: str, value: str):
    node.put(key, value)
    return {"status": "ok", "key": key, "value": value}

@app.get("/key/{key}")
def get_key(key: str):
    value = node.get(key)
    return {"key": key, "value": value}
