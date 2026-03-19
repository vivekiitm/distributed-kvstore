import hashlib

class HashRing:
    def __init__(self, nodes):
        self.ring = {}
        self.sorted_keys = []

        for node in nodes:
            h = self._hash(node)
            self.ring[h] = node
            self.sorted_keys.append(h)

        self.sorted_keys.sort()

    def _hash(self, key):
        return int(hashlib.md5(key.encode()).hexdigest(), 16)

    def get_node(self, key):
        h = self._hash(key)

        for node_hash in self.sorted_keys:
            if h <= node_hash:
                return self.ring[node_hash]

        return self.ring[self.sorted_keys[0]]
