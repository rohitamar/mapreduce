import hashlib
import zlib

class Partitioner:
    name = ""
    def __init__(self, num_buckets):
        self.num_buckets = num_buckets
    def get_bucket(self, key):
        raise NotImplementedError

class CRC32Partitioner(Partitioner):
    name = "crc32"
    def get_bucket(self, key):
        digest = zlib.crc32(key.encode("utf-8"))
        return digest % self.num_buckets

class SHA256Partitioner(Partitioner):
    name = "sha256"
    def get_bucket(self, key):
        digest = hashlib.sha256(key.encode("utf-8")).digest()
        big_int = int.from_bytes(digest, byteorder="big")
        return big_int % self.num_buckets

PARTITIONER_REGISTRY = {
    CRC32Partitioner.name: CRC32Partitioner,
    SHA256Partitioner.name: SHA256Partitioner,
}

def build_partitioner(name, num_buckets):
    try:
        return PARTITIONER_REGISTRY[name](num_buckets)
    except KeyError as exc:
        available = ", ".join(sorted(PARTITIONER_REGISTRY))
        raise ValueError(
            f"Unknown partitioner '{name}'. Available partitioners: {available}"
        ) from exc