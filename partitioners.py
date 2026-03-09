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

class PartitionerFactory:
    @staticmethod 
    def create(partitioner_type: str, num_buckets: int) -> Partitioner:
        if partitioner_type == "crc32":
            return CRC32Partitioner(num_buckets)
        elif partitioner_type == "sha256":
            return SHA256Partitioner(num_buckets)
        raise ValueError(f"Unknown partitioner_type: {partitioner_type}")