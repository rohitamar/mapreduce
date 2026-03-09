import hashlib
import os

class BufferManager:
    def __init__(
        self,
        worker_id,
        num_buckets,
        dump_dir="./dump",
        buffer_threshold_bytes=64 * 1024 * 1024,
    ):
        self.worker_id = worker_id
        self.num_buckets = num_buckets
        self.dump_dir = dump_dir

        self.buffer_threshold_bytes = buffer_threshold_bytes
        self.buffer = [[] for _ in range(self.num_buckets)]          
        self.buffered_bytes = 0
        
        self._num_flushes = 0

        os.makedirs(self.dump_dir, exist_ok=True)
    
    def _sha_to_bucket(self, key):
        digest = hashlib.sha256(key.encode("utf-8")).digest()
        big_int = int.from_bytes(digest, byteorder="big")
        return big_int % self.num_buckets
    
    def write_pairs(self, pairs, serializer):
        for key, value in pairs:
            key_str = str(key)
            bucket = self._sha_to_bucket(key_str)
            line = serializer(key_str, value)
            
            self.buffer[bucket].append(line)
            self.buffered_bytes += len(line.encode("utf-8"))

            if self.buffered_bytes >= self.buffer_threshold_bytes:
                self.flush()

    def flush(self):
        if self.buffered_bytes == 0:
            return

        for bucket, lines in enumerate(self.buffer):
            if not lines:
                continue

            lines.sort(key=lambda line: line.split("\t", 1)[0])
            file_name = (
                f"{self.dump_dir}/map-{self.worker_id}-partition-{bucket}-run-{self._num_flushes}"
            )
            with open(file_name, "w", encoding="utf-8") as out:
                out.writelines(lines)

        self.buffer = [[] for _ in range(self.num_buckets)]
        self.buffered_bytes = 0
        self._num_flushes += 1
    
    @property
    def get_num_flushes(self):
        return self._num_flushes
