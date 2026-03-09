import os

class BufferManager:
    def __init__(
        self,
        worker_id,
        num_buckets,
        partitioner,
        serializer,
        dump_dir="./dump",
        buffer_threshold_bytes=64 * 1024 * 1024,
    ):
        self.worker_id = worker_id
        self.num_buckets = num_buckets
        self.partitioner = partitioner
        self.serializer = serializer
        self.dump_dir = dump_dir

        self.buffer_threshold_bytes = buffer_threshold_bytes
        self.buffer = [[] for _ in range(self.num_buckets)]          
        self.buffered_bytes = 0
        
        self._num_flushes = 0

        os.makedirs(self.dump_dir, exist_ok=True)
    
    def write_pairs(self, pairs):
        for key, value in pairs:
            key_str = str(key)
            bucket = self.partitioner.get_bucket(key_str)
            
            self.buffer[bucket].append((key_str, value))
            
            # f"{key_str}\t{value}\n" --> len(key_str) + len(value) + 2 (\t and \n)
            self.buffered_bytes += len(key_str.encode("utf-8")) + len(str(value).encode("utf-8")) + 2

            if self.buffered_bytes >= self.buffer_threshold_bytes:
                self.flush()

    def flush(self):
        if self.buffered_bytes == 0:
            return

        for bucket, lines in enumerate(self.buffer):
            if not lines:
                continue

            lines.sort()
            file_name = (
                f"{self.dump_dir}/map-{self.worker_id}-partition-{bucket}-run-{self._num_flushes}"
            )
            
            with open(file_name, "w", encoding="utf-8") as out:
                for key, value in lines:
                    out.write(self.serializer(key, value))

        self.buffer = [[] for _ in range(self.num_buckets)]
        self.buffered_bytes = 0
        self._num_flushes += 1
    
    @property
    def get_num_flushes(self):
        return self._num_flushes
